// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Tests the alarm() watchdog in common/signal_handler.h.
//
// A fatal signal is delivered on the thread that faulted, and the handler runs
// on that thread's stack. So if the thread faulted while holding the allocator
// lock, anything in the handler that allocates waits on a lock only that
// thread could release -- the process then neither serves traffic nor dies.
// The watchdog exists to make it die.
//
// Each test forks a child that reproduces that deadlock, so we can assert on
// whether the child dies without taking down the test runner.

#include <pthread.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <cstring>
#include <thread>

#include "gtest/gtest.h"

namespace doris {
namespace {

// Shortened from the 60s used in production so the tests finish quickly.
constexpr unsigned int kWatchdogSeconds = 3;

// Stands in for a jemalloc arena mutex.
pthread_mutex_t g_allocator_lock = PTHREAD_MUTEX_INITIALIZER;

// Same two steps as FailureWatchdogHandler in common/signal_handler.h:
// restore the default action, then signal ourselves so the kernel dumps core.
void watchdog_handler(int) {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = SIG_DFL;
    sigaction(SIGABRT, &action, nullptr);
    kill(getpid(), SIGABRT);
}

// Crash handler that deadlocks, like boost::stacktrace() calling malloc() when
// the faulting thread already holds the allocator lock.
void crash_handler_that_deadlocks(int) {
    pthread_mutex_lock(&g_allocator_lock); // never returns: we hold it already
}

// Same, but arms the watchdog first -- this is what the fix adds.
void crash_handler_with_watchdog(int) {
    ::signal(SIGALRM, &watchdog_handler);
    alarm(kWatchdogSeconds);
    pthread_mutex_lock(&g_allocator_lock); // still never returns
}

// Runs in the forked child: take the lock, crash while holding it, and let
// `handler` run. Mirrors the compaction thread that crashed inside malloc().
[[noreturn]] void crash_while_holding_lock(void (*handler)(int)) {
    ::signal(SIGSEGV, handler);
    pthread_mutex_lock(&g_allocator_lock);

    volatile int* bad_pointer = nullptr;
    *bad_pointer = 1;

    _exit(1); // Unreachable.
}

// Result of watching a child for `timeout_seconds`.
struct ChildResult {
    bool died = false;   // false means it was still alive and we had to kill it
    int signal_number = 0;
    int seconds = 0;
};

ChildResult wait_for_child(pid_t pid, int timeout_seconds) {
    ChildResult result;
    int status = 0;

    for (; result.seconds < timeout_seconds; ++result.seconds) {
        if (waitpid(pid, &status, WNOHANG) == pid) {
            result.died = true;
            result.signal_number = WIFSIGNALED(status) ? WTERMSIG(status) : 0;
            return result;
        }
        sleep(1);
    }

    // Still alive. Clean up so the suite does not leak a stuck process.
    kill(pid, SIGKILL);
    waitpid(pid, &status, 0);
    return result;
}

} // namespace

class SignalHandlerTest : public testing::Test {};

// Without the watchdog the process hangs forever. This pins the problem the
// fix solves; if it ever fails, the fix is no longer needed.
TEST_F(SignalHandlerTest, WithoutWatchdogProcessHangs) {
    const pid_t pid = fork();
    ASSERT_NE(pid, -1) << strerror(errno);
    if (pid == 0) {
        crash_while_holding_lock(&crash_handler_that_deadlocks);
    }

    const ChildResult result = wait_for_child(pid, kWatchdogSeconds + 3);

    EXPECT_FALSE(result.died) << "child died by itself, so it never deadlocked "
                                 "and this test is not checking anything";
}

// With the watchdog the process must die, even though the only thread that
// could release the lock is the one stuck waiting for it.
TEST_F(SignalHandlerTest, WatchdogKillsHungProcess) {
    const pid_t pid = fork();
    ASSERT_NE(pid, -1) << strerror(errno);
    if (pid == 0) {
        crash_while_holding_lock(&crash_handler_with_watchdog);
    }

    const ChildResult result = wait_for_child(pid, kWatchdogSeconds + 10);

    ASSERT_TRUE(result.died) << "watchdog never fired";
    // SIGABRT, not SIGALRM: SIGALRM's default action terminates without a core.
    EXPECT_EQ(result.signal_number, SIGABRT) << "died without producing a core";
    EXPECT_LE(result.seconds, static_cast<int>(kWatchdogSeconds) + 2) << "fired late";
}

// sleep() used to be built on alarm() and would cancel a pending one. The
// handler has exactly that shape: one thread arms alarm() while the other
// crashed threads sit in while(true) sleep(1). If that were still true, the
// watchdog would be silently cancelled.
TEST_F(SignalHandlerTest, SleepDoesNotCancelAlarm) {
    // A pending alarm must survive a sleep() call.
    ::signal(SIGALRM, SIG_IGN);
    alarm(30);
    sleep(1);
    const unsigned int remaining = alarm(0); // read the alarm, then disarm it
    EXPECT_GE(remaining, 28U) << "sleep() cancelled the alarm";

    // And it must still fire while another thread loops on sleep().
    static std::atomic<bool> fired {false};
    ::signal(SIGALRM, [](int) { fired = true; });
    alarm(kWatchdogSeconds);

    std::atomic<bool> stop {false};
    std::thread sleeper([&stop] {
        while (!stop) {
            sleep(1);
        }
    });
    sleep(kWatchdogSeconds + 2);
    stop = true;
    sleeper.join();

    ::signal(SIGALRM, SIG_DFL);

    EXPECT_TRUE(fired.load()) << "alarm did not fire while another thread slept";
}

} // namespace doris
