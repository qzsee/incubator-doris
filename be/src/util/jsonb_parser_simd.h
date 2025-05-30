/*
 *  Copyright (c) 2014, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

/*
 * This file defines JsonbParserTSIMD (template) and JsonbParser.
 *
 * JsonbParserTSIMD is a template class which implements a JSON parser.
 * JsonbParserTSIMD parses JSON text, and serialize it to JSONB binary format
 * by using JsonbWriterT object. By default, JsonbParserTSIMD creates a new
 * JsonbWriterT object with an output stream object.  However, you can also
 * pass in your JsonbWriterT or any stream object that implements some basic
 * interface of std::ostream (see JsonbStream.h).
 *
 * JsonbParser specializes JsonbParserTSIMD with JsonbOutStream type (see
 * JsonbStream.h). So unless you want to provide own a different output stream
 * type, use JsonbParser object.
 *
 * ** Parsing JSON **
 * JsonbParserTSIMD parses JSON string, and directly serializes into JSONB
 * packed bytes. There are three ways to parse a JSON string: (1) using
 * c-string, (2) using string with len, (3) using std::istream object. You can
 * use custom streambuf to redirect output. JsonbOutBuffer is a streambuf used
 * internally if the input is raw character buffer.
 *
 * You can reuse an JsonbParserTSIMD object to parse/serialize multiple JSON
 * strings, and the previous JSONB will be overwritten.
 *
 * If parsing fails (returned false), the error code will be set to one of
 * JsonbErrType, and can be retrieved by calling getErrorCode().
 *
 * ** External dictionary **
 * During parsing a JSON string, you can pass a call-back function to map a key
 * string to an id, and store the dictionary id in JSONB to save space. The
 * purpose of using an external dictionary is more towards a collection of
 * documents (which has common keys) rather than a single document, so that
 * space saving will be significant.
 *
 * ** Endianness **
 * Note: JSONB serialization doesn't assume endianness of the server. However
 * you will need to ensure that the endianness at the reader side is the same
 * as that at the writer side (if they are on different machines). Otherwise,
 * proper conversion is needed when a number value is returned to the
 * caller/writer.
 *
 * @author Tian Xia <tianx@fb.com>
 * 
 * this file is copied from 
 * https://github.com/facebook/mysql-5.6/blob/fb-mysql-5.6.35/fbson/FbsonJsonParser.h
 * and modified by Doris
 */

#ifndef JSONB_JSONBJSONPARSERSIMD_H
#define JSONB_JSONBJSONPARSERSIMD_H

#include <simdjson.h>

#include <cmath>
#include <limits>

#include "jsonb_document.h"
#include "jsonb_error.h"
#include "jsonb_writer.h"
#include "string_parser.hpp"

namespace doris {

using int128_t = __int128;
class JsonbParserTSIMD {
public:
    JsonbParserTSIMD() : err_(JsonbErrType::E_NONE) {}

    explicit JsonbParserTSIMD(JsonbOutStream& os) : writer_(os), err_(JsonbErrType::E_NONE) {}

    // parse a UTF-8 JSON string
    bool parse(const std::string& str) { return parse(str.c_str(), str.size()); }

    // parse a UTF-8 JSON c-style string (NULL terminated)
    bool parse(const char* c_str) { return parse(c_str, strlen(c_str)); }

    // parse a UTF-8 JSON string with length
    bool parse(const char* pch, size_t len) {
        // reset state before parse
        reset();

        if (!pch || len == 0) {
            err_ = JsonbErrType::E_EMPTY_DOCUMENT;
            VLOG_DEBUG << "empty json string";
            return false;
        }

        // parse json using simdjson, return false on exception
        try {
            simdjson::padded_string json_str {pch, len};
            simdjson::ondemand::document doc = parser_.iterate(json_str);

            // simdjson process top level primitive types specially
            // so some repeated code here
            switch (doc.type()) {
            case simdjson::ondemand::json_type::object:
            case simdjson::ondemand::json_type::array: {
                parse(doc.get_value());
                break;
            }
            case simdjson::ondemand::json_type::null: {
                if (writer_.writeNull() == 0) {
                    err_ = JsonbErrType::E_OUTPUT_FAIL;
                    LOG(WARNING) << "writeNull failed";
                }
                break;
            }
            case simdjson::ondemand::json_type::boolean: {
                if (writer_.writeBool(doc.get_bool()) == 0) {
                    err_ = JsonbErrType::E_OUTPUT_FAIL;
                    LOG(WARNING) << "writeBool failed";
                }
                break;
            }
            case simdjson::ondemand::json_type::string: {
                write_string(doc.get_string());
                break;
            }
            case simdjson::ondemand::json_type::number: {
                write_number(doc.get_number(), doc.raw_json_token());
                break;
            }
            }

            return err_ == JsonbErrType::E_NONE;
        } catch (simdjson::simdjson_error& e) {
            err_ = JsonbErrType::E_EXCEPTION;
            VLOG_DEBUG << "simdjson parse exception: " << e.what();
            return false;
        }
    }

    // parse json, recursively if necessary, by simdjson
    //  and serialize to binary format by writer
    void parse(simdjson::ondemand::value value) {
        switch (value.type()) {
        case simdjson::ondemand::json_type::null: {
            if (writer_.writeNull() == 0) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                LOG(WARNING) << "writeNull failed";
            }
            break;
        }
        case simdjson::ondemand::json_type::boolean: {
            if (writer_.writeBool(value.get_bool()) == 0) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                LOG(WARNING) << "writeBool failed";
            }
            break;
        }
        case simdjson::ondemand::json_type::string: {
            write_string(value.get_string());
            break;
        }
        case simdjson::ondemand::json_type::number: {
            write_number(value.get_number(), value.raw_json_token());
            break;
        }
        case simdjson::ondemand::json_type::object: {
            if (!writer_.writeStartObject()) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                LOG(WARNING) << "writeStartObject failed";
                break;
            }

            for (auto kv : value.get_object()) {
                std::string_view key;
                simdjson::error_code e = kv.unescaped_key().get(key);
                if (e != simdjson::SUCCESS) {
                    err_ = JsonbErrType::E_INVALID_KEY_STRING;
                    LOG(WARNING) << "simdjson get key failed: " << e;
                    break;
                }

                if (writer_.writeKey(key.data(), key.size()) == 0) {
                    err_ = JsonbErrType::E_OUTPUT_FAIL;
                    LOG(WARNING) << "writeKey failed key: " << key;
                    break;
                }

                // parse object value
                parse(kv.value());
                if (err_ != JsonbErrType::E_NONE) {
                    LOG(WARNING) << "parse object value failed";
                    break;
                }
            }
            if (err_ != JsonbErrType::E_NONE) {
                break;
            }

            if (!writer_.writeEndObject()) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                LOG(WARNING) << "writeEndObject failed";
                break;
            }

            break;
        }
        case simdjson::ondemand::json_type::array: {
            if (!writer_.writeStartArray()) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                LOG(WARNING) << "writeStartArray failed";
                break;
            }

            for (auto elem : value.get_array()) {
                // parse array element
                parse(elem.value());
                if (err_ != JsonbErrType::E_NONE) {
                    LOG(WARNING) << "parse array element failed";
                    break;
                }
            }
            if (err_ != JsonbErrType::E_NONE) {
                break;
            }

            if (!writer_.writeEndArray()) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                LOG(WARNING) << "writeEndArray failed";
                break;
            }

            break;
        }
        default: {
            err_ = JsonbErrType::E_INVALID_TYPE;
            LOG(WARNING) << "unknown value type: "; // << value;
            break;
        }

        } // end of switch
    }

    void write_string(std::string_view str) {
        // start writing string
        if (!writer_.writeStartString()) {
            err_ = JsonbErrType::E_OUTPUT_FAIL;
            LOG(WARNING) << "writeStartString failed";
            return;
        }

        // write string
        if (str.size() > 0) {
            if (writer_.writeString(str.data(), str.size()) == 0) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                LOG(WARNING) << "writeString failed";
                return;
            }
        }

        // end writing string
        if (!writer_.writeEndString()) {
            err_ = JsonbErrType::E_OUTPUT_FAIL;
            LOG(WARNING) << "writeEndString failed";
            return;
        }
    }

    void write_number(simdjson::ondemand::number num, std::string_view raw_string) {
        if (num.is_double()) {
            double number = num.get_double();
            // When a double exceeds the precision that can be represented by a double type in simdjson, it gets converted to 0.
            // The correct approach, should be to truncate the double value instead.
            if (number == 0) {
                StringParser::ParseResult result;
                number = StringParser::string_to_float<double>(raw_string.data(), raw_string.size(),
                                                               &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    err_ = JsonbErrType::E_INVALID_NUMBER;
                    LOG(WARNING) << "invalid number, raw string is: " << raw_string;
                    return;
                }
            }

            if (writer_.writeDouble(number) == 0) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                LOG(WARNING) << "writeDouble failed";
                return;
            }
        } else if (num.is_int64() || num.is_uint64()) {
            int128_t val = num.is_int64() ? (int128_t)num.get_int64() : (int128_t)num.get_uint64();
            int size = 0;
            if (val >= std::numeric_limits<int8_t>::min() &&
                val <= std::numeric_limits<int8_t>::max()) {
                size = writer_.writeInt8((int8_t)val);
            } else if (val >= std::numeric_limits<int16_t>::min() &&
                       val <= std::numeric_limits<int16_t>::max()) {
                size = writer_.writeInt16((int16_t)val);
            } else if (val >= std::numeric_limits<int32_t>::min() &&
                       val <= std::numeric_limits<int32_t>::max()) {
                size = writer_.writeInt32((int32_t)val);
            } else if (val >= std::numeric_limits<int64_t>::min() &&
                       val <= std::numeric_limits<int64_t>::max()) {
                size = writer_.writeInt64((int64_t)val);
            } else { // INT128
                size = writer_.writeInt128(val);
            }

            if (size == 0) {
                err_ = JsonbErrType::E_OUTPUT_FAIL;
                LOG(WARNING) << "writeInt failed";
                return;
            }
        } else {
            err_ = JsonbErrType::E_INVALID_NUMBER;
            LOG(WARNING) << "invalid number: " << num.as_double();
            return;
        }
    }

    JsonbWriterT<JsonbOutStream>& getWriter() { return writer_; }

    JsonbErrType getErrorCode() { return err_; }

    // clear error code
    void clearErr() { err_ = JsonbErrType::E_NONE; }

    void reset() {
        writer_.reset();
        clearErr();
    }

private:
    simdjson::ondemand::parser parser_;
    JsonbWriterT<JsonbOutStream> writer_;
    JsonbErrType err_;
};

using JsonbParser = JsonbParserTSIMD;

} // namespace doris

#endif // JSONB_JSONBJSONPARSERSIMD_H
