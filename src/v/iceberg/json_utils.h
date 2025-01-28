/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/chunked_hash_map.h"
#include "json/document.h"

namespace iceberg {

std::optional<std::reference_wrapper<const json::Value>>
parse_optional(const json::Value& v, std::string_view member_name);

const json::Value&
parse_required(const json::Value& v, std::string_view member_name);

json::Value::ConstArray
parse_required_array(const json::Value& v, std::string_view member_name);
json::Value::ConstObject
parse_required_object(const json::Value& v, std::string_view member_name);

std::optional<json::Value::ConstArray>
parse_optional_array(const json::Value& v, std::string_view member_name);
std::optional<json::Value::ConstObject>
parse_optional_object(const json::Value& v, std::string_view member_name);

ss::sstring
parse_required_str(const json::Value& v, std::string_view member_name);

int32_t parse_required_i32(const json::Value& v, std::string_view member_name);
int64_t parse_required_i64(const json::Value& v, std::string_view member_name);

std::optional<int32_t>
parse_optional_i32(const json::Value& v, std::string_view member_name);
std::optional<int64_t>
parse_optional_i64(const json::Value& v, std::string_view member_name);

std::optional<ss::sstring>
parse_optional_str(const json::Value& v, std::string_view member_name);

bool parse_required_bool(const json::Value& v, std::string_view member_name);

std::string_view
extract_between(char start_ch, char end_ch, std::string_view s);

chunked_hash_map<ss::sstring, ss::sstring>
parse_required_string_map(const json::Value&, std::string_view);

std::optional<chunked_hash_map<ss::sstring, ss::sstring>>
parse_optional_string_map(const json::Value&, std::string_view);

} // namespace iceberg
