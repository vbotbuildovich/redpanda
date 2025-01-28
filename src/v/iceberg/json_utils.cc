/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/json_utils.h"

#include <fmt/format.h>

#include <stdexcept>

namespace iceberg {

namespace {
chunked_hash_map<ss::sstring, ss::sstring>
parse_string_map(const json::Value& map_json, std::string_view member_name) {
    if (!map_json.IsObject()) {
        throw std::invalid_argument(fmt::format(
          "Expected map field '{}' to be an object type", member_name));
    }
    const auto& map_obj = map_json.GetObject();
    chunked_hash_map<ss::sstring, ss::sstring> ret;
    ret.reserve(map_obj.MemberCount());
    for (const auto& property : map_obj) {
        if (!property.name.IsString() || !property.value.IsString()) {
            throw std::invalid_argument(fmt::format(
              "Expected '{}' field to be a string map. Current type map<{},{}>",
              member_name,
              property.name.GetType(),
              property.value.GetType()));
        }

        ret.emplace(property.name.GetString(), property.value.GetString());
    }
    return ret;
}
} // namespace

std::optional<std::reference_wrapper<const json::Value>>
parse_optional(const json::Value& v, std::string_view member_name) {
    if (!v.IsObject()) {
        throw std::invalid_argument(
          fmt::format("Expected JSON object to parse field '{}'", member_name));
    }
    auto iter = v.FindMember(member_name.data());
    if (iter == v.MemberEnd()) {
        return std::nullopt;
    }
    return iter->value;
}

const json::Value&
parse_required(const json::Value& v, std::string_view member_name) {
    if (!v.IsObject()) {
        throw std::invalid_argument(
          fmt::format("Expected JSON object to parse field '{}'", member_name));
    }
    auto iter = v.FindMember(member_name.data());
    if (iter == v.MemberEnd()) {
        throw std::invalid_argument(
          fmt::format("No member named '{}'", member_name));
    }
    return iter->value;
}

json::Value::ConstArray
parse_required_array(const json::Value& v, std::string_view member_name) {
    const auto& array_json = parse_required(v, member_name);
    if (!array_json.IsArray()) {
        throw std::invalid_argument(fmt::format(
          "Expected array for field '{}': {}",
          member_name,
          array_json.GetType()));
    }
    return array_json.GetArray();
}

std::optional<json::Value::ConstArray>
parse_optional_array(const json::Value& v, std::string_view member_name) {
    const auto json = parse_optional(v, member_name);
    if (!json.has_value()) {
        return std::nullopt;
    }
    const auto& val = json.value().get();
    if (!val.IsArray()) {
        throw std::invalid_argument(fmt::format(
          "Expected array for field '{}': {}", member_name, val.GetType()));
    }
    return val.GetArray();
}

json::Value::ConstObject
parse_required_object(const json::Value& v, std::string_view member_name) {
    const auto& obj_json = parse_required(v, member_name);
    if (!obj_json.IsObject()) {
        throw std::invalid_argument(fmt::format(
          "Expected object for field '{}': {}",
          member_name,
          obj_json.GetType()));
    }
    return obj_json.GetObject();
}

std::optional<json::Value::ConstObject>
parse_optional_object(const json::Value& v, std::string_view member_name) {
    const auto json = parse_optional(v, member_name);
    if (!json.has_value()) {
        return std::nullopt;
    }
    const auto& val = json.value().get();
    if (!val.IsObject()) {
        throw std::invalid_argument(fmt::format(
          "Expected object for field '{}': {}", member_name, val.GetType()));
    }
    return val.GetObject();
}

ss::sstring
parse_required_str(const json::Value& v, std::string_view member_name) {
    const auto& str_json = parse_required(v, member_name);
    if (!str_json.IsString()) {
        throw std::invalid_argument(
          fmt::format("Expected string for field '{}'", member_name));
    }
    return str_json.GetString();
}

int32_t parse_required_i32(const json::Value& v, std::string_view member_name) {
    const auto& int_json = parse_required(v, member_name);
    if (!int_json.IsInt()) {
        throw std::invalid_argument(
          fmt::format("Expected integer for field '{}'", member_name));
    }
    return int_json.GetInt();
}

int64_t parse_required_i64(const json::Value& v, std::string_view member_name) {
    const auto& int_json = parse_required(v, member_name);
    if (!int_json.IsInt64()) {
        throw std::invalid_argument(
          fmt::format("Expected int64 for field '{}'", member_name));
    }
    return int_json.GetInt64();
}

std::optional<int32_t>
parse_optional_i32(const json::Value& v, std::string_view member_name) {
    const auto json = parse_optional(v, member_name);
    if (!json.has_value()) {
        return std::nullopt;
    }
    if (!json->get().IsInt()) {
        throw std::invalid_argument(
          fmt::format("Expected integer for field '{}'", member_name));
    }
    return json->get().GetInt();
}

std::optional<int64_t>
parse_optional_i64(const json::Value& v, std::string_view member_name) {
    const auto json = parse_optional(v, member_name);
    if (!json.has_value()) {
        return std::nullopt;
    }
    if (!json->get().IsInt64()) {
        throw std::invalid_argument(
          fmt::format("Expected int64 for field '{}'", member_name));
    }
    return json->get().GetInt64();
}

std::optional<ss::sstring>
parse_optional_str(const json::Value& v, std::string_view member_name) {
    const auto json = parse_optional(v, member_name);
    if (!json.has_value()) {
        return std::nullopt;
    }
    if (!json->get().IsString()) {
        throw std::invalid_argument(
          fmt::format("Expected string for field '{}'", member_name));
    }
    return json->get().GetString();
}

bool parse_required_bool(const json::Value& v, std::string_view member_name) {
    const auto& bool_json = parse_required(v, member_name);
    if (!bool_json.IsBool()) {
        throw std::invalid_argument(
          fmt::format("Expected bool for field '{}'", member_name));
    }
    return bool_json.GetBool();
}

std::string_view
extract_between(char start_ch, char end_ch, std::string_view s) {
    auto start_pos = s.find(start_ch);
    auto end_pos = s.find(end_ch, start_pos);

    if (start_pos != std::string::npos && end_pos != std::string::npos) {
        return s.substr(start_pos + 1, end_pos - start_pos - 1);
    }
    throw std::invalid_argument(
      fmt::format("Missing wrappers '{}' or '{}' in {}", start_ch, end_ch, s));
}

chunked_hash_map<ss::sstring, ss::sstring>
parse_required_string_map(const json::Value& v, std::string_view member_name) {
    const auto& map_json = parse_required(v, member_name);

    return parse_string_map(map_json, member_name);
}

std::optional<chunked_hash_map<ss::sstring, ss::sstring>>
parse_optional_string_map(const json::Value& v, std::string_view member_name) {
    const auto& map_json = parse_optional(v, member_name);
    if (!map_json) {
        return std::nullopt;
    }
    return parse_string_map(*map_json, member_name);
}

} // namespace iceberg
