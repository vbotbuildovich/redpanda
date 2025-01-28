/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/partition_json.h"

#include "iceberg/json_utils.h"
#include "iceberg/partition.h"
#include "iceberg/transform_json.h"
#include "json/document.h"

#include <stdexcept>

namespace iceberg {

partition_field parse_partition_field(const json::Value& v) {
    auto source_id = parse_required_i32(v, "source-id");
    auto field_id = parse_required_i32(v, "field-id");
    auto name = parse_required_str(v, "name");
    auto transform_str = parse_required_str(v, "transform");
    auto transform = transform_from_str(transform_str);
    return partition_field{
      .source_id = nested_field::id_t{source_id},
      .field_id = partition_field::id_t{field_id},
      .name = name,
      .transform = transform,
    };
}

chunked_vector<partition_field>
parse_partition_fields(const json::Value::ConstArray& fields_array) {
    chunked_vector<partition_field> fs;
    fs.reserve(fields_array.Size());
    for (const auto& f : fields_array) {
        fs.emplace_back(parse_partition_field(f));
    }
    return fs;
}
partition_spec parse_partition_spec(const json::Value& v) {
    auto spec_id = parse_required_i32(v, "spec-id");
    auto fields_array = parse_required_array(v, "fields");
    auto fs = parse_partition_fields(fields_array);
    return partition_spec{
      .spec_id = partition_spec::id_t{spec_id},
      .fields = std::move(fs),
    };
}

} // namespace iceberg

namespace json {

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::partition_field& m) {
    w.StartObject();
    w.Key("source-id");
    w.Int(m.source_id());
    w.Key("field-id");
    w.Int(m.field_id());
    w.Key("name");
    w.String(m.name);
    w.Key("transform");
    w.String(transform_to_str(m.transform));
    w.EndObject();
}

void rjson_serialize(
  iceberg::json_writer& w,
  const chunked_vector<iceberg::partition_field>& fields) {
    w.StartArray();
    for (const auto& f : fields) {
        rjson_serialize(w, f);
    }
    w.EndArray();
}

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::partition_spec& m) {
    w.StartObject();
    w.Key("spec-id");
    w.Int(m.spec_id());
    w.Key("fields");
    rjson_serialize(w, m.fields);
    w.EndObject();
}

} // namespace json
