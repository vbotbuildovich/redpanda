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

#include "container/fragmented_vector.h"
#include "iceberg/datatypes.h"
#include "iceberg/transform.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

namespace iceberg {

struct partition_field {
    using id_t = named_type<int32_t, struct field_id_tag>;
    nested_field::id_t source_id;
    id_t field_id;
    ss::sstring name;
    transform transform;

    friend bool operator==(const partition_field&, const partition_field&)
      = default;
};

struct partition_spec {
    using id_t = named_type<int32_t, struct spec_id_tag>;
    id_t spec_id;
    chunked_vector<partition_field> fields;

    friend bool operator==(const partition_spec&, const partition_spec&)
      = default;
    partition_spec copy() const {
        return {
          .spec_id = spec_id,
          .fields = fields.copy(),
        };
    }
};

} // namespace iceberg
