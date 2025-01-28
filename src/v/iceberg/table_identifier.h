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
#include "base/seastarx.h"
#include "container/fragmented_vector.h"

#include <seastar/core/sstring.hh>

namespace iceberg {
struct table_identifier {
    chunked_vector<ss::sstring> ns;
    ss::sstring table;

    table_identifier copy() const {
        return table_identifier{
          .ns = ns.copy(),
          .table = table,
        };
    }
};
std::ostream& operator<<(std::ostream& o, const table_identifier& id);
} // namespace iceberg
