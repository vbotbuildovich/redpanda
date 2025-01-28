/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/transform.h"

namespace iceberg {

struct transform_comparison_visitor {
    template<typename T, typename U>
    bool operator()(const T&, const U&) const {
        static_assert(!std::is_same<T, U>::value);
        return false;
    }
    bool
    operator()(const bucket_transform& lhs, const bucket_transform& rhs) const {
        return lhs.n == rhs.n;
    }
    bool operator()(
      const truncate_transform& lhs, const truncate_transform& rhs) const {
        return lhs.length == rhs.length;
    }
    template<typename T>
    bool operator()(const T&, const T&) const {
        return true;
    }
};

bool operator==(const transform& lhs, const transform& rhs) {
    return std::visit(transform_comparison_visitor{}, lhs, rhs);
}

} // namespace iceberg
