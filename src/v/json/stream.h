// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "json/_include_first.h"

#include <rapidjson/stream.h>

namespace json {

template<typename Encoding = rapidjson::UTF8<>>
using GenericStringStream = rapidjson::GenericStringStream<Encoding>;

using StringStream = GenericStringStream<>;

} // namespace json
