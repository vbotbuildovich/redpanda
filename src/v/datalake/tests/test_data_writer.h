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

#include "datalake/data_writer_interface.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <cstdint>
#include <memory>
namespace datalake {

class test_data_writer : public parquet_file_writer {
public:
    explicit test_data_writer(
      const iceberg::struct_type& schema, bool return_error)
      : _schema(schema.copy())
      , _result{}
      , _return_error{return_error} {}

    ss::future<writer_error> add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */) override {
        _result.row_count++;
        writer_error status = _return_error
                                ? writer_error::parquet_conversion_error
                                : writer_error::ok;
        return ss::make_ready_future<writer_error>(status);
    }

    ss::future<result<local_file_metadata, writer_error>> finish() override {
        return ss::make_ready_future<result<local_file_metadata, writer_error>>(
          _result);
    }

private:
    iceberg::struct_type _schema;
    local_file_metadata _result;
    bool _return_error;
};
class test_data_writer_factory : public parquet_file_writer_factory {
public:
    explicit test_data_writer_factory(bool return_error)
      : _return_error{return_error} {}

    ss::future<result<std::unique_ptr<parquet_file_writer>, writer_error>>
    create_writer(const iceberg::struct_type& schema) override {
        co_return std::make_unique<test_data_writer>(
          std::move(schema), _return_error);
    }

private:
    iceberg::struct_type _schema;
    bool _return_error;
};

} // namespace datalake
