/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/parquet/writer.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "container/contiguous_range_map.h"
#include "serde/parquet/column_writer.h"
#include "serde/parquet/metadata.h"
#include "serde/parquet/shredder.h"

namespace serde::parquet {

class writer::impl {
public:
    impl(options opts, ss::output_stream<char> output)
      : _opts(std::move(opts))
      , _output(std::move(output)) {}

    ss::future<> init() {
        index_schema(_opts.schema);
        _opts.schema.for_each([this](const schema_element& element) {
            if (!element.is_leaf()) {
                return;
            }
            _column_writers.emplace(element.position, element);
            _schema_leaves.emplace(element.position, &element);
        });
        // write the leading magic bytes
        co_await write_iobuf(iobuf::from("PAR1"));
    }

    ss::future<> write_row(group_value row) {
        co_await shred_record(
          _opts.schema, std::move(row), [this](shredded_value sv) {
              return write_value(std::move(sv));
          });
        // TODO: periodically flush the row_group if we're using enough memory
        // or at least flush the pages if we don't want to create too many row
        // groups.
    }

    ss::future<> close() {
        chunked_vector<row_group> row_groups;
        row_groups.push_back(co_await flush_row_group());
        int64_t num_rows = 0;
        for (const auto& rg : row_groups) {
            num_rows += rg.num_rows;
        }
        auto encoded_footer = encode(file_metadata{
          .version = 2,
          .schema = flatten(_opts.schema),
          .num_rows = num_rows,
          .row_groups = std::move(row_groups),
          .key_value_metadata = std::move(_opts.metadata),
          .created_by = fmt::format(
            "Redpanda version {} (build {})", _opts.version, _opts.build),
        });
        size_t footer_size = encoded_footer.size_bytes();
        co_await write_iobuf(std::move(encoded_footer));
        co_await write_iobuf(encode_footer_size(footer_size));
        co_await write_iobuf(iobuf::from("PAR1"));
        co_await _output.close();
    }

private:
    iobuf encode_footer_size(size_t size) {
        iobuf b;
        auto le_size = ss::cpu_to_le(static_cast<uint32_t>(size));
        // NOLINTNEXTLINE(*reinterpret-cast*)
        b.append(reinterpret_cast<const uint8_t*>(&le_size), sizeof(le_size));
        return b;
    }

    ss::future<> write_value(shredded_value sv) {
        auto& col = _column_writers.at(sv.schema_element_position);
        col.add(std::move(sv.val), sv.rep_level, sv.def_level);
        return ss::now();
    }

    ss::future<row_group> flush_row_group() {
        row_group rg{};
        rg.file_offset = static_cast<int64_t>(_offset);
        for (auto& [pos, col] : _column_writers) {
            const schema_element* leaf = _schema_leaves.at(pos);
            auto page = col.flush_page();
            const auto& data_header = std::get<data_page_header>(
              page.header.type);
            rg.num_rows = data_header.num_rows;
            auto page_size = static_cast<int64_t>(page.serialized.size_bytes());
            rg.total_byte_size += page_size;
            rg.columns.push_back(column_chunk{
              .meta_data = column_meta_data{
                .type = leaf->type,
                .encodings = {data_header.data_encoding},
                .path_in_schema = leaf->path.copy(),
                .codec = compression_codec::uncompressed,
                .num_values = data_header.num_values,
                .total_uncompressed_size = page.header.uncompressed_page_size + page.serialized_header_size,
                .total_compressed_size = page.header.compressed_page_size + page.serialized_header_size,
                .key_value_metadata = {},
                .data_page_offset = static_cast<int64_t>(_offset),
              },
            });
            co_await write_iobuf(std::move(page.serialized));
        }
        co_return rg;
    }

    ss::future<> write_iobuf(iobuf b) {
        _offset += b.size_bytes();
        co_await write_iobuf_to_output_stream(std::move(b), _output);
    }

    options _opts;
    ss::output_stream<char> _output;
    size_t _offset = 0;
    contiguous_range_map<int32_t, schema_element*> _schema_leaves;
    contiguous_range_map<int32_t, column_writer> _column_writers;
};

writer::writer(options opts, ss::output_stream<char> output)
  : _impl(std::make_unique<writer::impl>(std::move(opts), std::move(output))) {}

writer::writer(writer&&) noexcept = default;
writer& writer::operator=(writer&&) noexcept = default;
writer::~writer() noexcept = default;

ss::future<> writer::init() { return _impl->init(); }

ss::future<> writer::write_row(group_value row) {
    return _impl->write_row(std::move(row));
}

ss::future<> writer::close() { return _impl->close(); }

} // namespace serde::parquet
