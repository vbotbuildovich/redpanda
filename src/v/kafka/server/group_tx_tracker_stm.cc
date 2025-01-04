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

#include "kafka/server/group_tx_tracker_stm.h"

namespace kafka {

static constexpr std::chrono::milliseconds max_permissible_tx_timeout{15min};

group_tx_tracker_stm::group_tx_tracker_stm(
  ss::logger& logger,
  raft::consensus* raft,
  ss::sharded<features::feature_table>& feature_table)
  : raft::persisted_stm<>("group_tx_tracker_stm.snapshot", logger, raft)
  , group_data_parser<group_tx_tracker_stm>()
  , _feature_table(feature_table)
  , _serializer(make_consumer_offsets_serializer()) {}

void group_tx_tracker_stm::maybe_add_tx_begin_offset(
  model::record_batch_type fence_type,
  kafka::group_id group,
  model::producer_identity pid,
  model::offset offset,
  model::timestamp ts,
  model::timeout_clock::duration tx_timeout) {
    auto it = _all_txs.find(group);
    if (it == _all_txs.end()) {
        vlog(
          klog.debug,
          "[{}] group not found, ignoring fence of type: {} at offset: {}",
          group,
          fence_type,
          offset);
        return;
    }
    it->second.maybe_add_tx_begin(
      group, fence_type, pid, offset, ts, tx_timeout);
}

void group_tx_tracker_stm::maybe_end_tx(
  kafka::group_id group, model::producer_identity pid, model::offset offset) {
    auto it = _all_txs.find(group);
    if (it == _all_txs.end()) {
        vlog(
          klog.debug,
          "[{}] group not found, ignoring end transaction for pid: {} at "
          "offset: {}",
          group,
          pid,
          offset);
        return;
    }
    auto& group_data = it->second;
    auto p_it = group_data.producer_states.find(pid);
    if (p_it == group_data.producer_states.end()) {
        vlog(
          klog.debug,
          "[{}] ignoring end transaction, no in progress transaction for pid: "
          "{} at offset: {}",
          group,
          pid,
          offset);
        return;
    }
    group_data.begin_offsets.erase(p_it->second.begin_offset);
    group_data.producer_states.erase(p_it);
    group_data.producer_to_begin_deprecated.erase(pid);
}

ss::future<> group_tx_tracker_stm::do_apply(const model::record_batch& b) {
    auto holder = _gate.hold();
    co_await parse(b.copy());
}

model::offset group_tx_tracker_stm::max_collectible_offset() {
    auto result = last_applied_offset();
    for (const auto& [_, group_state] : _all_txs) {
        if (!group_state.begin_offsets.empty()) {
            result = std::min(
              result, model::prev_offset(*group_state.begin_offsets.begin()));
        }
    }
    return result;
}

ss::future<> group_tx_tracker_stm::apply_local_snapshot(
  raft::stm_snapshot_header, iobuf&& snap_buf) {
    auto holder = _gate.hold();
    iobuf_parser parser(std::move(snap_buf));
    auto snap = co_await serde::read_async<snapshot>(parser);
    _all_txs = std::move(snap.transactions);
}

ss::future<raft::stm_snapshot>
group_tx_tracker_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    auto holder = _gate.hold();
    // Copy over the snapshot state for a consistent view.
    auto offset = last_applied_offset();
    snapshot snap;
    snap.transactions = _all_txs;
    iobuf snap_buf;
    apply_units.return_all();
    co_await serde::write_async(snap_buf, snap);
    // snapshot versioning handled via serde.
    co_return raft::stm_snapshot::create(0, offset, std::move(snap_buf));
}

ss::future<> group_tx_tracker_stm::apply_raft_snapshot(const iobuf&) {
    // Transaction commit/abort ensures the data structures are cleaned
    // up and bounded in size and all the open transactions are only
    // in the non evicted part of the log, so nothing to do.
    return ss::now();
}

ss::future<iobuf> group_tx_tracker_stm::take_snapshot(model::offset) {
    return ss::make_ready_future<iobuf>(iobuf());
}

ss::future<> group_tx_tracker_stm::handle_raft_data(model::record_batch batch) {
    co_await model::for_each_record(batch, [this](model::record& r) {
        auto record_type = _serializer.get_metadata_type(r.key().copy());
        switch (record_type) {
        case offset_commit:
        case noop:
            return;
        case group_metadata:
            handle_group_metadata(
              _serializer.decode_group_metadata(std::move(r)));
            return;
        }
        __builtin_unreachable();
    });
}

void group_tx_tracker_stm::handle_group_metadata(group_metadata_kv md) {
    if (md.value) {
        vlog(klog.trace, "[group: {}] update", md.key.group_id);
        // A group may checkpoint periodically as the member's state changes,
        // here we retain the group state if the group already exists.
        _all_txs.try_emplace(md.key.group_id, per_group_state{});
    } else {
        vlog(klog.trace, "[group: {}] tombstone", md.key.group_id);
        // A tombstone indicates all the group state can be purged and
        // any transactions can be ignored. Although care must be taken
        // to ensure there are no open transactions before tombstoning
        // a group in the main state machine.
        _all_txs.erase(md.key.group_id);
    }
}

ss::future<> group_tx_tracker_stm::handle_tx_offsets(
  model::record_batch_header, kafka::group_tx::offsets_metadata) {
    // Transaction boundaries are determined by fence/commit or abort
    // batches
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_fence_v0(
  model::record_batch_header header, kafka::group_tx::fence_metadata_v0 fence) {
    // fence_v0 has no timeout, use a max permissible timeout.
    // fence_v0 has been deprecated for long, this is not a problem in practice
    auto timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
      max_permissible_tx_timeout);
    maybe_add_tx_begin_offset(
      header.type,
      std::move(fence.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset,
      header.max_timestamp,
      timeout);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_fence_v1(
  model::record_batch_header header, kafka::group_tx::fence_metadata_v1 fence) {
    maybe_add_tx_begin_offset(
      header.type,
      std::move(fence.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset,
      header.max_timestamp,
      fence.transaction_timeout_ms);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_fence(
  model::record_batch_header header, kafka::group_tx::fence_metadata fence) {
    maybe_add_tx_begin_offset(
      header.type,
      std::move(fence.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset,
      header.max_timestamp,
      fence.transaction_timeout_ms);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_abort(
  model::record_batch_header header, kafka::group_tx::abort_metadata data) {
    maybe_end_tx(
      std::move(data.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_commit(
  model::record_batch_header header, kafka::group_tx::commit_metadata data) {
    auto pid = model::producer_identity{
      header.producer_id, header.producer_epoch};
    maybe_end_tx(std::move(data.group_id), pid, header.base_offset);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_version_fence(
  features::feature_table::version_fence) {
    // ignore
    return ss::now();
}

bool group_tx_tracker_stm_factory::is_applicable_for(
  const storage::ntp_config& config) const {
    const auto& ntp = config.ntp();
    return ntp.ns == model::kafka_consumer_offsets_nt.ns
           && ntp.tp.topic == model::kafka_consumer_offsets_nt.tp;
}

group_tx_tracker_stm_factory::group_tx_tracker_stm_factory(
  ss::sharded<features::feature_table>& feature_table)
  : _feature_table(feature_table) {}

void group_tx_tracker_stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<kafka::group_tx_tracker_stm>(
      klog, raft, _feature_table);
    raft->log()->stm_manager()->add_stm(stm);
}

void group_tx_tracker_stm::per_group_state::maybe_add_tx_begin(
  const kafka::group_id& group,
  model::record_batch_type fence_type,
  model::producer_identity pid,
  model::offset offset,
  model::timestamp begin_ts,
  model::timeout_clock::duration tx_timeout) {
    auto it = producer_states.find(pid);
    if (it == producer_states.end()) {
        auto p_state = producer_tx_state{
          .fence_type = fence_type,
          .begin_offset = offset,
          .batch_ts = begin_ts,
          .timeout = tx_timeout};
        if (p_state.expired_deprecated_fence_tx()) {
            vlog(
              klog.debug,
              "[{}] Ignoring stale tx_fence batch at offset: {}, considering "
              "it expired",
              group,
              offset);
            return;
        }
        vlog(
          klog.debug,
          "[{}] Adding begin tx : {}, pid: {} at offset: {}, ts: {} with "
          "timeout: {}",
          group,
          fence_type,
          pid,
          offset,
          begin_ts,
          tx_timeout);
        begin_offsets.emplace(offset);
        producer_states[pid] = p_state;
        producer_to_begin_deprecated[pid] = offset;
    }
}

bool group_tx_tracker_stm::producer_tx_state::expired_deprecated_fence_tx()
  const {
    // A bug in 24.2.0 resulted in a situation where tx_fence
    // batches were retained _after_ compaction while their corresponding
    // data/commit/abort batches were compacted away. This applied to
    // only group transactions that used tx_fence to begin the
    // transaction.
    // After this buggy compaction, these uncleaned tx_fence batches are
    // accounted as open transactions when computing
    // max_collectible_offset thus blocking further compaction after
    // upgrade to 24.2.x.
    if (fence_type != model::record_batch_type::tx_fence) {
        return false;
    }
    // note: this is a heuristic to ignore any transactions that have long been
    // expired and we do not want them to block max collectible offset.
    // clamp the timeout, incase timeout is unset
    auto max_timeout
      = std::chrono::duration_cast<model::timeout_clock::duration>(
        2 * max_permissible_tx_timeout);
    auto clamped_timeout = std::min(max_timeout, 2 * timeout);
    return model::timestamp_clock::now()
           > model::to_time_point(batch_ts) + clamped_timeout;
}

} // namespace kafka
