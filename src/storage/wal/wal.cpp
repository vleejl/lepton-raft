#include "storage/wal/wal.h"

#include <raft.pb.h>

#include <asio/use_future.hpp>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <format>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <tl/expected.hpp>
#include <utility>
#include <vector>

#include "basic/defer.h"
#include "basic/failpoint_async.h"
#include "basic/logger.h"
#include "basic/pbutil.h"
#include "basic/time.h"
#include "coroutine/coro_macros.h"
#include "error/error.h"
#include "error/expected.h"
#include "error/io_error.h"
#include "error/leaf.h"
#include "error/leaf_expected.h"
#include "error/logic_error.h"
#include "error/wal_error.h"
#include "fmt/format.h"
#include "magic_enum.hpp"
#include "raft_core/pb/protobuf.h"
#include "raft_core/raw_node.h"
#include "storage/fileutil/directory.h"
#include "storage/fileutil/file_reader.h"
#include "storage/fileutil/locked_file_endpoint.h"
#include "storage/fileutil/path.h"
#include "storage/fileutil/read_dir.h"
#include "storage/ioutil/reader.h"
#include "storage/pb/types.h"
#include "storage/pb/wal_protobuf.h"
#include "storage/wal/decoder.h"
#include "storage/wal/wal.h"
#include "storage/wal/wal_file.h"
#include "v4/proxy.h"
#include "wal.pb.h"
namespace lepton::storage::wal {

constexpr auto WARN_SYNC_DURATION = std::chrono::seconds{1};

std::vector<pro::proxy_view<ioutil::reader>> to_reader_views(std::vector<wal_segment>& files) {
  std::vector<pro::proxy_view<ioutil::reader>> result;
  result.reserve(files.size());

  for (auto& seg : files) {
    std::visit(
        [&](auto& f_ptr) {
          // f_ptr 是 unique_ptr<file_reader> 或 unique_ptr<locked_file_endpoint>
          if (f_ptr) {                         // 安全检查
            result.emplace_back(f_ptr.get());  // 注意这里要 .get()
          }
        },
        seg);
  }

  return result;
}

bool exist_wal(const std::string& dir) {
  auto read_dir_result = fileutil::read_dir(dir, fileutil::read_dir_op::with_ext(".wal"));
  if (!read_dir_result) {
    return false;
  }
  auto& entries = read_dir_result.value();
  return !entries.empty();
}

leaf::result<std::pair<std::uint64_t, std::uint64_t>> parse_wal_name(std::string_view str) {
  // 检查后缀是否为 .wal (C++20 ends_with)
  if (!str.ends_with(".wal")) {
    return new_error(logic_error::INVALID_FORMAT, "bad WAL name: missing .wal suffix");
  }

  // 注意：sscanf 需要 null-terminated 字符串，所以如果是 string_view 需要转换或确保格式
  uint64_t seq = 0;
  uint64_t index = 0;

  // 预期的格式长度通常是 16 + 1 + 16 + 4 = 37 字符
  // %16llx 解析 16 位十六进制
  int matched = std::sscanf(str.data(), "%16" SCNx64 "-%16" SCNx64 ".wal", &seq, &index);

  if (matched != 2) {
    return new_error(logic_error::INVALID_FORMAT, "bad WAL name: invalid format");
  }

  return {seq, index};
}

/**
 * 检查 WAL 文件序列号是否连续增加。
 * names 必须预先根据序列号排序。
 */
bool is_valid_seq(std::span<const std::string> names) {
  uint64_t last_seq = 0;

  for (const auto& name : names) {
    // 解析文件名，提取序列号 (seq)
    auto parse_result = parse_wal_name(name);
    if (!parse_result) {
      LOG_CRITICAL("failed to parse WAL file name: {}", name);
      continue;
    }
    auto [cur_seq, _] = parse_result.value();

    // 如果不是第一个文件，检查当前序列号是否为上一个序列号 + 1
    if (last_seq != 0 && cur_seq != last_seq + 1) {
      return false;
    }

    last_seq = cur_seq;
  }

  return true;
}

leaf::result<int> search_index(const std::vector<std::string>& names, uint64_t index) {
  // 从后往前遍历，寻找第一个满足 index >= curIndex 的位置
  for (int i = static_cast<int>(names.size()) - 1; i >= 0; --i) {
    const std::string& name = names[static_cast<size_t>(i)];
    auto parse_result = parse_wal_name(name);
    if (!parse_result) {
      LOG_CRITICAL("failed to parse WAL file name: {}", name);
      continue;
    }
    auto [_, cur_index] = parse_result.value();
    if (index >= cur_index) {
      return i;
    }
  }
  return new_error(wal_error::ERR_NO_MATCHING_SEGMENT);
}

/**
 * 过滤并返回有效的 WAL 文件名列表
 */
std::vector<std::string> check_wal_names(const std::vector<std::string>& names) {
  std::vector<std::string> wnames;

  for (const auto& name : names) {
    // 尝试解析 WAL 文件名
    if (!parse_wal_name(name)) {
      // 检查是否以 ".tmp" 结尾
      if (!name.ends_with(".tmp")) {
        LOG_WARN("ignored file in WAL directory: {}", name);
      }
      continue;
    }

    // 解析成功，加入结果集
    wnames.push_back(name);
  }

  return wnames;
}

leaf::result<std::vector<std::string>> read_wal_names(const std::string& dir) {
  BOOST_LEAF_AUTO(entries, fileutil::read_dir(dir));
  auto wnames = check_wal_names(entries);
  if (wnames.empty()) {
    LOG_ERROR("no WAL files found in directory: {}", dir);
    return new_error(logic_error::ERR_FILE_NOT_FOUND, fmt::format("no WAL files found in directory: {}", dir));
  }
  return wnames;
}

leaf::result<std::pair<std::vector<std::string>, uint64_t>> select_wal_files(const std::string& dir,
                                                                             std::uint64_t snap_index) {
  BOOST_LEAF_AUTO(wnames, read_wal_names(dir));
  BOOST_LEAF_AUTO(name_index, search_index(wnames, snap_index));
  if (!is_valid_seq(std::span(wnames).subspan(static_cast<std::size_t>(name_index)))) {
    return new_error(
        wal_error::ERR_INCONTINUOUS_SEQUENCE,
        fmt::format("wal: file sequence numbers (starting from {}) do not increase continuously", name_index));
  }
  return {wnames, static_cast<uint64_t>(name_index)};
}

// openWALFiles when write is false
leaf::result<std::vector<wal_segment>> open_readonly_wal_files(asio::any_io_executor executor, const std::string& dir,
                                                               std::span<const std::string> names) {
  std::vector<wal_segment> file_readers;
  for (const auto& name : names) {
    auto filepath = fileutil::join_paths(dir, name);
    BOOST_LEAF_AUTO(file_handle, fileutil::create_file_reader(executor, filepath));
    file_readers.push_back(std::move(file_handle));
  }
  return file_readers;
}

// openWALFiles when write is true
leaf::result<std::vector<wal_segment>> open_locked_wal_file(rocksdb::Env* env, asio::any_io_executor executor,
                                                            const std::string& dir,
                                                            std::span<const std::string> names) {
  std::vector<wal_segment> lock_files;
  for (const auto& name : names) {
    auto filepath = fileutil::join_paths(dir, name);
    BOOST_LEAF_AUTO(file_handle,
                    fileutil::create_locked_file_endpoint(env, executor, filepath, asio::file_base::read_write));
    lock_files.push_back(std::move(file_handle));
  }
  return lock_files;
}

leaf::result<wal_handle> open_at_index(rocksdb::Env* env, asio::any_io_executor executor, const std::string& dir,
                                       std::size_t segment_size_bytes, pb::snapshot&& snap, bool write,
                                       std::shared_ptr<lepton::logger_interface> logger) {
  BOOST_LEAF_AUTO(wal_files_pair, select_wal_files(dir, snap.index()));
  auto& [wal_names, start_index] = wal_files_pair;
  assert(start_index < wal_names.size());
  std::span<const std::string> names = {wal_names.data() + start_index, wal_names.size() - start_index};
  if (write) {
    BOOST_LEAF_AUTO(lock_files, open_locked_wal_file(env, executor, dir, names));
    assert(!lock_files.empty());
    BOOST_LEAF_AUTO(file_pipeline_handle, new_file_pipeline(executor, env, dir, segment_size_bytes, logger));
    auto wal_handle = std::make_shared<wal>(env, executor, dir, segment_size_bytes, std::move(snap),
                                            std::move(lock_files), std::move(file_pipeline_handle), logger);
    LOG_TRACE("wal tail file name: {}, decoder last crc: {}", wal_handle->tail()->name(),
              wal_handle->decoder_->last_crc());
    BOOST_LEAF_AUTO(_, parse_wal_name(fileutil::base_name(wal_handle->tail()->name())));
    return wal_handle;
  } else {
    BOOST_LEAF_AUTO(file_readers, open_readonly_wal_files(executor, dir, names));
    return std::make_shared<wal>(env, executor, dir, segment_size_bytes, std::move(snap), std::move(file_readers),
                                 nullptr, logger);
  }
}

leaf::result<wal_handle> open(rocksdb::Env* env, asio::any_io_executor executor, const std::string& dir,
                              std::size_t segment_size_bytes, pb::snapshot&& snap,
                              std::shared_ptr<lepton::logger_interface> logger) {
  BOOST_LEAF_AUTO(wal_handle, open_at_index(env, executor, dir, segment_size_bytes, std::move(snap), true, logger));
  BOOST_LEAF_AUTO(dir_handle, fileutil::new_directory(env, dir));
  wal_handle->set_directory(std::move(dir_handle));
  return wal_handle;
}

leaf::result<wal_handle> open_for_read(rocksdb::Env* env, asio::any_io_executor executor, const std::string& dir,
                                       std::size_t segment_size_bytes, pb::snapshot&& snap,
                                       std::shared_ptr<lepton::logger_interface> logger) {
  return open_at_index(env, executor, dir, segment_size_bytes, std::move(snap), false, logger);
}

asio::awaitable<expected<raftpb::HardState>> verify(asio::any_io_executor executor, const std::string& dir,
                                                    const pb::snapshot& snap,
                                                    std::shared_ptr<lepton::logger_interface> logger) {
  auto file_readers_result = leaf_to_expected([&]() -> leaf::result<std::vector<wal_segment>> {
    BOOST_LEAF_AUTO(wal_files_pair, select_wal_files(dir, snap.index()));
    auto& [wal_names, start_index] = wal_files_pair;
    BOOST_LEAF_AUTO(file_readers, open_readonly_wal_files(executor, dir, wal_names));
    return file_readers;
  });
  if (!file_readers_result) {
    LOGGER_ERROR(logger, "create decoder failed, error:{}", file_readers_result.error().message());
    co_return tl::unexpected(file_readers_result.error());
  }
  auto file_readers = std::move(file_readers_result.value());
  // create a new decoder from the readers on the WAL files
  auto decoder_handle = std::make_unique<decoder>(executor, logger, to_reader_views(file_readers));

  walpb::Record rec;
  std::string metadata;
  std::error_code ec;
  auto match = false;
  raftpb::HardState state;
  while (!ec) {
    auto result = co_await decoder_handle->decode_record(rec);
    if (!result) {
      ec = result.error();
      continue;
    }

    switch (rec.type()) {
      case walpb::METADATA_TYPE: {
        if ((!metadata.empty()) && metadata != rec.data()) {
          co_return unexpected(wal_error::ERR_METADATA_CONFLICT);
        }
        metadata = rec.data();
        break;
      }
        // We ignore all entry and state type records as these
        // are not necessary for validating the WAL contents
      case walpb::ENTRY_TYPE: {
        break;
      }
      case walpb::STATE_TYPE: {
        protobuf_must_parse(rec.data(), state, *logger);
        break;
      }
      case walpb::CRC_TYPE: {
        auto crc = decoder_handle->last_crc();
        // Current crc of decoder must match the crc of the record.
        // We need not match 0 crc, since the decoder is a new one at this point.
        if ((crc != 0) && !pb::validate_rec_crc(rec, crc)) {
          co_return unexpected(wal_error::ERR_CRC_MISMATCH);
        }
        decoder_handle->update_crc(rec.crc());
        break;
      }
      case walpb::SNAPSHOT_TYPE: {
        pb::snapshot last_snap;
        protobuf_must_parse(rec.data(), last_snap, *logger);
        if (last_snap.index() == snap.index()) {
          if (last_snap.term() != snap.term()) {
            co_return unexpected(wal_error::ERR_SNAPSHOT_MISMATCH);
          }
          match = true;
        }
        break;
      }
      default: {
        co_return unexpected(wal_error::ERR_REC_TYPE_INVALID);
        break;
      }
    }
  }

  // We do not have to read out all the WAL entries
  // as the decoder is opened in read mode.
  if (ec != io_error::IO_EOF && ec != io_error::UNEXPECTED_EOF) {
    co_return tl::unexpected(ec);
  }

  if (!match) {
    co_return unexpected(wal_error::ERR_SNAPSHOT_NOT_FOUND);
  }

  co_return state;
}

asio::awaitable<expected<pb::repeated_snapshot>> valid_snapshot_entries(
    asio::any_io_executor executor, const std::string& wal_dir, std::shared_ptr<lepton::logger_interface> logger) {
  auto file_readers_result = leaf_to_expected([&]() -> leaf::result<std::vector<wal_segment>> {
    BOOST_LEAF_AUTO(wal_names, read_wal_names(wal_dir));
    BOOST_LEAF_AUTO(file_readers, open_readonly_wal_files(executor, wal_dir, wal_names));
    return file_readers;
  });
  if (!file_readers_result) {
    LOGGER_ERROR(logger, "open readonly wal files failed, error:{}", file_readers_result.error().message());
    co_return tl::unexpected(file_readers_result.error());
  }

  auto file_readers = std::move(file_readers_result.value());
  // create a new decoder from the readers on the WAL files
  auto decoder_handle = std::make_unique<decoder>(executor, logger, to_reader_views(file_readers));

  pb::repeated_snapshot snaps;
  walpb::Record rec;
  std::error_code ec;
  raftpb::HardState state;
  while (!ec) {
    auto result = co_await decoder_handle->decode_record(rec);
    if (!result) {
      ec = result.error();
      continue;
    }

    switch (rec.type()) {
      case walpb::METADATA_TYPE: {
        break;
      }
      case walpb::ENTRY_TYPE: {
        break;
      }
      case walpb::STATE_TYPE: {
        protobuf_must_parse(rec.data(), state, *logger);
        break;
      }
      case walpb::CRC_TYPE: {
        auto crc = decoder_handle->last_crc();
        // current crc of decoder must match the crc of the record.
        // do no need to match 0 crc, since the decoder is a new one at this case.
        if ((crc != 0) && !pb::validate_rec_crc(rec, crc)) {
          co_return unexpected(wal_error::ERR_CRC_MISMATCH);
        }
        decoder_handle->update_crc(rec.crc());
        break;
      }
      case walpb::SNAPSHOT_TYPE: {
        pb::snapshot lodaed_snap;
        protobuf_must_parse(rec.data(), lodaed_snap, *logger);
        snaps.Add(std::move(lodaed_snap));
        break;
      }
      default: {
        co_return unexpected(wal_error::ERR_REC_TYPE_INVALID);
        break;
      }
    }
  }
  // We do not have to read out all the WAL entries
  // as the decoder is opened in read mode.
  if (ec != io_error::IO_EOF && ec != io_error::UNEXPECTED_EOF) {
    co_return tl::unexpected(ec);
  }
  auto it = std::remove_if(snaps.begin(), snaps.end(), [&](const pb::snapshot& s) {
    return s.index() > state.commit();  // filter out any snaps that are newer than the committed hardstate
  });
  snaps.erase(it, snaps.end());
  co_return snaps;
}

asio::awaitable<expected<wal_handle>> create_wal(rocksdb::Env* env, asio::any_io_executor executor,
                                                 const std::string& dirpath, const std::string& metadata,
                                                 const std::size_t segment_size_bytes,
                                                 std::shared_ptr<lepton::logger_interface> logger) {
  if (exist_wal(dirpath)) {
    LOGGER_ERROR(logger, "dirpath {} already exists", dirpath);
    co_return unexpected(io_error::PARH_HAS_EXIT);
  }

  // keep temporary wal directory so WAL initialization appears atomic
  const auto temp_dir_path = fmt::format("{}.tmp", dirpath);
  if (fileutil::path_exist(temp_dir_path)) {
    if (auto s = leaf_to_expected([&]() { return fileutil::remove_all(temp_dir_path); }); !s.has_value()) {
      LOGGER_ERROR(logger, "Failed to delete existing temp dir {}: {}", temp_dir_path, s.error().message());
      co_return tl::unexpected(s.error());
    }
  }

  DEFER({ (void)fileutil::remove_all(temp_dir_path); });

  auto wal_handle_result = leaf_to_expected([&]() -> leaf::result<wal_handle> {
    LEPTON_LEAF_CHECK(fileutil::create_dir_all(env, temp_dir_path));
    auto wal_file_path = std::filesystem::path(temp_dir_path) / wal_file_name(0, 0);
    BOOST_LEAF_AUTO(wal_file_handle, create_new_wal_file(env, executor, wal_file_path.string(), false));
    BOOST_LEAF_AUTO(_, wal_file_handle->seek_end());
    LEPTON_LEAF_CHECK(wal_file_handle->preallocate(segment_size_bytes, true));
    BOOST_LEAF_AUTO(encoder, new_file_encoder(executor, wal_file_handle.get(), 0, logger));
    auto wal_handle =
        std::make_shared<wal>(executor, env, std::move(encoder), dirpath, metadata, segment_size_bytes, logger);
    wal_handle->append_lock_file(std::move(wal_file_handle));
    return wal_handle;
  });
  if (!wal_handle_result) {
    LOGGER_ERROR(logger, "Failed to create wal in temp dir {}: {}", temp_dir_path, wal_handle_result.error().message());
    co_return tl::unexpected(wal_handle_result.error());
  }
  auto wal_handle = std::move(wal_handle_result.value());
  CO_CHECK_AWAIT(wal_handle->save_crc(0));

  walpb::Record record;
  record.set_type(::walpb::RecordType::METADATA_TYPE);
  record.set_data(metadata);
  CO_CHECK_AWAIT(wal_handle->encode(record));
  CO_CHECK_AWAIT(wal_handle->save_snapshot(pb::snapshot{}));

  if (auto result = leaf_to_expected([&]() -> leaf::result<void> { return wal_handle->rename_wal(temp_dir_path); });
      !result.has_value()) {
    LOGGER_WARN(logger, "Failed to rename wal from {} to {}: {}", temp_dir_path, dirpath, result.error().message());
    co_return tl::unexpected{result.error()};
  }

  std::filesystem::path dir_path(wal_handle->dir());
  auto parent_path = dir_path.parent_path();
  if (auto result = leaf_to_expected([&]() -> leaf::result<void> {
        // directory was renamed; sync parent dir to persist rename
        BOOST_LEAF_AUTO(parent_dir, fileutil::new_directory(env, parent_path.string()));
        if (!parent_dir.fsync()) {
          (void)parent_dir.close();
          return new_error(io_error::FSYNC_FAILED, fmt::format("Failed to fsync parent dir {}", parent_path.string()));
        }
        LEPTON_LEAF_CHECK(parent_dir.close());
        return {};
      });
      !result.has_value()) {
    LOGGER_WARN(logger, "Failed to fsync parent directory {}", parent_path.string());
    wal_handle->cleanup_wal();
    co_return tl::unexpected{result.error()};
  }
  co_return wal_handle;
}

wal::wal(rocksdb::Env* env, asio::any_io_executor executor, const std::string& dir_path,
         const std::size_t segment_size_bytes, pb::snapshot&& snap, std::vector<wal_segment>&& segments,
         file_pipeline_handle&& file_pipeline, std::shared_ptr<lepton::logger_interface> logger)
    : executor_(executor),
      strand_(executor),
      env_(env),
      dir_(dir_path),
      segment_size_bytes_(segment_size_bytes),
      start_(std::move(snap)),
      segments_(std::move(segments)),
      file_pipeline_(std::move(file_pipeline)),
      logger_(std::move(logger)) {
  assert(!segments_.empty());
  auto readers = to_reader_views(segments_);
  assert(!readers.empty());
  decoder_ = std::make_unique<decoder>(executor, logger_, readers);
}

asio::awaitable<expected<wal::read_result>> wal::read_all() {
  ASIO_ENSURE_STRAND(strand_)

  if (decoder_ == nullptr) {
    co_return unexpected(wal_error::ERR_DECODER_NOT_FOUND);
  }

  std::error_code ec;
  auto match = false;
  read_result ret_result;
  auto& metadata = ret_result.metadata;
  auto& entries = ret_result.entries;
  auto& hard_state = ret_result.state;
  while (!ec) {
    walpb::Record rec;
    if (auto result = co_await decoder_->decode_record(rec); !result) {
      ec = result.error();
      continue;
    }
    LOGGER_TRACE(logger_, "decode record content: {}", rec.DebugString());

    switch (rec.type()) {
      case walpb::METADATA_TYPE: {
        if ((!metadata.empty()) && metadata != rec.data()) {
          co_return unexpected(wal_error::ERR_METADATA_CONFLICT);
        }
        metadata = rec.data();
        break;
      }
      case walpb::ENTRY_TYPE: {
        raftpb::Entry entry;
        protobuf_must_parse(rec.data(), entry, *logger_);
        // 0 <= e.Index-w.start.Index - 1 < len(ents)
        if (entry.index() > start_.index()) {
          // prevent "panic: runtime error: slice bounds out of range [:13038096702221461992] with capacity 0"
          // - 背景：在 Raft 中，如果 Leader 崩溃，新 Leader 可能会发送索引相同但 Term
          // 不同的日志，覆盖掉旧的未提交日志。
          // - 做法：ents[:offset] 会丢弃掉 offset 之后的所有旧数据，然后把最新的 e 加上去。这保证了 ents
          // 始终保存的是最新的、有效的日志流。

          // index 从 1 开始，数组下标从 0 开始
          auto diff = entry.index() - start_.index() - 1;
          if (diff >= static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            // 索引太大，超过 int 表示范围
            LOGGER_ERROR(logger_,
                         "{}, snapshot[Index: {}, Term: {}], current entry[Index: {}, Term: {}], len(ents): {}",
                         magic_enum::enum_name(wal_error::ERR_SLICE_OUT_OF_RANGE), start_.index(), start_.term(),
                         entry.index(), entry.term(), entries.size());
            co_return unexpected(wal_error::ERR_SLICE_OUT_OF_RANGE);
          }
          auto offset = static_cast<int>(diff);
          assert(offset >= 0);
          const auto entries_size = entries.size();
          if (offset > entries_size) {
            // return error before append call causes runtime panic.
            // We still return the continuous WAL entries that have already been read.
            // Refer to https://github.com/etcd-io/etcd/pull/19038#issuecomment-2557414292.
            LOGGER_ERROR(logger_,
                         "{}, snapshot[Index: {}, Term: {}], current entry[Index: {}, Term: {}], len(ents): {}",
                         magic_enum::enum_name(wal_error::ERR_SLICE_OUT_OF_RANGE), start_.index(), start_.term(),
                         entry.index(), entry.term(), entries_size);
            co_return unexpected(wal_error::ERR_SLICE_OUT_OF_RANGE);
          } else if (offset == entries_size) {
            entry_index_ = entry.index();
            entries.Add(std::move(entry));
          } else {
            // The line below is potentially overriding some 'uncommitted' entries.
            // TODO(veejl): use protobuf 3
            entry_index_ = entry.index();
            entries.Mutable(offset)->Swap(&entry);
            entries.DeleteSubrange(offset + 1, entries_size - offset - 1);
          }
        }
        break;
      }
      case walpb::STATE_TYPE: {
        protobuf_must_parse(rec.data(), hard_state, *logger_);
        break;
      }
      case walpb::CRC_TYPE: {
        auto crc = decoder_->last_crc();
        // Current crc of decoder must match the crc of the record.
        // We need not match 0 crc, since the decoder is a new one at this point.
        if ((crc != 0) && !pb::validate_rec_crc(rec, crc)) {
          hard_state.Clear();
          co_return unexpected(wal_error::ERR_CRC_MISMATCH);
        }
        decoder_->update_crc(rec.crc());
        break;
      }
      case walpb::SNAPSHOT_TYPE: {
        pb::snapshot snap;
        protobuf_must_parse(rec.data(), snap, *logger_);
        if (snap.index() == start_.index()) {
          if (snap.term() != start_.term()) {
            hard_state.Clear();
            co_return unexpected(wal_error::ERR_SNAPSHOT_MISMATCH);
          }
          match = true;
        }
        break;
      }
      default: {
        hard_state.Clear();
        co_return unexpected(wal_error::ERR_REC_TYPE_INVALID);
        break;
      }
    }
  }

  if (tail() == nullptr) {
    // We do not have to read out all entries in read mode.
    // The last record maybe a partial written one, so
    // `io.ErrUnexpectedEOF` might be returned.
    if (ec != io_error::IO_EOF && ec != io_error::UNEXPECTED_EOF) {
      hard_state.Clear();
      co_return tl::unexpected(ec);
    }
  } else {
    // We must read all the entries if WAL is opened in write mode.
    if (ec != io_error::IO_EOF) {
      hard_state.Clear();
      co_return tl::unexpected(ec);
    }
    // decodeRecord() will return io.EOF if it detects a zero record,
    // but this zero record may be followed by non-zero records from
    // a torn write. Overwriting some of these non-zero records, but
    // not all, will cause CRC errors on WAL open. Since the records
    // were never fully synced to disk in the first place, it's safe
    // to zero them out to avoid any CRC errors from new writes.
    assert(tail());
    if (auto ret = leaf_to_expected([&]() { return tail()->seek_start(decoder_->last_valid_off()); }); !ret) {
      LOGGER_ERROR(logger_, "seek start failed, error:{}", ret.error().message());
      co_return tl::unexpected(ret.error());
    }
    if (auto ret = leaf_to_expected([&]() { return tail()->zero_to_end(); }); !ret) {
      LOGGER_ERROR(logger_, "zero_to_end failed, error:{}", ret.error().message());
      co_return tl::unexpected(ret.error());
    }
  }
  ec = std::error_code{};
  if (!match) {
    ret_result.ec = make_error_code(wal_error::ERR_SNAPSHOT_NOT_FOUND);
  }
  // close decoder, disable reading
  start_.Clear();
  metadata_ = metadata;
  if (tail() != nullptr) {
    // create encoder (chain crc with the decoder), enable appending
    if (auto ret = leaf_to_expected([&]() -> leaf::result<void> {
          BOOST_LEAF_AUTO(encoder, new_file_encoder(executor_, tail(), decoder_->last_crc(), logger_));
          encoder_ = std::move(encoder);
          return {};
        });
        !ret) {
      LOGGER_ERROR(logger_, "new_file_encoder failed, error:{}", ret.error().message());
      co_return tl::unexpected(ret.error());
    }
  }
  decoder_ = nullptr;
  co_return ret_result;
}

asio::awaitable<expected<void>> wal::save_crc(std::uint32_t prev_crc) {
  walpb::Record record;
  record.set_type(::walpb::RecordType::CRC_TYPE);
  record.set_crc(prev_crc);
  co_return co_await encoder_->encode(record);
}

asio::awaitable<expected<void>> wal::save_snapshot(const pb::snapshot& snapshot) {
  if (auto ret = pb::validate_snapshot_for_write(snapshot); !ret) {
    LOGGER_ERROR(logger_, "validate failed, error:{}", ret.error().message());
    co_return ret;
  }

  ASIO_ENSURE_STRAND(strand_)

  walpb::Record record;
  record.set_type(::walpb::RecordType::SNAPSHOT_TYPE);
  if (snapshot.ByteSizeLong() > 0) {
    record.set_data(must_serialize_to_string(snapshot, *logger_));
  }

  CO_CHECK_AWAIT(encoder_->encode(record));

  // update enti only when snapshot is ahead of last index
  if (entry_index_ < snapshot.index()) {
    entry_index_ = snapshot.index();
  }

  co_return co_await sync();
}

asio::awaitable<expected<void>> wal::save_entry(const raftpb::Entry& entry) {
  walpb::Record record;
  record.set_type(::walpb::RecordType::ENTRY_TYPE);
  if (entry.ByteSizeLong() > 0) {
    record.set_data(must_serialize_to_string(entry, *logger_));
  }
  CO_CHECK_AWAIT(encoder_->encode(record));
  entry_index_ = entry.index();
  co_return ok();
}

asio::awaitable<expected<void>> wal::save_state(raftpb::HardState&& state) {
  if (core::pb::is_empty_hard_state(state)) {
    co_return ok();
  }
  state_ = std::move(state);
  walpb::Record record;
  record.set_type(::walpb::RecordType::STATE_TYPE);
  if (state_.ByteSizeLong() > 0) {
    record.set_data(must_serialize_to_string(state_, *logger_));
  }
  CO_CHECK_AWAIT(encoder_->encode(record));
  co_return ok();
}

asio::awaitable<expected<void>> wal::save(const core::pb::repeated_entry& entries, raftpb::HardState&& state) {
  ASIO_ENSURE_STRAND(strand_)

  // short cut, do not call sync
  if (core::pb::is_empty_hard_state(state) && entries.empty()) {
    co_return ok();
  }

  auto must_sync = core::must_sync(state, state_, entries.size());
  for (const auto& ent : entries) {
    CO_CHECK_AWAIT(save_entry(ent));
  }
  CO_CHECK_AWAIT(save_state(std::move(state)));

  auto seek_curr_result = leaf_to_expected([&]() -> leaf::result<std::uint64_t> { return tail()->seek_curr(); });
  if (!seek_curr_result) {
    LOGGER_ERROR(logger_, "file:{}, seek failed, error:{}", tail()->name(), seek_curr_result.error().message());
    co_return tl::unexpected(seek_curr_result.error());
  }
  auto curr_off = *seek_curr_result;
  if (curr_off < segment_size_bytes_) {
    if (must_sync) {
      FAILPOINT_ASYNC("wal_before_sync", executor_);
      auto sync_result = co_await sync();
      FAILPOINT_ASYNC("wal_after_sync", executor_);
      co_return sync_result;
    }
    co_return ok();
  }

  co_return co_await cut();
}

asio::awaitable<expected<void>> wal::cut() {
  assert(tail());
  // close old wal file; truncate to avoid wasting space if an early cut
  if (auto resize_result = leaf_to_expected([&]() -> leaf::result<void> {
        BOOST_LEAF_AUTO(off, tail()->seek_curr());
        // 释放 wal 文件磁盘空间
        return tail()->truncate(off);
      });
      !resize_result) {
    LOGGER_ERROR(logger_, "file:{}, resize failed, error:{}", tail()->name(), resize_result.error().message());
    co_return resize_result;
  }
  if (auto result = co_await sync(); !result) {
    LOGGER_ERROR(logger_, "sync failed, error:{}", result.error().message());
    co_return result;
  }

  auto fpath = fileutil::join_paths(dir_, wal_file_name(seq() + 1, entry_index_ + 1));
  // create a temp wal file with name sequence + 1, or truncate the existing one
  auto new_tail_result = co_await file_pipeline_->open();
  if (!new_tail_result) {
    LOGGER_ERROR(logger_, "file pipeline open new tail file failed, error: {}", new_tail_result.error().message());
    co_return tl::unexpected(new_tail_result.error());
  }

  // update writer and save the previous crc
  auto prev_crc = encoder_->crc32();
  if (auto result = leaf_to_expected([&]() -> leaf::result<void> {
        append_lock_file(std::move(*new_tail_result));
        BOOST_LEAF_AUTO(encoder, new_file_encoder(executor_, tail(), prev_crc, logger_));
        encoder_ = std::move(encoder);
        return {};
      });
      !result) {
    LOGGER_ERROR(logger_, "update encodeer failed, error:{}", result.error().message());
    co_return result;
  }

  CO_CHECK_AWAIT(save_crc(prev_crc));

  walpb::Record record;
  record.set_type(::walpb::RecordType::METADATA_TYPE);
  record.set_data(metadata_);
  CO_CHECK_AWAIT(encoder_->encode(record));

  if (!core::pb::is_empty_hard_state(state_)) {
    CO_CHECK_AWAIT(save_state(std::move(state_)));
  }

  // atomically move temp wal file to wal file
  CO_CHECK_AWAIT(sync());

  if (auto result = leaf_to_expected([&]() -> leaf::result<void> {
        BOOST_LEAF_AUTO(off, tail()->seek_curr());
        LEPTON_LEAF_CHECK(fileutil::rename(tail()->name(), fpath));
        assert(dir_file_);
        LEPTON_LEAF_CHECK(dir_file_->fsync());
        // reopen newTail with its new path so calls to Name() match the wal filename format
        auto close_result = tail()->close();
        assert(close_result);
        BOOST_LEAF_AUTO(new_tail,
                        fileutil::create_locked_file_endpoint(env_, executor_, fpath, asio::file_base::write_only));
        LEPTON_LEAF_CHECK(new_tail->seek_start(off));
        segments_.back() = std::move(new_tail);
        return {};
      });
      !result) {
    LOGGER_ERROR(logger_, "update last tail file handle failed, error:{}", result.error().message());
    co_return result;
  }

  prev_crc = encoder_->crc32();
  if (auto result = leaf_to_expected([&]() -> leaf::result<void> {
        BOOST_LEAF_AUTO(encoder, new_file_encoder(executor_, tail(), prev_crc, logger_));
        encoder_ = std::move(encoder);
        return {};
      });
      !result) {
    LOGGER_ERROR(logger_, "update encodeer failed, error:{}", result.error().message());
    co_return result;
  }

  LOGGER_INFO(logger_, "created a new WAL segment: {}", fpath);
  co_return ok();
}

asio::awaitable<expected<void>> wal::sync() {
  if (encoder_ != nullptr) {
    CO_CHECK_AWAIT(encoder_->flush());
  }

  if (unsafe_no_sync_) {
    co_return ok();
  }

  auto start = std::chrono::steady_clock::now();
  auto result = tail()->fdatasync();
  if (auto took = time_since(start); took > WARN_SYNC_DURATION) {
    LOGGER_WARN(logger_, "slow fdatasyc, took: {} seconds, expect: {} seconds", to_seconds(took),
                to_seconds(WARN_SYNC_DURATION));
  }
  co_return result;
}

asio::awaitable<expected<void>> wal::release_lock_to(std::uint64_t index) {
  ASIO_ENSURE_STRAND(strand_)
  if (segments_.empty()) {
    co_return ok();
  }

  int smaller = 0;
  auto found = false;
  for (auto [i, file_handle] : segments_ | std::views::enumerate) {
    std::string_view file_name;
    auto result = std::visit(
        [&](auto& handle) {
          return leaf_to_expected([&]() {
            file_name = handle->name();
            return parse_wal_name(fileutil::base_name(handle->name()));
          });
        },
        file_handle);
    if (!result) {
      LOGGER_ERROR(logger_, "parse wal name:{} failed, error:{}", file_name, result.error().message());
      co_return tl::unexpected(result.error());
    }
    auto [_, lock_index] = *result;
    if (lock_index > index) {
      smaller = static_cast<int>(index) - 1;
      found = true;
      break;
    }
  }

  // if no lock index is greater than the release index, we can
  // release lock up to the last one(excluding).
  if (!found) {
    smaller = static_cast<int>(segments_.size()) - 1;
  }

  if (smaller <= 0) {
    co_return ok();
  }
  segments_.erase(segments_.begin(), segments_.begin() + smaller);
  co_return ok();
}

leaf::result<void> wal::rename_wal(const std::string& tmp_dir_path) {
  LEPTON_LEAF_CHECK(fileutil::remove_all(dir_));
  LEPTON_LEAF_CHECK(fileutil::rename(tmp_dir_path, dir_));
  BOOST_LEAF_AUTO(file_pipeline, new_file_pipeline(executor_, env_, dir_, segment_size_bytes_, logger_));
  file_pipeline_ = std::move(file_pipeline);
  BOOST_LEAF_AUTO(dir_file, fileutil::new_directory(env_, dir_));
  dir_file_ = std::move(dir_file);
  return {};
}

void wal::cleanup_wal() {
  co_spawn(
      executor_,
      [&]() -> asio::awaitable<void> {
        if (auto result = co_await close(); !result) {
          LOGGER_CRITICAL(logger_, "Failed to close wal during cleanup: {}", result.error().message());
        }
        co_return;
      },
      asio::use_future);

  using namespace std::chrono;
  auto now = system_clock::now();
  auto seconds = floor<std::chrono::seconds>(now);
  auto micros = duration_cast<microseconds>(now - seconds).count();
  auto broken_dir_name = std::format("{}.broken.{:%Y%m%d.%H%M%S}.{:06}", dir_, seconds, micros);
  if (auto result = leaf_to_expected([&]() { return fileutil::rename(dir_, broken_dir_name); }); !result) {
    LOGGER_CRITICAL(logger_, "Failed to rename broken wal dir {} to {}: {}", dir_, broken_dir_name,
                    result.error().message());
  }
  return;
}

asio::awaitable<expected<void>> wal::close() {
  ASIO_ENSURE_STRAND(strand_)

  if (is_closed_) co_return ok();
  is_closed_ = true;
  if (file_pipeline_ != nullptr) {
    co_await file_pipeline_->close();
    file_pipeline_.reset();
  }

  if (tail() != nullptr) {
    CO_CHECK_AWAIT(sync());
  }
  for (auto& file_handle : segments_) {
    auto result = std::visit([](auto& handle) { return handle->close(); }, file_handle);
    if (!result) {
      LOGGER_WARN(logger_, "Failed to close wal file handle: {}", result.error().message());
    }
  }
  if (dir_file_) {
    co_return leaf_to_expected([&]() -> leaf::result<void> { return dir_file_->close(); });
  }
  co_return ok();
}

fileutil::locked_file_endpoint* wal::tail() {
  if (segments_.empty()) return nullptr;
  auto& seg = segments_.back();
  if (auto* p = std::get_if<fileutil::locked_file_endpoint_handle>(&seg)) {
    return p->get();  // unique_ptr -> raw pointer
  }
  return nullptr;
}

std::uint64_t wal::seq() {
  auto tail_segment = tail();
  if (tail_segment == nullptr) {
    return 0;
  }
  auto parse_result = leaf::try_handle_some(
      [&]() -> leaf::result<std::uint64_t> {
        BOOST_LEAF_AUTO(v, parse_wal_name(fileutil::base_name(tail_segment->name())));
        auto [seq, _] = v;
        return seq;
      },
      [&](const lepton_error& e) -> leaf::result<std::uint64_t> {
        LOG_CRITICAL(e.message);
        return new_error(e);
      });
  assert(parse_result);
  return *parse_result;
}

}  // namespace lepton::storage::wal
