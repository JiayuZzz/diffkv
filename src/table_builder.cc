#include "table_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <atomic>
#include <iostream>
#include "monitoring/statistics.h"

std::atomic<uint64_t> blob_merge_time{0};
std::atomic<uint64_t> blob_read_time{0};
std::atomic<uint64_t> blob_add_time{0};
std::atomic<uint64_t> blob_finish_time{0};
std::atomic<uint64_t> foreground_blob_add_time{0};
std::atomic<uint64_t> foreground_blob_finish_time{0};
//rocksdb::Env* env_ = rocksdb::Env::Default();
// extern rocksdb::Env* env_;

namespace rocksdb {
namespace titandb {
Env* env_ = Env::Default();

TitanTableBuilder::~TitanTableBuilder() {
  blob_merge_time += blob_merge_time_;
  blob_read_time += blob_read_time_;
  blob_add_time += blob_add_time_;
  blob_finish_time += blob_finish_time_;
}

void TitanTableBuilder::Add(const Slice& key, const Slice& value) {
  TitanStopWatch swadd(env_, blob_add_time_);
  if (!ok()) return;

  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    status_ = Status::Corruption(Slice());
    return;
  }

  uint64_t prev_bytes_read = 0;
  uint64_t prev_bytes_written = 0;
  SavePrevIOBytes(&prev_bytes_read, &prev_bytes_written);

  if (ikey.type == kTypeBlobIndex &&
      cf_options_.blob_run_mode == TitanBlobRunMode::kFallback) {
    std::cerr<<"fall back"<<std::endl;
    // we ingest value from blob file
    Slice copy = value;
    BlobIndex index;
    status_ = index.DecodeFrom(&copy);
    if (!ok()) {
      return;
    }

    BlobRecord record;
    PinnableSlice buffer;

    auto storage = blob_storage_.lock();
    assert(storage != nullptr);
    ReadOptions options;  // dummy option
    Status get_status = storage->Get(options, index, &record, &buffer);
    UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_,
                  &io_bytes_written_);
    if (get_status.ok()) {
      ikey.type = kTypeValue;
      std::string index_key;
      AppendInternalKey(&index_key, ikey);
      base_builder_->Add(index_key, record.value);
      bytes_read_ += record.size();
    } else {
      // Get blob value can fail if corresponding blob file has been GC-ed
      // deleted. In this case we write the blob index as is to compaction
      // output.
      // TODO: return error if it is indeed an error.
      base_builder_->Add(key, value);
    }
  } else if (ikey.type == kTypeValue &&
             value.size() >= cf_options_.min_blob_size &&
             cf_options_.blob_run_mode == TitanBlobRunMode::kNormal) {
    // we write to blob file and insert index
    std::string index_value;
    AddBlob(ikey.user_key, value, &index_value);
    UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_,
                  &io_bytes_written_);
    if (ok()) {
      ikey.type = kTypeBlobIndex;
      std::string index_key;
      AppendInternalKey(&index_key, ikey);
      base_builder_->Add(index_key, index_value);
    }
  } else if (ikey.type == kTypeBlobIndex && cf_options_.level_merge &&
             /*start_level_ != 0 &&*/ target_level_ >= merge_level_ &&
             cf_options_.blob_run_mode == TitanBlobRunMode::kNormal) {
    // we merge value to new blob file
    BlobIndex index;
    Slice copy = value;
    status_ = index.DecodeFrom(&copy);
    if (!ok()) {
      return;
    }
    // if (db_options_.sep_before_flush &&
        // index.blob_handle.size >= cf_options_.mid_blob_size) {
      // base_builder_->Add(key, value);
      // return;
    // }
    auto storage = blob_storage_.lock();
    assert(storage != nullptr);
    auto blob_file = storage->FindFile(index.file_number).lock();
    if (ShouldMerge(blob_file)) {
      auto it = merging_files_.find(index.file_number);
      if (it == merging_files_.end()) {
        std::unique_ptr<BlobFilePrefetcher> prefetcher;
        status_ = storage->NewPrefetcher(index.file_number, &prefetcher);
        if (!status_.ok()) {
          std::cerr<<"create prefetcher error!"<<std::endl;
          base_builder_->Add(key, value);
          return;
        }
        it = merging_files_.emplace(index.file_number, std::move(prefetcher))
                 .first;
      }
      BlobRecord record;
      PinnableSlice buffer;
      Status s;
      {
        TitanStopWatch sw(env_, blob_read_time_);
        s = it->second->Get(ReadOptions(), index.blob_handle, &record, &buffer);
      }
      if (s.ok()) {
        std::string index_value;
        {
          TitanStopWatch sw(env_, blob_merge_time_);
          AddBlob(ikey.user_key, record.value, &index_value);
        }
        if (ok()) {
          std::string index_key;
          ikey.type = kTypeBlobIndex;
          AppendInternalKey(&index_key, ikey);
          base_builder_->Add(index_key, index_value);
          return;
        } else {
          std::cerr<<"not ok!"<<std::endl;
        }
      }
    }
    base_builder_->Add(key, value);
  } else {
    base_builder_->Add(key, value);
  }
}

void TitanTableBuilder::AddBlob(const Slice& key, const Slice& value,
                                std::string* index_value) {
  if (!ok()) return;
  StopWatch write_sw(db_options_.env, stats_, BLOB_DB_BLOB_FILE_WRITE_MICROS);

  if (!blob_builder_) {
    status_ = blob_manager_->NewFile(&blob_handle_);
    if (!ok()) return;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Titan table builder created new blob file %" PRIu64 ".",
                   blob_handle_->GetNumber());
    blob_builder_.reset(
        new BlobFileBuilder(db_options_, cf_options_, blob_handle_->GetFile()));
  }

  RecordTick(stats_, BLOB_DB_NUM_KEYS_WRITTEN);
  RecordInHistogram(stats_, BLOB_DB_KEY_SIZE, key.size());
  RecordInHistogram(stats_, BLOB_DB_VALUE_SIZE, value.size());
  AddStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_SIZE, value.size());
  bytes_written_ += key.size() + value.size();

  BlobIndex index;
  BlobRecord record;
  record.key = key;
  record.value = value;
  index.file_number = blob_handle_->GetNumber();
  blob_builder_->Add(record, &index.blob_handle);
  RecordTick(stats_, BLOB_DB_BLOB_FILE_BYTES_WRITTEN, index.blob_handle.size);
  bytes_written_ += record.size();
  if (ok()) {
    index.EncodeTo(index_value);
    if (blob_handle_->GetFile()->GetFileSize() >=
        cf_options_.blob_file_target_size) {
      FinishBlobFile();
    }
  }
}

void TitanTableBuilder::FinishBlobFile() {
  TitanStopWatch sw(env_, blob_finish_time_);
  if (blob_builder_) {
    blob_builder_->Finish();
    if (ok()) {
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Titan table builder finish output file %" PRIu64 ".",
                     blob_handle_->GetNumber());
      std::shared_ptr<BlobFileMeta> file = std::make_shared<BlobFileMeta>(
          blob_handle_->GetNumber(), blob_handle_->GetFile()->GetFileSize(),
          blob_builder_->NumEntries(), target_level_,
          blob_builder_->GetSmallestKey(), blob_builder_->GetLargestKey(),
          kSorted);
      file->FileStateTransit(BlobFileMeta::FileEvent::kFlushOrCompactionOutput);
      finished_blobs_.push_back({file, std::move(blob_handle_)});
      blob_builder_.reset();
    } else {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "Titan table builder finish failed. Delete output file %" PRIu64 ".",
          blob_handle_->GetNumber());
      status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
    }
  }
}

Status TitanTableBuilder::status() const {
  Status s = status_;
  if (s.ok()) {
    s = base_builder_->status();
  }
  if (s.ok() && blob_builder_) {
    s = blob_builder_->status();
  }
  return s;
}

Status TitanTableBuilder::Finish() {
  base_builder_->Finish();
  FinishBlobFile();
  status_ = blob_manager_->BatchFinishFiles(cf_id_, finished_blobs_);
  if (!status_.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Titan table builder failed on finish: %s",
                    status_.ToString().c_str());
  }
  UpdateInternalOpStats();
  return status();
}

void TitanTableBuilder::Abandon() {
  base_builder_->Abandon();
  if (blob_builder_) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Titan table builder abandoned. Delete output file %" PRIu64
                   ".",
                   blob_handle_->GetNumber());
    blob_builder_->Abandon();
    status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
  }
}

uint64_t TitanTableBuilder::NumEntries() const {
  return base_builder_->NumEntries();
}

uint64_t TitanTableBuilder::FileSize() const {
  return base_builder_->FileSize();
}

bool TitanTableBuilder::NeedCompact() const {
  return base_builder_->NeedCompact();
}

TableProperties TitanTableBuilder::GetTableProperties() const {
  return base_builder_->GetTableProperties();
}

bool TitanTableBuilder::ShouldMerge(
    const std::shared_ptr<rocksdb::titandb::BlobFileMeta>& file) {
  assert(cf_options_.level_merge);
  // Values in blob file should be merged if
  // 1. Corresponding keys are being compacted to last two level from lower
  // level
  // 2. Blob file is marked by GC or range merge
  return file != nullptr && file->file_type() == kSorted &&
         (static_cast<int>(file->file_level()) < target_level_ ||
          file->file_state() == BlobFileMeta::FileState::kToMerge);
}

void TitanTableBuilder::UpdateInternalOpStats() {
  if (stats_ == nullptr) {
    return;
  }
  TitanInternalStats* internal_stats = stats_->internal_stats(cf_id_);
  if (internal_stats == nullptr) {
    return;
  }
  InternalOpType op_type = InternalOpType::COMPACTION;
  if (target_level_ == 0) {
    op_type = InternalOpType::FLUSH;
  }
  InternalOpStats* internal_op_stats =
      internal_stats->GetInternalOpStatsForType(op_type);
  assert(internal_op_stats != nullptr);
  AddStats(internal_op_stats, InternalOpStatsType::COUNT);
  AddStats(internal_op_stats, InternalOpStatsType::BYTES_READ, bytes_read_);
  AddStats(internal_op_stats, InternalOpStatsType::BYTES_WRITTEN,
           bytes_written_);
  AddStats(internal_op_stats, InternalOpStatsType::IO_BYTES_READ,
           io_bytes_read_);
  AddStats(internal_op_stats, InternalOpStatsType::IO_BYTES_WRITTEN,
           io_bytes_written_);
  if (blob_builder_ != nullptr) {
    AddStats(internal_op_stats, InternalOpStatsType::OUTPUT_FILE_NUM);
  }
}

Status ForegroundBuilder::Add(const Slice &key, const Slice &value, WriteBatch &wb) {
    uint64_t add_time = 0;
    Status s;
    {
    TitanStopWatch swadd(env_, add_time);
    mutex_[0].lock();
    std::string k = key.ToString();
    if (value.size() < cf_options_.min_blob_size ||
        (cf_options_.level_merge && value.size() < cf_options_.mid_blob_size)) {
      auto iter = keys_.find(k);
      if (iter == keys_.end()) {
      } else {
        discardable_ += iter->second;
        keys_.erase(iter);
      }
      mutex_[0].unlock();
      return Status::InvalidArgument();
    }
    if (!handle_ && !builder_) {
      s = blob_file_manager_->NewFile(&handle_);
      if (!s.ok()) return s;
      builder_ = std::unique_ptr<BlobFileBuilder>(
          new BlobFileBuilder(db_options_, cf_options_, handle_->GetFile()));
    }
    BlobRecord blob_record;
    blob_record.key = key;
    blob_record.value = value;
    BlobIndex blob_index;
    blob_index.file_number = handle_->GetNumber();
    builder_->Add(blob_record, &blob_index.blob_handle);
    auto iter = keys_.find(k);
    if (iter == keys_.end()) {
      keys_[k] = blob_index.blob_handle.size;
    } else {
      discardable_ += iter->second;
      iter->second = blob_index.blob_handle.size;
    }
    if (handle_->GetFile()->GetFileSize() >=
        cf_options_.blob_file_target_size) {
      // pool.push_back(std::thread(&ForegroundBuilder::FinishBlob, this,
                                //  std::move(handle_), std::move(builder_),
                                //  discardable_));
      FinishBlob(std::move(handle_), std::move(builder_), discardable_);
      builder_.reset();
      handle_.reset();
      keys_.clear();
      discardable_ = 0;
    }
    mutex_[0].unlock();
    std::string index_entry;
    blob_index.EncodeTo(&index_entry);

    s = WriteBatchInternal::PutBlobIndex(&wb, cf_id_, blob_record.key,
                                         index_entry);
    }
    foreground_blob_add_time += add_time;
    return s;
  }

void ForegroundBuilder::Finish() {
    std::vector<std::thread> p(2);
    std::vector<std::pair<std::shared_ptr<BlobFileMeta>,
                          std::unique_ptr<BlobFileHandle>>> files;
    mutex_[0].lock();
    // pool.push_back(std::thread(&ForegroundBuilder::FinishBlob, this,
                              //  std::move(handle_), std::move(builder_),
                              //  discardable_));
    FinishBlob(std::move(handle_), std::move(builder_), discardable_);
    builder_.reset();
    handle_.reset();
    keys_.clear();
    discardable_ = 0;
    p = std::move(pool);
    pool = std::vector<std::thread>();
    files = std::move(finished_files_);
    finished_files_ = std::vector<std::pair<std::shared_ptr<BlobFileMeta>,
                          std::unique_ptr<BlobFileHandle>>>();
    for (auto &t : p) t.join();
    mutex_[0].unlock();
    blob_file_manager_->BatchFinishFiles(cf_id_, finished_files_);
  }

  Status ForegroundBuilder::FinishBlob(std::unique_ptr<BlobFileHandle> &&handle,
                    std::unique_ptr<BlobFileBuilder> &&builder,
                    uint64_t discardable) {
    uint64_t finish_time = 0;
    Status s;
    {
    TitanStopWatch sw(env_, finish_time);
    std::vector<std::pair<std::shared_ptr<BlobFileMeta>,
                          std::unique_ptr<BlobFileHandle>>>
        files;
    if (!builder && !handle) return s;
    s = builder->Finish();
    if (s.ok()) {
      auto file = std::make_shared<BlobFileMeta>(
          handle->GetNumber(), handle->GetFile()->GetFileSize(),
          builder->NumEntries(), 0, builder->GetSmallestKey(),
          builder->GetLargestKey(), kUnSorted);
      file->FileStateTransit(BlobFileMeta::FileEvent::kReset);
      file->AddDiscardableSize(discardable);
      finished_files_.emplace_back(std::make_pair(file, std::move(handle)));
    }
    }
    foreground_blob_finish_time += finish_time;
    return s;
  }

}  // namespace titandb
}  // namespace rocksdb
