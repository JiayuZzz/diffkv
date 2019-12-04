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
// rocksdb::Env* env_ = rocksdb::Env::Default();
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
    // std::cerr<<"fall back"<<std::endl;
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
          std::cerr << "create prefetcher error!" << std::endl;
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
          std::cerr << "add blob not ok!" << std::endl;
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
  index.file_number = blob_handle_->GetNumber();
  BlobRecord record;
  if(cf_options_.level_merge) record.only_value = true;
  record.key = key;
  record.value = value;
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

Status ForegroundBuilder::Add(const Slice& key, const Slice& value,
                              WriteBatch& wb) {
  uint64_t add_time = 0;
  Status s;
  {
    TitanStopWatch swadd(env_, add_time);
    std::string k = key.ToString();
    int b = num_builders_ > 1 ? hash(k) % num_builders_ : 0;
    mutex_[b].lock();
    if (value.size() < cf_options_.min_blob_size ||
        (cf_options_.level_merge && value.size() < cf_options_.mid_blob_size)) {
      auto iter = keys_[b].find(k);
      if (iter == keys_[b].end()) {
      } else {
        discardable_[b] += iter->second;
        keys_[b].erase(iter);
      }
      mutex_[b].unlock();
      return Status::InvalidArgument();
    }
    if (!handle_[b] && !builder_[b]) {
      s = blob_file_manager_->NewFile(&handle_[b], env_options_);
      if (!s.ok()) return s;
      builder_[b] = std::unique_ptr<BlobFileBuilder>(
          new BlobFileBuilder(db_options_, cf_options_, handle_[b]->GetFile()));
      auto storage = blob_storage_.lock();
      if (!storage) {
        std::cerr << "no storage!" << std::endl;
        abort();
      }
      storage->AddBuildingFile(handle_[b]->GetNumber());
      keys_[b].clear();
      discardable_[b] = 0;
    }
    BlobRecord blob_record;
    blob_record.key = key;
    blob_record.value = value;
    BlobIndex blob_index;
    blob_index.file_number = handle_[b]->GetNumber();
    builder_[b]->Add(blob_record, &blob_index.blob_handle);
    auto iter = keys_[b].find(k);
    if (iter == keys_[b].end()) {
      keys_[b][k] = blob_index.blob_handle.size;
    } else {
      discardable_[b] += iter->second;
      iter->second = blob_index.blob_handle.size;
    }
    if (handle_[b]->GetFile()->GetFileSize() >=
        cf_options_.blob_file_target_size) {
      FinishBlob(b);
    }
    mutex_[b].unlock();
    std::string index_entry;
    blob_index.EncodeTo(&index_entry);

    s = WriteBatchInternal::PutBlobIndex(&wb, cf_id_, blob_record.key,
                                         index_entry);
  }
  foreground_blob_add_time += add_time;
  return s;
}

void ForegroundBuilder::Finish() {
  for (int i = 0; i < num_builders_; i++) {
    mutex_[i].lock();
    FinishBlob(i);
    blob_file_manager_->BatchFinishFiles(cf_id_, finished_files_[i]);
    finished_files_[i].clear();
    mutex_[i].unlock();
  }
}

Status ForegroundBuilder::FinishBlob(int b) {
  uint64_t finish_time = 0;
  Status s;
  {
    TitanStopWatch sw(env_, finish_time);
    if (!builder_[b] && !handle_[b]) return s;
    s = builder_[b]->Finish();
    if (s.ok()) {
      auto file = std::make_shared<BlobFileMeta>(
          handle_[b]->GetNumber(), handle_[b]->GetFile()->GetFileSize(),
          builder_[b]->NumEntries(), 0, builder_[b]->GetSmallestKey(),
          builder_[b]->GetLargestKey(), kUnSorted);
      // std::cerr<<"finish file size "<<handle_[b]->GetFile()->GetFileSize()<<"
      // discardable size "<<discardable_[b]<<" file
      // "<<file->file_number()<<std::endl;
      file->AddDiscardableSize(discardable_[b]);
      if (file->GetDiscardableRatio() > 1) {
        std::cerr << "ratio " << file->GetDiscardableRatio() << std::endl;
        abort();
      }
      if (file->NoLiveData()) {
        std::cerr << "delete file " << file->file_number() << std::endl;
        env_->DeleteFile(
            BlobFileName(db_options_.dirname, file->file_number()));
      } else {
        file->FileStateTransit(BlobFileMeta::FileEvent::kReset);
        finished_files_[b].emplace_back(
            std::make_pair(file, std::move(handle_[b])));
      }
    } else {
      std::cerr << "finish failed" << std::endl;
      abort();
    }
    builder_[b].reset();
    handle_[b].reset();
    keys_[b].clear();
    discardable_[b] = 0;
  }
  foreground_blob_finish_time += finish_time;
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
