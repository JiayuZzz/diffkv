#pragma once

#include "blob_file_builder.h"
#include "blob_file_manager.h"
#include "blob_file_set.h"
#include "iostream"
#include "mutex"
#include "table/table_builder.h"
#include "titan/options.h"
#include "titan_stats.h"
#include "unordered_map"
#include "vector"

namespace rocksdb {
namespace titandb {

class TitanTableBuilder : public TableBuilder {
 public:
  TitanTableBuilder(uint32_t cf_id, const TitanDBOptions &db_options,
                    const TitanCFOptions &cf_options,
                    std::unique_ptr<TableBuilder> base_builder,
                    std::shared_ptr<BlobFileManager> blob_manager,
                    std::weak_ptr<BlobStorage> blob_storage, TitanStats *stats,
                    int merge_level, int target_level, int start_level = -1)
      : cf_id_(cf_id),
        db_options_(db_options),
        cf_options_(cf_options),
        base_builder_(std::move(base_builder)),
        blob_manager_(blob_manager),
        blob_storage_(blob_storage),
        stats_(stats),
        target_level_(target_level),
        merge_level_(merge_level),
        start_level_(start_level) {}

  void Add(const Slice &key, const Slice &value) override;

  Status status() const override;

  Status Finish() override;

  void Abandon() override;

  uint64_t NumEntries() const override;

  uint64_t FileSize() const override;

  bool NeedCompact() const override;

  TableProperties GetTableProperties() const override;

 private:
  friend class TableBuilderTest;

  bool ok() const { return status().ok(); }

  void AddBlob(const Slice &key, const Slice &value, std::string *index_value);

  bool ShouldMerge(const std::shared_ptr<BlobFileMeta> &file);

  void FinishBlobFile();

  void UpdateInternalOpStats();

  ~TitanTableBuilder();

  Status status_;
  uint32_t cf_id_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  std::unique_ptr<TableBuilder> base_builder_;
  std::unique_ptr<BlobFileHandle> blob_handle_;
  std::shared_ptr<BlobFileManager> blob_manager_;
  std::unique_ptr<BlobFileBuilder> blob_builder_;
  std::weak_ptr<BlobStorage> blob_storage_;
  std::vector<
      std::pair<std::shared_ptr<BlobFileMeta>, std::unique_ptr<BlobFileHandle>>>
      finished_blobs_;
  TitanStats *stats_;
  std::unordered_map<uint64_t, std::unique_ptr<BlobFilePrefetcher>>
      merging_files_;
  uint64_t blob_merge_time_{0};
  uint64_t blob_read_time_{0};
  uint64_t blob_add_time_{0};
  uint64_t blob_finish_time_{0};

  // target level in LSM-Tree for generated SSTs and blob files
  int target_level_;
  // with cf_options_.level_merge == true, if target_level_ is higher than or
  // equals to merge_level_, values belong to blob files which have lower level
  // than target_level_ will be merged to new blob file
  int merge_level_;
  int start_level_;

  // counters
  uint64_t bytes_read_ = 0;
  uint64_t bytes_written_ = 0;
  uint64_t io_bytes_read_ = 0;
  uint64_t io_bytes_written_ = 0;
};

class ForegroundBuilder {
 public:
  Status Add(const Slice &key, const Slice &value, WriteBatch &wb);

  void Finish();

  ForegroundBuilder(uint32_t cf_id,
                    std::shared_ptr<BlobFileManager> blob_file_manager,
                    const TitanDBOptions &db_options,
                    const TitanCFOptions &cf_options)
      : cf_id_(cf_id),
        blob_file_manager_(blob_file_manager),
        db_options_(db_options),
        cf_options_(cf_options) {
    ResetBuilder();
  }

  ForegroundBuilder() = default;

 private:
  uint32_t cf_id_;
  std::unique_ptr<BlobFileHandle> handle_;
  std::unique_ptr<BlobFileBuilder> builder_;
  std::shared_ptr<BlobFileManager> blob_file_manager_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  std::vector<std::thread> pool;
  std::vector<std::mutex> mutex_{2};
  std::unordered_map<std::string, uint64_t> keys_{};
  uint64_t discardable_{0};

  void ResetBuilder() {
    mutex_[0].lock();
    handle_.reset();
    builder_.reset();
    keys_.clear();
    discardable_ = 0;
    for (auto &t : pool) t.join();
    pool.clear();
    mutex_[0].unlock();
  }

  Status FinishBlob(std::unique_ptr<BlobFileHandle> &&handle,
                    std::unique_ptr<BlobFileBuilder> &&builder,
                    uint64_t discardable) {
    Status s;
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
      files.emplace_back(std::make_pair(file, std::move(handle)));
      s = blob_file_manager_->BatchFinishFiles(cf_id_, files);
    }
    return s;
  }
};

}  // namespace titandb
}  // namespace rocksdb
