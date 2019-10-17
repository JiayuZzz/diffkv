#pragma once

#include "blob_file_builder.h"
#include "blob_file_manager.h"
#include "blob_file_set.h"
#include "table/table_builder.h"
#include "titan/options.h"
#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

class TitanTableBuilder : public TableBuilder {
public:
  TitanTableBuilder(uint32_t cf_id, const TitanDBOptions &db_options,
                    const TitanCFOptions &cf_options,
                    std::unique_ptr<TableBuilder> base_builder,
                    std::shared_ptr<BlobFileManager> blob_manager,
                    std::weak_ptr<BlobStorage> blob_storage, TitanStats *stats,
                    int merge_level, int target_level)
      : cf_id_(cf_id),
        db_options_(db_options),
        cf_options_(cf_options),
        base_builder_(std::move(base_builder)),
        blob_manager_(blob_manager),
        blob_storage_(blob_storage),
        stats_(stats),
        target_level_(target_level),
        merge_level_(merge_level) {}

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

  void FinishBlob();

  void UpdateInternalOpStats();

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

  // target level in LSM-Tree for generated SSTs and blob files
  int target_level_;
  // with cf_options_.level_merge == true, if target_level_ is higher than or
  // equals to merge_level_, values belong to blob files which have lower level
  // than target_level_ will be merged to new blob file
  int merge_level_;

  // counters
  uint64_t bytes_read_ = 0;
  uint64_t bytes_written_ = 0;
  uint64_t io_bytes_read_ = 0;
  uint64_t io_bytes_written_ = 0;
};

class ForegroundBuilder {
public:
  Status Add(const Slice &key, const Slice &value, WriteBatch &wb) {
    Status s;
    int i = value.size() >= cf_options_.min_blob_size ? 1 : 0;
    if (!handles[i] && !builders[i]) {
      s = blob_file_manager_->NewFile(&handles[i]);
      if (!s.ok())
        return s;
      builders[i] = std::unique_ptr<BlobFileBuilder>(
          new BlobFileBuilder(db_options_, cf_options_, handles[i]->GetFile())
      );
    }
    BlobRecord blob_record;
    blob_record.key = key;
    blob_record.value = value;
    BlobIndex blob_index;
    blob_index.file_number = handles[i]->GetNumber();
    builders[i]->Add(blob_record, &blob_index.blob_handle);
    std::string index_entry;
    blob_index.EncodeTo(&index_entry);

    s = WriteBatchInternal::PutBlobIndex(&wb, cf_id_, blob_record.key, index_entry);
    return s;
  }

  Status Finish() {
    Status s;
    std::vector<std::pair<std::shared_ptr<BlobFileMeta>, std::unique_ptr<BlobFileHandle>>> files;
    for (int i = 0; i <= 1; i++) {
      if (!builders[i] && !handles[i])
        continue;
      s = builders[i]->Finish();
      if (!s.ok())
        return s;
      auto file = std::make_shared<BlobFileMeta>(handles[i]->GetNumber(),
                                                 handles[i]->GetFile()->GetFileSize(),
                                                 builders[i]->NumEntries(),
                                                 0, builders[i]->GetSmallestKey(),
                                                 builders[i]->GetLargestKey());
      s = blob_file_manager_->BatchFinishFiles(cf_id_, files);
      files.emplace_back(std::make_pair(file, std::move(handles[i])));
    }
    s = blob_file_manager_->BatchFinishFiles(cf_id_, files);
    ResetBuilder();
    return s;
  }

  ForegroundBuilder(std::shared_ptr<BlobFileManager> blob_file_manager, const TitanDBOptions &db_options,
                    const TitanCFOptions &cf_options)
      : blob_file_manager_(blob_file_manager), db_options_(db_options), cf_options_(cf_options) {
    ResetBuilder();
  }

  ForegroundBuilder() = default;

private:
  std::unique_ptr<BlobFileHandle> handles[2];
  std::unique_ptr<BlobFileBuilder> builders[2];
  std::shared_ptr<BlobFileManager> blob_file_manager_;
  TitanDBOptions db_options_;
  TitanCFOptions cf_options_;
  int cf_id_;

  void ResetBuilder() {
    handles[0] = nullptr;
    handles[1] = nullptr;
    builders[0] = nullptr;
    builders[1] = nullptr;
  }
};

}  // namespace titandb
}  // namespace rocksdb
