#include "titan_stats.h"
#include "titan/db.h"

#include <map>
#include <string>

namespace rocksdb {
namespace titandb {

static const std::string titandb_prefix = "rocksdb.titandb.";

static const std::string live_blob_size = "live-blob-size";
static const std::string num_live_blob_file = "num-live-blob-file";
static const std::string num_obsolete_blob_file = "num-obsolete-blob-file";
static const std::string live_blob_file_size = "live-blob-file-size";
static const std::string obsolete_blob_file_size = "obsolete-blob-file-size";
static const std::string gc_read_lsm_micros = "gc-read-lsm";
static const std::string gc_callback_micros = "gc-callback";
static const std::string gc_write_lsm_micros = "gc-write-lsm";
static const std::string gc_write_blob_file_micros = "gc-write-blob-file";
static const std::string gc_read_blob_file_micros = "gc-read-blob-file";

const std::string TitanDB::Properties::kLiveBlobSize =
    titandb_prefix + live_blob_size;
const std::string TitanDB::Properties::kNumLiveBlobFile =
    titandb_prefix + num_live_blob_file;
const std::string TitanDB::Properties::kNumObsoleteBlobFile =
    titandb_prefix + num_obsolete_blob_file;
const std::string TitanDB::Properties::kLiveBlobFileSize =
    titandb_prefix + live_blob_file_size;
const std::string TitanDB::Properties::kObsoleteBlobFileSize =
    titandb_prefix + obsolete_blob_file_size;
const std::string TitanDB::Properties::kGCReadLsmMicros =
    titandb_prefix + gc_read_lsm_micros;
const std::string TitanDB::Properties::kGCCallbackMicros =
    titandb_prefix + gc_callback_micros;
const std::string TitanDB::Properties::kGCWriteLsmMicros =
    titandb_prefix + gc_write_lsm_micros;
const std::string TitanDB::Properties::kGCWriteBlobFileMicros =
    titandb_prefix + gc_write_blob_file_micros;
const std::string TitanDB::Properties::kGCReadBlobFileMicros =
    titandb_prefix + gc_read_blob_file_micros;

const std::unordered_map<std::string, TitanInternalStats::StatsType>
    TitanInternalStats::stats_type_string_map = {
        {TitanDB::Properties::kLiveBlobSize,
         TitanInternalStats::LIVE_BLOB_SIZE},
        {TitanDB::Properties::kNumLiveBlobFile,
         TitanInternalStats::NUM_LIVE_BLOB_FILE},
        {TitanDB::Properties::kNumObsoleteBlobFile,
         TitanInternalStats::NUM_OBSOLETE_BLOB_FILE},
        {TitanDB::Properties::kLiveBlobFileSize,
         TitanInternalStats::LIVE_BLOB_FILE_SIZE},
        {TitanDB::Properties::kObsoleteBlobFileSize,
         TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE},
        {TitanDB::Properties::kGCReadLsmMicros,
         TitanInternalStats::GC_READ_LSM_MICROS},
        {TitanDB::Properties::kGCCallbackMicros,
         TitanInternalStats::GC_CALLBACK_MICROS},
        {TitanDB::Properties::kGCWriteLsmMicros,
         TitanInternalStats::GC_WRITE_LSM_MICROS},
        {TitanDB::Properties::kGCWriteBlobFileMicros,
         TitanInternalStats::GC_WRITE_BLOB_FILE_MICROS},
        {TitanDB::Properties::kGCReadBlobFileMicros,
         TitanInternalStats::GC_READ_BLOB_FILE_MICROS},
};

}  // namespace titandb
}  // namespace rocksdb
