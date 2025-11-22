#ifndef _LEPTON_WAL_DIRECTORY_MANAGER_H_
#define _LEPTON_WAL_DIRECTORY_MANAGER_H_
#include <fmt/format.h>
#include <rocksdb/env.h>
#include <rocksdb/status.h>

#include <string>

#include "lepton_error.h"
#include "wal_file.h"
namespace lepton {
class wal_directory_manager {
 public:
  bool file_exist(const std::string& file_name) { return env_->FileExists(file_name).ok(); }

  // IsDirWriteable checks if dir is writable by writing and removing a file
  // to dir. It returns nil if dir is writable.
  leaf::result<void> is_dir_writeable(const std::string& dir_name);

  leaf::result<void> ensure_directory_writable(const std::string& dir_name);

  leaf::result<void> create_dir_all(const std::string& dir_name);

  // createNewWALFile creates a WAL file.
  // To create a locked file, use *fileutil.LockedFile type parameter.
  // To create a standard file, use *os.File type parameter.
  // If forceNew is true, the file will be truncated if it already exists.
  leaf::result<wal_file> create_new_wal_file(const std::string& filename, bool force_new);

  // Create creates a WAL ready for appending records. The given metadata is
  // recorded at the head of each WAL file, and can be retrieved with ReadAll
  // after the file is Open.
  leaf::result<void> create_wal(const std::string& dirpath);

 private:
  rocksdb::Env* env_;
  asio::any_io_executor executor_;
};
}  // namespace lepton

#endif  // _LEPTON_WAL_DIRECTORY_MANAGER_H_
