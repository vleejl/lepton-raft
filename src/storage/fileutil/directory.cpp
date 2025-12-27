#include "storage/fileutil/directory.h"

namespace lepton::storage::fileutil {

leaf::result<directory> new_directory(rocksdb::Env* env, const std::string& path) {
  assert(env != nullptr);
  std::unique_ptr<rocksdb::Directory> dir;
  auto s = env->NewDirectory(path, &dir);
  if (s.ok()) {
    return directory{std::move(dir)};
  }
  return new_error(s);
}

}  // namespace lepton::storage::fileutil
