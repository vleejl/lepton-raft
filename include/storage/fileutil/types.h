#pragma once
#ifndef _LEPTON_STORAGE_FILEUTIL_TYPES_H_
#define _LEPTON_STORAGE_FILEUTIL_TYPES_H_

namespace lepton::storage::fileutil {
#ifdef _WIN32
using native_handle_t = void*;  // Windows 的 HANDLE 本质是 void*
#else
using native_handle_t = int;  // POSIX 的 FD 是 int
#endif
}  // namespace lepton::storage::fileutil

#endif  // _LEPTON_STORAGE_FILEUTIL_TYPES_H_
