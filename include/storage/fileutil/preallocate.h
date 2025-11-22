#ifndef _LEPTON_PREALLOCATE_H_
#define _LEPTON_PREALLOCATE_H_

#include <cstdint>
#include <system_error>
namespace lepton {
std::error_code preallocate(int fd, uint64_t length);
}  // namespace lepton

#endif  // _LEPTON_PREALLOCATE_H_
