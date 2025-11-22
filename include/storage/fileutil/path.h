#ifndef _LEPTON_PATH_H_
#define _LEPTON_PATH_H_
#include "leaf.h"
namespace lepton {
leaf::result<void> remove_all(const std::string& path);
}  // namespace lepton

#endif  // _LEPTON_PATH_H_
