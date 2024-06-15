#ifndef _LEPTON_ERROR_H_
#define _LEPTON_ERROR_H_
#include "leaf.hpp"

namespace lepton {
namespace leaf = boost::leaf;
template <typename T>
using result = boost::leaf::result<T>;

enum error_code {
  unknown_error,
  key_not_found_error,
};
}  // namespace lepton

#endif  // _LEPTON_ERROR_H_