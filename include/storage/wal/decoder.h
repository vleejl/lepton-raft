#ifndef _LEPTON_DECODER_H_
#define _LEPTON_DECODER_H_
#include <cstddef>

#include "reader.h"
namespace lepton {

constexpr std::size_t MIN_SECTOR_SIZE = 512;

// frameSizeBytes is frame size in bytes, including record size and padding size
constexpr std::size_t FRAME_SIZE_BYTES = 8;

class decoder {
 public:
  explicit decoder(const std::vector<pro::proxy_view<reader>>& opts = {}) : r_(r) {}

 private:
  pro::proxy_view<reader> r_;
};

}  // namespace lepton

#endif  // _LEPTON_DECODER_H_
