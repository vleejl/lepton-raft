#ifndef _LEPTON_IO_H_
#define _LEPTON_IO_H_

#include "expected.h"
#include "proxy.h"
#include "reader.h"
namespace lepton {

// ReadAtLeast reads from r into buf until it has read at least min bytes.
// It returns the number of bytes copied and an error if fewer bytes were read.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading fewer than min bytes,
// ReadAtLeast returns [ErrUnexpectedEOF].
// If min is greater than the length of buf, ReadAtLeast returns [ErrShortBuffer].
// On return, n >= min if and only if err == nil.
// If r returns an error having read at least min bytes, the error is dropped.
asio::awaitable<expected<std::size_t>> read_at_least(pro::proxy_view<reader> r, asio::mutable_buffer buf,
                                                     std::size_t min);

// ReadFull reads exactly len(buf) bytes from r into buf.
// It returns the number of bytes copied and an error if fewer bytes were read.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading some but not all the bytes,
// ReadFull returns [ErrUnexpectedEOF].
// On return, n == len(buf) if and only if err == nil.
// If r returns an error having read at least len(buf) bytes, the error is dropped.
asio::awaitable<expected<std::size_t>> read_full(pro::proxy_view<reader> r, asio::mutable_buffer buf);
}  // namespace lepton

#endif  // _LEPTON_IO_H_
