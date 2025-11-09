#ifndef _LEPTON_WAL_H_
#define _LEPTON_WAL_H_
#include <wal.pb.h>

namespace lepton {
// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.

}  // namespace lepton

#endif  // _LEPTON_WAL_H_
