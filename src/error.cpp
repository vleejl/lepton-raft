#include "error.h"

namespace lepton {

const system_error_category& get_system_error_category() {
  static system_error_category instance;
  return instance;
}

const encoding_error_category& get_encoding_error_category() {
  static encoding_error_category instance;
  return instance;
}

const storage_error_category& get_storage_error_category() {
  static storage_error_category instance;
  return instance;
}

const logic_error_category& get_logic_error_category() {
  static logic_error_category instance;
  return instance;
}

}  // namespace lepton
