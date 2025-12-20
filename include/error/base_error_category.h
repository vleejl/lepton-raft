#pragma once
#ifndef _LEPTON_BASE_ERROR_CATEGORY_H_
#define _LEPTON_BASE_ERROR_CATEGORY_H_
#include <system_error>
namespace lepton {

class base_error_category : public std::error_category {
 public:
  virtual const char* name() const noexcept override = 0;
  virtual std::string message(int ev) const override = 0;
};

}  // namespace lepton

#endif  // _LEPTON_BASE_ERROR_CATEGORY_H_
