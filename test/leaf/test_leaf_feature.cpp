#include <error.h>

#include <cstdint>

#include "gtest/gtest.h"
#include "leaf.hpp"
using namespace lepton;

template <typename T,
          typename std::enable_if<std::is_same<T, int>::value ||
                                      std::is_same<T, double>::value,
                                  int>::type = 0>
struct MyClass {
  T value;

  MyClass(T val) : value(val) {}

  void display() { std::cout << "Value: " << value << std::endl; }
};

leaf::result<std::uint32_t> do_some_thinge(std::uint32_t i) {
  if (i == 0) {
    return new_error(logic_error::CONFIG_INVALID, "test config invalid error");
  }
  if (i == 1) {
    return new_error(logic_error::KEY_NOT_FOUND, "test key not found error");
  }
  return new_error(system_error::UNKNOWN_ERROR, "test unknown error");
}

TEST(test_suite_leaf, test_leaf_try_handle_some) {
  auto r = leaf::try_handle_some(
      []() -> leaf::result<std::uint32_t> {
        BOOST_LEAF_AUTO(v, do_some_thinge(0));
        return v;
      },
      [](leaf::match<lepton_error<logic_error>, logic_error::CONFIG_INVALID,
                     logic_error::KEY_NOT_FOUND>)
          -> leaf::result<std::uint32_t> {
        // std::cout << err.err_code << " " << err.message << std::endl;
        return 0;
      },
      [](const lepton_error<system_error> &err) -> leaf::result<std::uint32_t> {
        std::cout << err.err_code << " " << err.message << std::endl;
        return 0;
      });
}