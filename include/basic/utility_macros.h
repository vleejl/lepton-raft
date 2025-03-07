#ifndef _LEPTON_UTILITY_MACROS_H_
#define _LEPTON_UTILITY_MACROS_H_

namespace lepton {
#define NOT_COPYABLE(Type)     \
  Type(const Type &) = delete; \
  Type &operator=(const Type &) = delete;

#define MOVABLE_BUT_NOT_COPYABLE(Type) \
  Type(Type &&) = default;             \
  Type &operator=(Type &&) = default;  \
  Type(const Type &) = delete;         \
  Type &operator=(const Type &) = delete;

#define NO_MOVABLE_BUT_COPYABLE(Type)      \
  Type(const Type &) = default;            \
  Type &operator=(const Type &) = default; \
  Type(Type &&) = delete;                  \
  Type &operator=(Type &&) = delete;

#define NONCOPYABLE_NONMOVABLE(Type)      \
  Type(const Type &) = delete;            \
  Type &operator=(const Type &) = delete; \
  Type(Type &&) = delete;                 \
  Type &operator=(Type &&) = delete;

// 自定义宏，传入类名和函数名来自动生成调度类型
#define PRO_DEF_PROXY_DISPATCH(class_name, func_name) \
  PRO_DEF_MEM_DISPATCH(class_name##_##func_name, func_name);
}  // namespace lepton

#endif  // _LEPTON_UTILITY_MACROS_H_