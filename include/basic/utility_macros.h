#ifndef _LEPTON_UTILITY_MACROS_H_
#define _LEPTON_UTILITY_MACROS_H_

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

#ifndef LEPTON_UNUSED
#if defined(__GNUC__) || defined(__clang__)
#define LEPTON_UNUSED(x) (void)(x)
#elif defined(_MSC_VER)
#define LEPTON_UNUSED(x) (void)(x)
#else
#define LEPTON_UNUSED(x) (void)(x)
#endif
#endif

#endif  // _LEPTON_UTILITY_MACROS_H_