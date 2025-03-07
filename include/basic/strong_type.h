#ifndef _LEPTON_STRONG_TYPE_H_
#define _LEPTON_STRONG_TYPE_H_
#include <concepts>
#include <format>
#include <iostream>

namespace lepton {

namespace type {
// Concept：约束基础类型必须是整数且可比较
template <typename T>
concept strong_underlying_type = std::integral<T> && requires(T a, T b) {
  { a == b } -> std::convertible_to<bool>;
  { a < b } -> std::convertible_to<bool>;
};

// 强类型模板（应用 Concept）
template <typename Tag, typename T>
requires strong_underlying_type<T>
struct strong_type {
  T value;
  constexpr explicit strong_type(T v = T{}) : value(v) {}

  // 显式转换
  explicit operator T() const { return value; }

  // 自动生成比较运算符
  auto operator<=>(const strong_type&) const = default;
};
}  // namespace type

}  // namespace lepton

namespace std {
template <typename Tag, typename T>
struct formatter<lepton::type::strong_type<Tag, T>> : formatter<T> {
  auto format(const lepton::type::strong_type<Tag, T>& obj,
              format_context& ctx) const {
    // 直接使用底层类型的格式化逻辑
    return formatter<T>::format(obj.value, ctx);

    // 如果希望添加类型标签信息（需要 RTTI）
    // return format_to(ctx.out(), "strong_type<{}>({})", typeid(Tag).name(),
    // obj.value);
  }
};

template <typename Tag, typename T>
std::ostream& operator<<(std::ostream& os,
                         const lepton::type::strong_type<Tag, T>& obj) {
  return os << obj.value;
}

}  // namespace std

#endif  // _LEPTON_STRONG_TYPE_H_
