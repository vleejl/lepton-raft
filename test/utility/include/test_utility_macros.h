#define SECTION(name) \
  (void)(name);       \
  for (int _section_once_ = 0; _section_once_ < 1; ++_section_once_)

#ifdef LEPTON_CRITICAL
#undef LEPTON_CRITICAL
#endif

#define LEPTON_CRITICAL(...)      \
  do {                            \
    SPDLOG_CRITICAL(__VA_ARGS__); \
    std::abort();                 \
  } while (0)

// 封装逻辑：等待协程结束并断言其返回的 expected
#define AWAIT_EXPECT_OK(awaitable)                                      \
  do {                                                                  \
    auto res = co_await (awaitable);                                    \
    EXPECT_TRUE(res.has_value()) << "Error: " << res.error().message(); \
  } while (0)
