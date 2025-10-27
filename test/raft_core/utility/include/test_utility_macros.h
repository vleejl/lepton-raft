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
