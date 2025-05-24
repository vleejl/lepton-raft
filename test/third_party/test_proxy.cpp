#include <gtest/gtest.h>
#include <proxy.h>

#include <memory>
#include <vector>

#include "test_raft_networking.h"
#include "test_raft_state_machine.h"
class proxy_test_suit : public testing::Test {
 protected:
  static void SetUpTestSuite() { std::cout << "run before first case..." << std::endl; }

  static void TearDownTestSuite() { std::cout << "run after last case..." << std::endl; }

  virtual void SetUp() override { std::cout << "enter from SetUp" << std::endl; }

  virtual void TearDown() override { std::cout << "exit from TearDown" << std::endl; }
};

struct proxy_test_state_machine_builer {
  lepton::leaf::result<void> step(raftpb::message &&) { return {}; }

  lepton::pb::repeated_message read_messages() { return {}; }

  void advance_messages_after_append() { std::cout << data << std::endl; }

  proxy_test_state_machine_builer() = default;
  explicit proxy_test_state_machine_builer(std::string data) : data(std::move(data)) {}
  std::string data;
};

TEST_F(proxy_test_suit, test_rtti) {
  auto p = pro::make_proxy<state_machine_builer, proxy_test_state_machine_builer>();
  pro::proxy_view<state_machine_builer> pro_view = p;
  std::cout << proxy_typeid(p).name() << "\n";
  std::cout << proxy_typeid(*pro_view).name() << "\n";
  pro::proxy_view<state_machine_builer> empty_pro_view;
  if (empty_pro_view) {
    std::cout << proxy_typeid(*empty_pro_view).name() << "\n";
  } else {
    std::cout << "empty proxy\n";
  }
  std::cout << proxy_typeid(*empty_pro_view).name() << "\n";
}

void test_proxy_lifetime(pro::proxy<state_machine_builer> &&p) {
  p->advance_messages_after_append();
  pro::proxy_view<state_machine_builer> p_view = p;
  std::cout << proxy_typeid(*p_view).name() << "\n";
  std::cout << proxy_typeid(*p_view).name() << "\n";
}

TEST_F(proxy_test_suit, proxy_lifetime) {
  std::vector<pro::proxy<state_machine_builer>> proxies;
  std::vector<pro::proxy_view<state_machine_builer>> proxy_views;
  {
    for (int i = 0; i < 10; ++i) {
      auto p = pro::make_proxy<state_machine_builer, proxy_test_state_machine_builer>("test data");
      proxies.push_back(std::move(p));
      proxy_views.push_back(proxies.back());
    }
  }
  for (auto &p : proxy_views) {
    p->advance_messages_after_append();
  }
  // 右值
  test_proxy_lifetime(pro::make_proxy<state_machine_builer, proxy_test_state_machine_builer>("test data 1"));
  proxy_test_state_machine_builer obj2("test data 2");
  auto p = pro::make_proxy<state_machine_builer, proxy_test_state_machine_builer>(std::move(obj2));
  p->advance_messages_after_append();
  test_proxy_lifetime(std::move(p));
}