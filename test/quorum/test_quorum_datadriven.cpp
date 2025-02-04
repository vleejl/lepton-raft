#include <fmt/core.h>
#include <gtest/gtest.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "cli.h"
#include "data_driven.h"
#include "joint.h"
#include "majority.h"
#include "proxy.h"
#include "quorum.h"
#include "utility_data_test.h"
using namespace lepton;

std::string vote_result2etcd_raft(quorum::vote_result v) {
  switch (v) {
    case quorum::vote_result::VOTE_PENDING: {
      return "VotePending";
    }
    case quorum::vote_result::VOTE_LOST: {
      return "VoteLost";
    }
    case quorum::vote_result::VOTE_WON: {
      return "VoteWon";
    }
  }
  assert(false);
}

quorum::map_ack_indexer make_lookuper(
    const std::vector<quorum::log_index>& idxs,
    const std::vector<uint64_t>& ids, const std::vector<uint64_t>& idsj) {
  std::map<std::uint64_t, quorum::log_index> l;
  size_t p = 0;  // next to consume from idxs

  // Append ids and idsj together
  std::vector<uint64_t> combined_ids = ids;
  combined_ids.insert(combined_ids.end(), idsj.begin(), idsj.end());

  // Populate the map
  for (auto id : combined_ids) {
    if (l.find(id) != l.end()) {
      continue;  // Skip if id already exists in the map
    }
    if (p < idxs.size()) {
      l[id] = idxs[p];
      p++;
    }
  }

  // Remove zero entries
  for (auto it = l.begin(); it != l.end();) {
    if (it->second == 0) {
      it = l.erase(it);  // Erase and move to the next entry
    } else {
      ++it;
    }
  }
  return quorum::map_ack_indexer{std::move(l)};
}

// This is an alternative implementation of (MajorityConfig).CommittedIndex(l).
quorum::log_index alternative_majority_committed_index(
    const quorum::majority_config& c,
    pro::proxy_view<quorum::acked_indexer_builer> l) {
  if (c.empty()) {
    return quorum::INVALID_LOG_INDEX;
  }

  std::unordered_map<std::uint64_t, quorum::log_index> id_to_idx;
  for (auto id : c.view()) {
    auto idx = l->acked_index(id);
    if (idx) {
      id_to_idx[id] = idx.value();
    }
  }

  // Build a map from index to voters who have acked that or any higher index.
  std::unordered_map<quorum::log_index, int> idx_to_votes;
  // 使用 ranges 和 tie 解包遍历 map
  for (auto&& [_, log_idx] : id_to_idx) {
    idx_to_votes[log_idx] = 0;
  }

  for (auto&& [_, log_id_x] : id_to_idx) {
    for (auto&& [log_id_y, _] : idx_to_votes) {
      if (log_id_y > log_id_x) {
        continue;
      }
      idx_to_votes[log_id_y]++;
    }
  }

  quorum::log_index max_quorum_idx = 0;
  // Find the maximum index that has achieved quorum.
  auto q = c.size() / 2 + 1;
  for (auto&& [log_index, n] : idx_to_votes) {
    if (n >= q && log_index > max_quorum_idx) {
      max_quorum_idx = log_index;
    }
  }
  return max_quorum_idx;
}

static std::string process_single_test_case(
    const std::string& cmd, const std::string& input,
    const std::map<std::string, std::vector<std::string>>& args_map) {
  // Two majority configs. The first one is always used (though it may
  // be empty) and the second one is used iff joint is true.
  bool joint = false;
  std::vector<std::uint64_t> ids;
  std::vector<std::uint64_t> idsj;

  // The committed indexes for the nodes in the config in the order in
  // which they appear in (ids,idsj), without repetition. An underscore
  // denotes an omission (i.e. no information for this voter); this is
  // different from 0. For example,
  //
  // cfg=(1,2) cfgj=(2,3,4) idxs=(_,5,_,7) initializes the idx for voter 2
  // to 5 and that for voter 4 to 7 (and no others).
  //
  // cfgj=zero is specified to instruct the test harness to treat cfgj
  // as zero instead of not specified (i.e. it will trigger a joint
  // quorum test instead of a majority quorum test for cfg only).
  std::vector<quorum::log_index> idxs;
  // Votes. These are initialized similar to idxs except the only values
  // used are 1 (voted against) and 2 (voted for). This looks awkward,
  // but is convenient because it allows sharing code between the two.
  std::vector<quorum::log_index> votes;

  for (const auto& [arg_key, arg_value_vec] : args_map) {
    if (arg_key == "cfg") {
      for (const auto& item : arg_value_vec) {
        auto _result = safe_stoull(item);
        if (!_result.has_value()) {
          assert(_result.has_value());
        }
        ids.push_back(_result.value());
      }
    } else if (arg_key == "cfgj") {
      joint = true;
      for (const auto& item : arg_value_vec) {
        if (item == "zero") {
          auto size = arg_value_vec.size();
          if (size != 1u) {
            assert(false);
          }
        } else {
          for (const auto& item : arg_value_vec) {
            auto _result = safe_stoull(item);
            if (!_result.has_value()) {
              assert(_result.has_value());
            }
            idsj.push_back(_result.value());
          }
        }
      }
    } else if (arg_key == "idx") {
      for (const auto& item : arg_value_vec) {
        std::uint64_t n = 0;
        if (item != "_") {
          auto _result = safe_stoull(item);
          if (!_result.has_value()) {
            assert(_result.has_value());
          }
          n = _result.value();
          assert(n != 0);
        }
        idxs.push_back(n);
      }
    } else if (arg_key == "votes") {
      for (const auto& item : arg_value_vec) {
        if (item == "y") {
          votes.push_back(2);
        } else if (item == "n") {
          votes.push_back(1);
        } else if (item == "_") {
          votes.push_back(0);
        } else {
          assert(false);
        }
      }
    } else {
      assert(false);
    }
  }

  // Build the two majority configs.
  quorum::majority_config c(std::set<std::uint64_t>{ids.begin(), ids.end()});
  quorum::majority_config cj(std::set<std::uint64_t>{idsj.begin(), idsj.end()});
  {
    auto input = &idxs;
    if (cmd == "vote") {
      input = &votes;
    }
    auto voters = quorum::joint_config{c.clone(), cj.clone()};
    if (voters.id_set().size() != input->size()) {
      // fmt.Sprintf("error: mismatched input (explicit or _) for voters %v:
      // %v", voters, input)
      assert(voters.id_set().size() == input->size());
    }
  }

  std::ostringstream buf;
  if (cmd == "committed") {
    auto l = make_lookuper(idxs, ids, idsj);
    pro::proxy_view<quorum::acked_indexer_builer> l_pro_view = &l;

    // Branch based on whether this is a majority or joint quorum
    // test case.
    if (!joint) {
      auto idx = c.committed_index(l_pro_view);
      buf << c.describe(l_pro_view);
      // These alternative computations should return the same
      // result. If not, print to the output.
      if (auto a_idx = alternative_majority_committed_index(c, l_pro_view);
          a_idx != idx) {
        buf << quorum::log_index_to_string(a_idx)
            << " <-- via alternative computation\n";
      }
      // Joining a majority with the empty majority should give same result.
      if (auto a_idx =
              quorum::joint_config{c.clone()}.committed_index(l_pro_view);
          a_idx != idx) {
        buf << quorum::log_index_to_string(a_idx)
            << " <-- via zero-joint quorum\n";
      }
      // Joining a majority with itself should give same result.
      if (auto a_idx = quorum::joint_config(c.clone(), c.clone())
                           .committed_index(l_pro_view);
          a_idx != idx) {
        buf << quorum::log_index_to_string(a_idx)
            << " <-- via self-joint quorum\n";
      }

      auto overlay = [](const quorum::majority_config& c,
                        pro::proxy_view<quorum::acked_indexer_builer> l,
                        std::uint64_t id,
                        quorum::log_index idx) -> quorum::map_ack_indexer {
        std::map<std::uint64_t, quorum::log_index> ll;
        for (auto iid : c.view()) {
          if (iid == id) {
            ll[iid] = idx;
          } else if (auto ack_idx = l->acked_index(iid); ack_idx) {
            ll[iid] = ack_idx.value();
          }
        }
        return quorum::map_ack_indexer{std::move(ll)};
      };
      for (auto id : c.view()) {
        if (auto iidx_result = l.acked_index(id); iidx_result) {
          auto iidx = iidx_result.value();
          if (idx > iidx && iidx > 0) {
            // If the committed index was definitely above the currently
            // inspected idx, the result shouldn't change if we lower it
            // further.
            auto lo = overlay(c, l_pro_view, id, iidx - 1);
            pro::proxy_view<quorum::acked_indexer_builer> lo_pro_view = &lo;
            if (auto a_idx = c.committed_index(lo_pro_view); a_idx != idx) {
              buf << quorum::log_index_to_string(a_idx) << " <-- overlaying "
                  << id << "->" << iidx;
            }

            auto lo0 = overlay(c, l_pro_view, id, iidx);
            pro::proxy_view<quorum::acked_indexer_builer> lo0_pro_view = &lo0;
            if (auto a_idx = c.committed_index(lo0_pro_view); a_idx != idx) {
              buf << quorum::log_index_to_string(a_idx) << " <-- overlaying "
                  << id << "->" << iidx;
            }
          }
        }
      }
      buf << quorum::log_index_to_string(idx) << "\n";
    } else {
      quorum::joint_config cc{c.clone(), cj.clone()};
      buf << cc.describe(l_pro_view);
      auto idx = cc.committed_index(l_pro_view);
      // Interchanging the majorities shouldn't make a difference. If it does,
      // print.
      if (auto a_idx =
              quorum::joint_config{cj.clone(), c.clone()}.committed_index(
                  l_pro_view);
          a_idx != idx) {
        buf << quorum::log_index_to_string(a_idx) << " <-- via symmetry\n";
      }
      buf << quorum::log_index_to_string(idx) << "\n";
    }
  } else if (cmd == "vote") {
    auto ll = make_lookuper(votes, ids, idsj);
    std::unordered_map<std::uint64_t, bool> l;
    for (const auto& [id, v] : ll.view()) {
      l[id] = v != 1;  // NB: 1 == false, 2 == true
    }

    if (!joint) {
      // Test a majority quorum.
      auto r = c.vote_result_statistics(l);
      buf << vote_result2etcd_raft(r) << "\n";
    } else {
      // Run a joint quorum test case.
      auto r =
          quorum::joint_config{c.clone(), cj.clone()}.vote_result_statistics(l);
      if (auto ar = quorum::joint_config{cj.clone(), c.clone()}
                        .vote_result_statistics(l);
          ar != r) {
        buf << fmt::format("{} <-- via symmetry\n", vote_result2etcd_raft(ar));
      }
      buf << vote_result2etcd_raft(r) << "\n";
    }
  } else {
    assert(false);
  }
  return buf.str();
}

TEST(quorum_data_driven_test_suit, test_data_driven_impl) {
  // 获取当前文件的路径
  std::filesystem::path current_file = __FILE__;

  // 获取当前文件所在目录
  std::filesystem::path current_dir = current_file.parent_path();

  // 拼接 "testdata" 目录
  std::filesystem::path project_dir = LEPTON_PROJECT_DIR;
  std::string test_dir = (current_dir / "testdata").string();

  // List all test files in the specified directory
  data_driven_group group{project_dir / (current_dir / "testdata").string()};
  data_driven::process_func func = process_single_test_case;
  group.run(func);
}