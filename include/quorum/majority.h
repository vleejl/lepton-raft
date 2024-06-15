#ifndef _LEPTON_MAJORITY_H_
#define _LEPTON_MAJORITY_H_

#include "proxy.h"
#include "quorum.h"
#include "utility_macros.h"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <fmt/core.h>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>
namespace lepton {

// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
class majority_config {
  NONCOPYABLE_NONMOVABLE(majority_config)
  friend class joint_config;

public:
  majority_config() = default;
  majority_config(std::set<std::uint64_t> &&id_set)
      : id_set_(std::move(id_set)) {}

  std::string string() const {
    std::ostringstream ss;
    ss << '(';
    for (auto iter = id_set_.begin(); iter != id_set_.end(); ++iter) {
      if (iter != id_set_.begin()) {
        ss << ' ';
      }
      ss << *iter;
    }
    ss << ')';
    return ss.str();
  }

  // Describe returns a (multi-line) representation of the commit indexes for
  // the given lookuper.
  std::string describe(pro::proxy<acked_indexer_builer> indexer) const {
    if (id_set_.empty()) {
      return "<empty majority quorum>";
    }

    struct tup {
      std::uint64_t id;
      log_index index;
      bool ok;
      int bar;
    };

    auto n = id_set_.size();
    std::vector<tup> info;
    for (auto iter : id_set_) {
      if (auto result = indexer.acked_index(iter)) {
        info.emplace_back(tup{iter, result.value(), true, 0});
      } else {
        info.emplace_back(tup{iter, 0, false, 0});
      }
    }

    std::sort(info.begin(), info.end(), [](const tup &lhs, const tup &rhs) {
      if (lhs.index == rhs.index) {
        return lhs.id < rhs.id;
      }
      return lhs.index < rhs.index;
    });

    for (std::size_t i = 0; i < info.size(); i++) {
      if ((i > 0) && (info[i - 1].index < info[i].index)) {
        info[i].bar = i;
      }
    }

    std::sort(info.begin(), info.end(),
              [](const tup &lhs, const tup &rhs) { return lhs.id < rhs.id; });
    std::ostringstream ss;
    ss << std::string(n, ' ') << "    idx\n";
    for (const auto &iter : info) {
      if (!iter.ok) {
        ss << '?' << std::string(n, ' ');
      } else {
        ss << std::string(iter.bar, 'x') << '>'
           << std::string(n - iter.bar, ' ');
      }
      ss << fmt::format(" {:5d}    (id={})\n", iter.index, iter.id);
    }
    return ss.str();
  }

  std::vector<std::uint64_t> slice() const {
    return std::vector<std::uint64_t>{id_set_.begin(), id_set_.end()};
  }

  // CommittedIndex computes the committed index from those supplied via the
  // provided AckedIndexer (for the active config).
  log_index committed_index(pro::proxy<acked_indexer_builer> indexer) const {
    if (id_set_.empty()) {
      // This plays well with joint quorums which, when one half is the zero
      // MajorityConfig, should behave like the other half.
      return INVALID_LOG_INDEX;
    }

    // Fill the slice with the indexes observed. Any unused slots will be
    // left as zero; these correspond to voters that may report in, but
    // haven't yet. We fill from the right (since the zeroes will end up on
    // the left after sorting below anyway).
    std::vector<log_index> s(id_set_.size(), 0);
    std::size_t i = 0;
    for (const auto &iter : id_set_) {
      if (auto result = indexer.acked_index(iter)) {
        assert(iter < s.size());
        s[i] = result.value();
        i++;
      }
    }

    std::sort(s.begin(), s.end());

    // The smallest index into the array for which the value is acked by a
    // quorum. In other words, from the end of the slice, move n/2+1 to the
    // left (accounting for zero-indexing).
    auto pos = id_set_.size() - (id_set_.size() / 2 + 1);
    return s[pos];
  }

  // VoteResult takes a mapping of voters to yes/no (true/false) votes and
  // returns a result indicating whether the vote is pending (i.e. neither a
  // quorum of yes/no has been reached), won (a quorum of yes has been reached),
  // or lost (a quorum of no has been reached).
  vote_result
  vote_result_statistics(const std::map<std::uint64_t, bool> &vote) const {
    if (id_set_.empty()) {
      // By convention, the elections on an empty config win. This comes in
      // handy with joint quorums because it'll make a half-populated joint
      // quorum behave like a majority quorum.
      return vote_result::VOTE_MIN;
    }

    auto pending_counter = 0;
    auto win_counter = 0;
    for (const auto &iter : id_set_) {
      auto val_iter = vote.find(iter);
      if (val_iter == vote.end()) {
        pending_counter++;
        continue;
      }
      if (val_iter->second) {
        win_counter++;
      } else {
        // lost_counter++;
      }
    }

    auto q = static_cast<int>(id_set_.size() / 2 + 1);
    if (win_counter >= q) {
      return vote_result::VOTE_MIN;
    }
    if (win_counter + pending_counter >= q) {
      return vote_result::VOTE_PENDING;
    }
    return vote_result::VOTE_LOST;
  }

private:
  std::set<std::uint64_t> id_set_;
};
} // namespace lepton

#endif // _LEPTON_MAJORITY_H_
