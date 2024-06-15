#ifndef _LEPTON_RAFT_NODE_
#define _LEPTON_RAFT_NODE_

#include "config.h"
#include "leaf.hpp"
#include "logger.h"
#include "majority.h"

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <system_error>

namespace lepton {
using asio_handler = std::function<void(const std::error_code &)>;
namespace leaf = boost::leaf;

enum class raft_state {
  LEARNER,
  FOLLOWER,
  PRE_CANDIDATE,
  CANDIDATE,
  LEADER,
};

class raft_node {
public:
  leaf::result<void> handle_msg() { return leaf::result<void>(); }

  void reset_randomized_election_timeout() {
    this->randomized_election_timeout =
        this->config_.election_tick + rand() % this->config_.election_tick;
  }

  void abort_leader_transfer() { this->leader_transferee_id = NONE; }

  void reset(std::uint64_t term) {
    if (this->term != term) {
      this->term = term;
      // If the reset term does not match the current term,
      // it means a new round of voting will be conducted.
      // At this point, it is necessary to reset the vote ID to None.
      this->vote_id = NONE;
    }
    this->leader_id = NONE;
    this->abort_leader_transfer();

    this->election_elapsed = 0;
    this->heartbeat_elapsed = 0;
    this->reset_randomized_election_timeout();

    /*
      TODO
        r.prs.ResetVotes()
        r.prs.Visit(func(id uint64, pr *tracker.Progress) {
                *pr = tracker.Progress{
                        Match:     0,
                        Next:      r.raftLog.lastIndex() + 1,
                        Inflights: tracker.NewInflights(r.prs.MaxInflight),
                        IsLearner: pr.IsLearner,
                }
                if id == r.id {
                        pr.Match = r.raftLog.lastIndex()
                }
        })

        r.pendingConfIndex = 0
        r.uncommittedSize = 0
        r.readOnly = newReadOnly(r.readOnly.option)
     */
  }

  // promotable indicates whether state machine can be promoted to leader,
  // which is true when its own id is in progress list.
  bool promotable() {
    // TODO
    return true;
  }

  // pastElectionTimeout returns true iff r.electionElapsed is greater
  // than or equal to the randomized election timeout in
  // [electiontimeout, 2 * electiontimeout - 1].
  bool past_election_timeout() {
    return this->election_elapsed > this->randomized_election_timeout;
  }

  void tick_election() {
    this->election_elapsed++;
    if (this->promotable() && this->past_election_timeout()) {
      // Satisfy the demand and allow to enter the next state.
      // At this point, reset election_elapsed
      this->election_elapsed = 0;
    }
  }

  void become_follower(std::uint64_t term, std::uint64_t leader) {
    this->state = raft_state::FOLLOWER;
    this->reset(term);
    this->leader_id = leader;
    this->term = term;
    spdlog::info("{} become follower at term:{}", this->id, this->term);
  }

private:
  const std::uint64_t &id;
  // the current leader id
  std::uint64_t leader_id;
  // indicates the leader ID of the current raft preparing to vote
  std::uint64_t vote_id;
  std::uint64_t leader_transferee_id;

  std::uint64_t term;
  raft_state state;

  // number of ticks since it reached last electionTimeout when it is leader
  // or candidate.
  // number of ticks since it reached last electionTimeout or received a
  // valid message from current leader when it is a follower.
  std::uint32_t election_elapsed;
  std::uint32_t heartbeat_elapsed;

  // randomized_election_timeout is a random number between
  // [electiontimeout, 2 * electiontimeout - 1]. It gets reset
  // when raft changes its state to follower or candidate.
  int randomized_election_timeout;

  const config config_;
};
} // namespace lepton

#endif // _LEPTON_RAFT_NODE_