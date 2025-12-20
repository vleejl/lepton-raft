#pragma once
#ifndef _LEPTON_TEST_RAFT_STATE_MACHINE_H_
#define _LEPTON_TEST_RAFT_STATE_MACHINE_H_
#include <proxy.h>

#include "error/lepton_error.h"
#include "raft_core/pb/types.h"

PRO_DEF_MEM_DISPATCH(state_machine_step, step);

PRO_DEF_MEM_DISPATCH(state_machine_read_messages, read_messages);

PRO_DEF_MEM_DISPATCH(state_machine_advance_messages_after_append, advance_messages_after_append);

// clang-format off
struct state_machine_builer : pro::facade_builder 
  ::add_convention<state_machine_step, lepton::leaf::result<void>(raftpb::message&&)> 
  ::add_convention<state_machine_read_messages, lepton::core::pb::repeated_message()>
  ::add_convention<state_machine_advance_messages_after_append, void()>
  ::add_skill<pro::skills::as_view>
  ::add_skill<pro::skills::rtti>
  ::build{};
// clang-format on

#endif  // _LEPTON_TEST_RAFT_STATE_MACHINE_H_
