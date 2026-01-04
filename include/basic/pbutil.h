#pragma once
#ifndef _LEPTON_PBUTIL_H_
#define _LEPTON_PBUTIL_H_
#include <cassert>
#include <string>

#include "basic/logger.h"
namespace lepton {
template <typename T>
concept protobuf_serializable = requires(const T& t, std::string& s) {
  { t.SerializeToString(&s) } -> std::same_as<bool>;
};

template <protobuf_serializable pb_message>
std::string must_serialize_to_string(const pb_message& msg, logger_interface& logger) {
  std::string data;
  if (!msg.SerializeToString(&data)) {
    assert(false);
    LOGGER_CRITICAL(logger, "serialize {} failed", msg.GetTypeName());
    return "";
  }
  return data;
}

template <protobuf_serializable pb_message>
void protobuf_must_parse(const std::string& data, pb_message& msg, logger_interface& logger) {
  if (!msg.ParseFromString(data)) {
    assert(false);
    LOGGER_CRITICAL(logger, "parse {} failed", msg.GetTypeName());
  }
}

}  // namespace lepton

#endif  // _LEPTON_PBUTIL_H_
