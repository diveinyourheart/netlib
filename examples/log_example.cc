#include <utility>

#include "netlib/log/logging.h"

int main() {
  netlib::log::LoggerOptions options{
      .name = "netlib.example",
      .minimum_level = netlib::log::LogLevel::kTrace,
      .queue_capacity = 1024,
      .color_mode = netlib::log::ColorMode::kAuto,
      .flush_each_record = false,
  };
  if (!netlib::log::Logger::InitializeStdout(std::move(options))) {
    return 1;
  }
  NETLIB_LOG_TRACE("trace message");
  NETLIB_LOG_DEBUG("debug message");
  NETLIB_LOG_INFO("server started on port={} workers={}", 8080, 4);
  NETLIB_LOG_WARN("worker queue is above 80%");
  NETLIB_LOG_ERROR("request failed\nretry scheduled");
  return netlib::log::Logger::Flush() ? 0 : 1;
}
