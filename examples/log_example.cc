#include <memory>
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
  std::unique_ptr<netlib::log::Logger> logger =
      netlib::log::Logger::CreateStdout(std::move(options));
  if (!logger) {
    return 1;
  }

  NETLIB_LOG_TRACE(*logger, "trace message");
  NETLIB_LOG_DEBUG(*logger, "debug message");
  NETLIB_LOG_INFO(*logger, "server started on port={} workers={}", 8080, 4);
  NETLIB_LOG_WARN(*logger, "worker queue is above 80%");
  NETLIB_LOG_ERROR(*logger, "request failed\nretry scheduled");
  logger->Flush();
  logger->Stop();
  return 0;
}
