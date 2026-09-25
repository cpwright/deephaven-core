/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#define CATCH_CONFIG_RUNNER
#include <chrono>
#include <cstdio>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using deephaven::client::tests::GlobalEnvironmentForTests;

namespace {
/**
 * Logs the start and end of every test case straight to the stderr file descriptor. The XML
 * reporter captures std::cout and std::cerr into its report, which is lost if the process dies on
 * a signal or is killed for hanging; these lines bypass that capture, so the last one printed
 * names the test that was running.
 */
class ProgressListener final : public Catch::TestEventListenerBase {
public:
  using TestEventListenerBase::TestEventListenerBase;

  void testCaseStarting(const Catch::TestCaseInfo &info) final {
    Log("START", info.name.c_str());
  }

  void testCaseEnded(const Catch::TestCaseStats &stats) final {
    Log(stats.totals.assertions.allOk() ? "PASS" : "FAIL", stats.testInfo.name.c_str());
  }

private:
  static void Log(const char *what, const char *name) {
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::fprintf(stderr, "[progress %lld.%03lld] %s: %s\n", static_cast<long long>(millis / 1000),
        static_cast<long long>(millis % 1000), what, name);
    std::fflush(stderr);
  }
};
}  // namespace

CATCH_REGISTER_LISTENER(ProgressListener)

int main(int argc, char *argv[], char **envp) {
  // Process envp so we don't have to call getenv(), which Windows complains about.
  GlobalEnvironmentForTests::Init(envp);
  return Catch::Session().run(argc, argv);
}
