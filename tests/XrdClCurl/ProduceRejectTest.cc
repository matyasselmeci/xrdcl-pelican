/****************************************************************
 *
 * xrdcl-pelican implements an XRootD client plugin for interacting with the Pelican Platform
 * Copyright (C) 2026 Morgridge Institute for Research
 *
 * This library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library.  If not, see <https://www.gnu.org/licenses/>.
 *
 ***************************************************************/

// Regression tests for the use-after-free in HandlerQueue::Produce(): when the
// per-origin scheduler rejects an operation, Produce() must Fail() it on a live
// object. The bug moved the operation's last shared_ptr into the scheduler
// (which drops it on rejection, destroying the op) and then called the virtual
// Fail() through the freed vtable.

#include "XrdClCurl/XrdClCurlOps.hh"
#include "XrdClCurl/XrdClCurlTagScheduler.hh"
#include "XrdClCurl/XrdClCurlUtil.hh"

#include <XrdCl/XrdClDefaultEnv.hh>
#include <XrdCl/XrdClLog.hh>
#include <XrdCl/XrdClXRootDResponses.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

using namespace XrdClCurl;

namespace {

// Records the status an operation delivers to its client handler.
class RecordingHandler final : public XrdCl::ResponseHandler {
public:
    void HandleResponse(XrdCl::XRootDStatus *status,
                        XrdCl::AnyObject   *response) override {
        ++calls;
        code  = status->code;
        errNo = status->errNo;
        delete status;
        delete response;
    }
    int      calls = 0;
    uint16_t code  = 0;
    uint32_t errNo = 0;
};

// A no-network CurlOperation carrying a real ResponseHandler, so a rejection
// flows through the production Fail() -> HandleResponse() path.
class TestOp final : public CurlOperation {
public:
    TestOp(const std::string &url, XrdCl::Log *log, XrdCl::ResponseHandler *handler)
        : CurlOperation(handler, url, timespec{30, 0}, log, nullptr, nullptr) {}
    void     Success() override {}
    HttpVerb GetVerb() const override { return HttpVerb::GET; }
};

// One origin's pending FIFO is small, so a burst to that origin overflows it
// while the global buffer stays well clear.
TagScheduler::Config BurstCfg() {
    TagScheduler::Config c;
    c.per_tag_starving_percent = 90;
    c.per_tag_active_percent   = 90;
    c.pending_buffer_size      = 200;
    c.per_tag_pending_size     = 8;
    c.ema_window               = std::chrono::seconds(5);
    return c;
}

// Lifecycle events recorded into an observer that outlives the operation, so a
// verdict is reachable even without a sanitizer if the op's storage is reused.
struct OpObserver {
    bool fail_called        = false;
    bool destroyed          = false;
    bool fail_after_destroy = false;
};

// A minimal op that reports Fail() and destruction to an external observer.
// A null observer makes it an inert filler op.
class ProbeOp final : public CurlOperation {
public:
    ProbeOp(const std::string &url, XrdCl::Log *log, OpObserver *obs)
        : CurlOperation(nullptr, url, timespec{30, 0}, log, nullptr, nullptr),
          m_obs(obs) {}

    ~ProbeOp() override {
        if (m_obs) m_obs->destroyed = true;
    }

    void Fail(uint16_t, uint32_t, const std::string &) override {
        if (m_obs) {
            if (m_obs->destroyed) m_obs->fail_after_destroy = true;
            m_obs->fail_called = true;
        }
    }

    void Success() override {}
    HttpVerb GetVerb() const override { return HttpVerb::GET; }

private:
    OpObserver *m_obs;
};

// A global pending buffer of one slot: the first admission parks in the
// scheduler (never consumed), so any later admission is rejected.
TagScheduler::Config OneSlotCfg() {
    TagScheduler::Config c;
    c.per_tag_starving_percent = 90;
    c.per_tag_active_percent   = 90;
    c.pending_buffer_size      = 1;
    c.per_tag_pending_size     = 100;
    c.ema_window               = std::chrono::seconds(5);
    return c;
}

} // namespace

// A burst of requests to a single slow origin overflows that origin's pending
// FIFO; every shed request must be failed with errRetry, delivered to its
// ResponseHandler. This drives HandlerQueue::Produce the way the client does,
// and under AddressSanitizer it flags the freed-vtable dispatch directly.
TEST(HandlerQueueProduce, BurstBeyondOriginCapDeliversRetry) {
    auto *log = XrdCl::DefaultEnv::GetLog();

    constexpr int kBurst = 25;                       // > per_tag_pending_size
    std::vector<RecordingHandler> handlers(kBurst);  // outlives the scheduler
    TagScheduler sched(4, BurstCfg(), log);
    HandlerQueue queue(50);
    queue.SetScheduler(&sched);

    // Same origin for every request, nothing consuming the queue: the origin's
    // FIFO fills and the remainder are shed.
    for (int i = 0; i < kBurst; ++i) {
        queue.Produce(std::make_shared<TestOp>("https://slow.example.org/obj",
                                               log, &handlers[i]));
    }

    int rejected = 0;
    for (auto &h : handlers) {
        if (h.calls == 0) continue;   // accepted: queued, not yet delivered
        ++rejected;
        EXPECT_EQ(h.calls, 1);
        EXPECT_EQ(h.code, XrdCl::errRetry)
            << "a shed request must be failed with errRetry";
    }

    EXPECT_GT(rejected, 0)
        << "a burst beyond the per-origin cap must shed load";
    EXPECT_EQ(rejected, kBurst - sched.Pending())
        << "each request is either queued or failed -- none lost";
}

// A focused, deterministic companion: even without a sanitizer, a rejected op
// must be failed before it is destroyed.
TEST(HandlerQueueProduce, RejectedOpIsFailedNotFreed) {
    auto *log = XrdCl::DefaultEnv::GetLog();
    TagScheduler sched(4, OneSlotCfg(), log);

    HandlerQueue queue(50);
    queue.SetScheduler(&sched);

    // Occupy the single pending slot; this op parks in the scheduler and
    // forces the next admission to be rejected.
    queue.Produce(std::make_shared<ProbeOp>("https://filler/test", log, nullptr));

    // The op under test: Produce() receives (and moves in) the only reference.
    OpObserver obs;
    queue.Produce(std::make_shared<ProbeOp>("https://rejectme/test", log, &obs));

    EXPECT_TRUE(obs.fail_called)
        << "a scheduler-rejected operation must be Fail()ed";
    EXPECT_FALSE(obs.fail_after_destroy)
        << "Fail() must run on a live operation, not after it has been destroyed";
}
