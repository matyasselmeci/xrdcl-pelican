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

// Regression test for a use-after-free in HandlerQueue::Produce(): when the
// scheduler rejects an op, Produce() must Fail() it on a live object rather
// than after its last shared_ptr has been dropped.

#include "XrdClCurl/XrdClCurlOps.hh"
#include "XrdClCurl/XrdClCurlTagScheduler.hh"
#include "XrdClCurl/XrdClCurlUtil.hh"

#include <XrdCl/XrdClDefaultEnv.hh>
#include <XrdCl/XrdClLog.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

using namespace XrdClCurl;

namespace {

// Lifecycle events recorded into an observer that outlives the operation,
// so a verdict is reachable even if the op's own storage has been recycled.
struct OpObserver {
    bool fail_called        = false;
    bool destroyed          = false;
    bool fail_after_destroy = false;
};

// Minimal concrete CurlOperation (same construction pattern as the scheduler
// test's DummyOp) that reports Fail() and destruction to an external observer.
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

// A global pending buffer of one slot: the first admission is accepted and
// (never consumed) parks in the scheduler, so any later admission is rejected.
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

TEST(HandlerQueueProduce, RejectedOpIsFailedNotFreed) {
    auto *log = XrdCl::DefaultEnv::GetLog();
    TagScheduler sched(4, OneSlotCfg(), log);

    HandlerQueue queue(50);
    queue.SetScheduler(&sched);

    // Occupy the single pending slot. This op is accepted and parked in the
    // scheduler (never consumed), forcing the next admission to be rejected.
    queue.Produce(std::make_shared<ProbeOp>("https://filler/test", log, nullptr));

    // The op under test: Produce() receives (and moves in) the only reference.
    OpObserver obs;
    queue.Produce(std::make_shared<ProbeOp>("https://rejectme/test", log, &obs));

    EXPECT_TRUE(obs.fail_called)
        << "a scheduler-rejected operation must be Fail()ed";
    EXPECT_FALSE(obs.fail_after_destroy)
        << "Fail() must run on a live operation, not after it has been destroyed";
}
