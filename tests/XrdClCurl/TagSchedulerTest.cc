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

// Unit tests for the per-tag fair scheduler.

#include "XrdClCurl/XrdClCurlOps.hh"
#include "XrdClCurl/XrdClCurlTagScheduler.hh"

#include <XrdCl/XrdClDefaultEnv.hh>
#include <XrdCl/XrdClLog.hh>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

using namespace XrdClCurl;
using namespace std::chrono_literals;

namespace {

// Minimal concrete CurlOperation for driving the scheduler without
// touching curl. Same pattern as RedirectTest's TestRedirectOp:
// stubs out the two pure virtuals and takes a URL so
// SchedulerTagFor (in Util.cc) can parse a host from it.
class DummyOp final : public CurlOperation {
public:
    explicit DummyOp(const std::string &url, XrdCl::Log *log)
        : CurlOperation(nullptr, url, timespec{30, 0}, log, nullptr, nullptr)
    {}
    void Success() override {}
    HttpVerb GetVerb() const override { return HttpVerb::GET; }
};

std::shared_ptr<CurlOperation> MakeOp(const std::string &host) {
    return std::make_shared<DummyOp>("https://" + host + "/test",
                                     XrdCl::DefaultEnv::GetLog());
}

TagScheduler::Config PermissiveCfg() {
    TagScheduler::Config c;
    c.per_tag_starving_percent = 90;
    c.per_tag_active_percent   = 90;
    c.pending_buffer_size      = 200;
    c.per_tag_pending_size     = 100;
    c.ema_window               = 5s;
    return c;
}

} // namespace

// -----------------------------------------------------------------
// Baseline accept + dispatch.
// -----------------------------------------------------------------
TEST(TagScheduler, AcceptsAndDispatches) {
    TagScheduler sched(100, PermissiveCfg(), XrdCl::DefaultEnv::GetLog());
    const int N = 10;
    for (int i = 0; i < N; i++) {
        ASSERT_TRUE(sched.Admit("originA", MakeOp("originA")));
    }
    for (int i = 0; i < N; i++) {
        auto op = sched.TryConsume();
        ASSERT_NE(op, nullptr) << "iteration " << i;
    }
    // Nothing else in queue.
    ASSERT_EQ(sched.TryConsume(), nullptr);
    ASSERT_EQ(sched.Pending(), 0);
}

// -----------------------------------------------------------------
// Global pending buffer full → next admits rejected.
// -----------------------------------------------------------------
TEST(TagScheduler, GlobalBufferFullRejects) {
    auto c = PermissiveCfg();
    c.per_tag_starving_percent = 25; // starving cap = 1 (of 4)
    c.per_tag_active_percent   = 25;
    c.pending_buffer_size      = 3;
    c.per_tag_pending_size     = 100;
    TagScheduler sched(4, c, XrdCl::DefaultEnv::GetLog());

    // No TryConsume so the queue actually fills.
    for (int i = 0; i < 3; i++) {
        ASSERT_TRUE(sched.Admit("originA", MakeOp("originA")));
    }
    ASSERT_FALSE(sched.Admit("originA", MakeOp("originA")))
        << "global pending cap reached at 3, 4th admit must be rejected";
}

// -----------------------------------------------------------------
// Per-tag pending cap: one tag full doesn't block a different tag.
// -----------------------------------------------------------------
TEST(TagScheduler, PerTagPendingFullRejects) {
    auto c = PermissiveCfg();
    c.per_tag_starving_percent = 25; // starving cap = 1 (of 4)
    c.per_tag_active_percent   = 25;
    c.pending_buffer_size      = 100;
    c.per_tag_pending_size     = 2;
    TagScheduler sched(4, c, XrdCl::DefaultEnv::GetLog());

    ASSERT_TRUE(sched.Admit("originA", MakeOp("originA")));
    ASSERT_TRUE(sched.Admit("originA", MakeOp("originA")));
    // originA has a 3rd starving in the queue... no wait, per_tag_pending=2
    // means the FIFO holds AT MOST 2, and the 3rd is rejected.  Note: one
    // of the earlier admits may still be waiting in FIFO because the
    // starving cap (1) prevents dispatching more than one, and we haven't
    // called TryConsume.
    ASSERT_FALSE(sched.Admit("originA", MakeOp("originA")));

    // A different tag with an empty FIFO is still admissible.
    ASSERT_TRUE(sched.Admit("originB", MakeOp("originB")));
}

// -----------------------------------------------------------------
// Starving cap: no more than N% dispatched until first byte fires.
// -----------------------------------------------------------------
TEST(TagScheduler, StarvingCap) {
    auto c = PermissiveCfg();
    c.per_tag_starving_percent = 30; // starving cap = ceil(30%×10) = 3
    c.per_tag_active_percent   = 90;
    TagScheduler sched(10, c, XrdCl::DefaultEnv::GetLog());

    // Admit 10 for originA.
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(sched.Admit("originA", MakeOp("originA")));
    }
    // Only 3 can be dispatched; the 4th TryConsume returns nullptr.
    std::vector<std::shared_ptr<CurlOperation>> got;
    for (int i = 0; i < 3; i++) {
        auto op = sched.TryConsume();
        ASSERT_NE(op, nullptr) << "at i=" << i;
        got.push_back(op);
    }
    EXPECT_EQ(sched.TryConsume(), nullptr)
        << "starving cap should block a 4th dispatch until first-byte fires";
}

// -----------------------------------------------------------------
// First-byte hook lets more transfers dispatch.
// -----------------------------------------------------------------
TEST(TagScheduler, FirstByteReleasesStarvingSlot) {
    auto c = PermissiveCfg();
    c.per_tag_starving_percent = 30; // starving cap = 3
    c.per_tag_active_percent   = 90;
    TagScheduler sched(10, c, XrdCl::DefaultEnv::GetLog());

    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(sched.Admit("originA", MakeOp("originA")));
    }
    // Dispatch 3.
    std::vector<std::shared_ptr<CurlOperation>> got;
    for (int i = 0; i < 3; i++) {
        auto op = sched.TryConsume();
        ASSERT_NE(op, nullptr);
        got.push_back(op);
    }
    // 4th is blocked.
    ASSERT_EQ(sched.TryConsume(), nullptr);

    // Fire first-byte on the three. Scheduler decrements starving.
    for (auto &op : got) {
        ASSERT_TRUE(op->m_on_first_byte) << "scheduler should have installed the hook";
        op->m_on_first_byte();
    }
    // Now three more should dispatch.
    for (int i = 0; i < 3; i++) {
        auto op = sched.TryConsume();
        ASSERT_NE(op, nullptr) << "at i=" << i;
    }
}

// -----------------------------------------------------------------
// Active cap: even after first-byte, no more than N% may be active.
// -----------------------------------------------------------------
TEST(TagScheduler, ActiveCap) {
    auto c = PermissiveCfg();
    c.per_tag_starving_percent = 90; // starving is not the binding cap here
    c.per_tag_active_percent   = 40; // active cap = ceil(40%×10) = 4
    TagScheduler sched(10, c, XrdCl::DefaultEnv::GetLog());

    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(sched.Admit("originA", MakeOp("originA")));
    }
    std::vector<std::shared_ptr<CurlOperation>> got;
    for (int i = 0; i < 4; i++) {
        auto op = sched.TryConsume();
        ASSERT_NE(op, nullptr);
        got.push_back(op);
    }
    // Fire first-byte on all four — no longer starving, but still
    // occupying active slots.
    for (auto &op : got) op->m_on_first_byte();
    // 5th is blocked by active cap.
    ASSERT_EQ(sched.TryConsume(), nullptr)
        << "active cap should block dispatch until an op finishes";

    // Fire done on one — active decrements, one more dispatch allowed.
    got[0]->m_on_done();
    auto next = sched.TryConsume();
    ASSERT_NE(next, nullptr);
}

// -----------------------------------------------------------------
// Fairness across two backlogged tags: each caps at its own share.
// -----------------------------------------------------------------
TEST(TagScheduler, FairnessAcrossTags) {
    auto c = PermissiveCfg();
    c.per_tag_starving_percent = 25; // cap per tag = ceil(25%×10) = 3
    TagScheduler sched(10, c, XrdCl::DefaultEnv::GetLog());

    for (int i = 0; i < 10; i++) ASSERT_TRUE(sched.Admit("originA", MakeOp("originA")));
    for (int i = 0; i < 10; i++) ASSERT_TRUE(sched.Admit("originB", MakeOp("originB")));

    // Drain everything the scheduler is willing to dispatch under the
    // starving cap: at most 3 per tag = 6 total.
    std::map<std::string, int> per_tag_dispatched;
    while (auto op = sched.TryConsume()) {
        // Derive tag from the URL host, same way SchedulerTagFor does.
        std::string host = op->GetUrl().substr(std::string("https://").size());
        host = host.substr(0, host.find('/'));
        per_tag_dispatched[host]++;
    }
    EXPECT_EQ(per_tag_dispatched["originA"], 3);
    EXPECT_EQ(per_tag_dispatched["originB"], 3);
}

// -----------------------------------------------------------------
// Snapshot / GetMonitoringJson exposes the per-tag counters.
// -----------------------------------------------------------------
TEST(TagScheduler, MonitoringJsonHasPerTagDetail) {
    auto c = PermissiveCfg();
    c.per_tag_starving_percent = 25; // cap = 1 (of 4)
    c.per_tag_active_percent   = 25;
    c.per_tag_pending_size     = 2;
    TagScheduler sched(4, c, XrdCl::DefaultEnv::GetLog());

    ASSERT_TRUE(sched.Admit("badorigin.example.com", MakeOp("badorigin.example.com")));
    ASSERT_TRUE(sched.Admit("badorigin.example.com", MakeOp("badorigin.example.com")));
    ASSERT_FALSE(sched.Admit("badorigin.example.com", MakeOp("badorigin.example.com")))
        << "per-tag FIFO cap should reject the third";

    std::string js = sched.GetMonitoringJson();
    // Cheap substring checks — we don't pull in a JSON parser here.
    EXPECT_NE(js.find("\"per_tag\":"), std::string::npos)
        << "monitoring JSON should have a per_tag key";
    EXPECT_NE(js.find("\"origin\":\"badorigin.example.com\""), std::string::npos)
        << "monitoring JSON should include the origin string";
    EXPECT_NE(js.find("\"rejects_per_tag\":1"), std::string::npos)
        << "monitoring JSON should split rejects by reason";
    EXPECT_NE(js.find("\"admits\":2"), std::string::npos);
}

// -----------------------------------------------------------------
// Shutdown wakes any parked Consume.
// -----------------------------------------------------------------
TEST(TagScheduler, ShutdownWakesConsume) {
    TagScheduler sched(4, PermissiveCfg(), XrdCl::DefaultEnv::GetLog());
    std::atomic<bool> returned{false};
    std::thread t([&]() {
        auto op = sched.Consume(1s);
        (void)op;
        returned = true;
    });
    // Give the consumer a moment to park.
    std::this_thread::sleep_for(20ms);
    sched.Shutdown();
    t.join();
    ASSERT_TRUE(returned);
}
