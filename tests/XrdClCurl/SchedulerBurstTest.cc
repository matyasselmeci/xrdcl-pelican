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

// HTTP-level integration test for the TagScheduler: fires a burst of
// concurrent GETs at the "tight-scheduler" cache (launched by
// curl-setup.sh with XRD_CURLPENDINGBUFFER=1 &c.) so a single origin
// tag saturates the scheduler and the extras are shed with an HTTP
// error the client can see.
//
// The requests target /test-public/slow_read.txt, which the test fixture's
// slow-OSS layer serves at 500 KB/s from a 1 GiB "file"; that keeps
// admitted transfers occupying the pool long enough for concurrent
// admissions to overflow the buffer and get rejected via
// HandlerQueue::Produce -> handler->Fail(errRetry, …).

#include "../XrdClCurlCommon/TransferTest.hh"

#include <curl/curl.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

// libcurl WRITEDATA callback that appends the received body bytes
// into the std::string passed as the userdata pointer.  We capture
// the body so the test can assert the "resource temporarily
// unavailable" marker that identifies a scheduler-shed response.
size_t AppendBody(char *ptr, size_t size, size_t nmemb, void *userdata) {
    auto *dst = static_cast<std::string *>(userdata);
    dst->append(ptr, size * nmemb);
    return size * nmemb;
}

struct Result {
    long http_status = 0;
    long curl_code   = 0;
    std::string curl_error;
    std::string body;
};

} // namespace

class SchedulerBurst : public TransferFixture {};

// The end-to-end wire test: fire N concurrent GETs at the tight
// cache and confirm that at least one comes back with a non-2xx
// status code (i.e., the cache surfaced the scheduler rejection to
// the HTTP client rather than silently piling requests onto stalled
// slots).  We do NOT assert a specific HTTP code — the mapping from
// XrdCl errRetry into an XrdHttp response depends on the installed
// XRootD version; the operator contract we care about is "the wire
// visibly refuses under overload."
TEST_F(SchedulerBurst, BurstShedsRequestsUnderTightCaps) {
    const std::string tight_cache = GetEnv("TIGHT_CACHE_URL");
    ASSERT_FALSE(tight_cache.empty())
        << "curl-setup.sh must export TIGHT_CACHE_URL — did the tight "
           "cache fail to start?";
    const std::string ca_file = GetEnv("X509_CA_FILE");
    ASSERT_FALSE(ca_file.empty());

    // /test-public/slow_read.txt: fed by the slow-OSS layer (see
    // tests/XrdClCurlCommon/XrdOssSlowOpen.cc) at ~500 KB/s from a
    // 1 GiB "file", so an admitted transfer stays in-flight long
    // enough for concurrent admissions to fill the scheduler's
    // 1-slot FIFO.
    //
    // The tight cache is configured with a permissive authdb
    // (u * /test-public lr) so we don't have to entangle this test with
    // the fixture's SciTokens setup — the property under test is
    // the scheduler's shed behaviour, not auth.
    const std::string url = tight_cache + "/test-public/slow_read.txt";

    // Some hosts curl_easy_init lazily; make sure global init is
    // done before the threads fan out.
    ASSERT_EQ(curl_global_init(CURL_GLOBAL_DEFAULT), 0);

    constexpr int kBurst = 20;
    std::vector<Result> results(kBurst);
    std::vector<std::thread> workers;
    workers.reserve(kBurst);

    for (int i = 0; i < kBurst; i++) {
        workers.emplace_back([&, i]() {
            CURL *c = curl_easy_init();
            if (!c) return;
            char err[CURL_ERROR_SIZE] = {0};
            curl_easy_setopt(c, CURLOPT_URL, url.c_str());
            curl_easy_setopt(c, CURLOPT_CAINFO, ca_file.c_str());
            std::string body_capture;
            curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, AppendBody);
            curl_easy_setopt(c, CURLOPT_WRITEDATA, &body_capture);
            curl_easy_setopt(c, CURLOPT_ERRORBUFFER, err);
            // Range: 0-4095 keeps successful transfers small so
            // the accepted requests eventually complete and don't
            // stall the test if we misjudge timing. The scheduler
            // rejection fires before any body is served, so 429s
            // return immediately regardless of the requested range.
            curl_easy_setopt(c, CURLOPT_RANGE, "0-4095");
            // Timeout: even the accepted /test-public/slow_read.txt
            // transfers must not hold this thread indefinitely.
            // 15s is well over the ~10s a 4 KiB @ 500 KB/s would
            // take. Rejected requests return in ms.
            curl_easy_setopt(c, CURLOPT_TIMEOUT, 15L);

            CURLcode rc = curl_easy_perform(c);
            long status = 0;
            curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &status);
            results[i] = Result{status, static_cast<long>(rc),
                                rc == CURLE_OK ? "" : err,
                                std::move(body_capture)};

            curl_easy_cleanup(c);
        });
    }
    for (auto &t : workers) t.join();

    // Tally: how many succeeded (2xx), how many were shed (non-2xx),
    // how many curl-level errored.
    int ok = 0, shed = 0, curl_err = 0;
    for (const auto &r : results) {
        if (r.curl_code != 0) {
            curl_err++;
        } else if (r.http_status >= 200 && r.http_status < 300) {
            ok++;
        } else {
            shed++;
        }
    }
    // Log the histogram so a flake is easy to debug.
    std::vector<std::pair<long, int>> histogram;
    for (const auto &r : results) {
        bool merged = false;
        for (auto &h : histogram) {
            if (h.first == r.http_status) { h.second++; merged = true; break; }
        }
        if (!merged) histogram.emplace_back(r.http_status, 1);
    }
    std::string hist_str;
    for (const auto &h : histogram) {
        hist_str += "status=" + std::to_string(h.first) +
                    ":" + std::to_string(h.second) + " ";
    }
    GTEST_LOG_(INFO) << "burst results: ok=" << ok << " shed=" << shed
                     << " curl_err=" << curl_err
                     << " histogram={" << hist_str << "}";

    EXPECT_GT(shed, 0)
        << "the tight scheduler must produce at least one visible "
           "wire-level rejection across a " << kBurst
        << "-way concurrent burst; instead got " << ok
        << " successes and " << curl_err
        << " curl-level errors";

    // Additionally, at least one shed response body must carry the
    // "temporarily unavailable" marker (EAGAIN's strerror). That's
    // the signature specific to the scheduler shed path in the
    // current XrdHttp mapping, distinguishing it from unrelated 5xx
    // errors such as origin-side failures. Ideally this would be a
    // 429 with Retry-After, but XrdHttp maps errRetry through the
    // XrdCl→errno→HTTP chain and lands on 500 today.
    int shed_with_marker = 0;
    for (const auto &r : results) {
        if (r.curl_code == 0 && !(r.http_status >= 200 && r.http_status < 300) &&
            r.body.find("temporarily unavailable") != std::string::npos) {
            shed_with_marker++;
        }
    }
    EXPECT_GT(shed_with_marker, 0)
        << "at least one shed response body must contain the "
           "\"temporarily unavailable\" marker specific to the "
           "scheduler-shed path";
}
