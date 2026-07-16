/***************************************************************
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

#include "XrdClCurlFactory.hh"
#include "XrdClCurlFile.hh"
#include "XrdClCurlFilesystem.hh"
#include "XrdClCurlUtil.hh"
#include "XrdClCurlOps.hh"
#include "XrdClCurlParseTimeout.hh"
#include "XrdClCurlTagScheduler.hh"
#include "XrdClCurlWorker.hh"

#include "XrdCl/XrdClConstants.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdXrootd/XrdXrootdGStream.hh"
#include "XrdVersion.hh"

#include <stdio.h>
#include <unistd.h>

XrdVERSIONINFO(XrdClGetPlugIn, XrdClGetPlugIn)

using namespace XrdClCurl;

struct timespec
Factory::GetHeaderTimeoutWithDefault(time_t oper_timeout)
{
    if (oper_timeout == 0) {
        int val = XrdCl::DefaultRequestTimeout;
        XrdCl::DefaultEnv::GetEnv()->GetInt( "RequestTimeout", val );
        oper_timeout = val;
    }
    if (oper_timeout <= 0) {
        return {0, 0};
    }
    return {oper_timeout, 0};
}

bool Factory::m_initialized = false;
std::shared_ptr<XrdClCurl::HandlerQueue> Factory::m_queue;
std::unique_ptr<XrdClCurl::TagScheduler> Factory::m_scheduler;
XrdCl::Log *Factory::m_log = nullptr;
std::once_flag Factory::m_init_once;
std::string Factory::m_stats_location;
std::chrono::system_clock::time_point Factory::m_start{};

std::mutex Factory::m_shutdown_lock;
std::thread Factory::m_monitor_tid;
std::condition_variable Factory::m_shutdown_requested_cv;
bool Factory::m_shutdown_requested = false;

// shutdown trigger, must be last of the static members
Factory::shutdown_s Factory::m_shutdowns;

void
Factory::Initialize()
{
    std::unique_lock lock(m_shutdown_lock);
    if (m_shutdown_requested) {
      return;
    }
    std::call_once(m_init_once, [&] {
        m_log = XrdCl::DefaultEnv::GetLog();
        if (!m_log) {
            return;
        }
        m_log->SetTopicName(kLogXrdClCurl, "XrdClCurl");

        auto env = XrdCl::DefaultEnv::GetEnv();
        if (!env) {
            return;
        }

        SetupX509();

        // The location for the client to write the statistics file; this will be dropped
        // atomically every ~5 seconds and is meant to be complementary to the future g-stream.
        env->PutString("CurlStatisticsLocation", "");
        env->ImportString("CurlStatisticsLocation", "XRD_CURLSTATISTICSLOCATION");
        if (env->GetString("CurlStatisticsLocation", m_stats_location)) {
            m_log->Debug(kLogXrdClCurl, "Will write client statistics to %s", m_stats_location.c_str());
        } else {
            m_log->Debug(kLogXrdClCurl, "Not writing client statistics to disk");
        }
        m_start = std::chrono::system_clock::now();

        // The minimum value we will accept from the request for a header timeout.
        // (i.e., the amount of time the plugin will wait to receive headers from the remote server)
        env->PutString("CurlMinimumHeaderTimeout", "");
        env->ImportString("CurlMinimumHeaderTimeout", "XRD_CURLMINIMUMHEADERTIMEOUT");

        // The default value of the header timeout (the amount of time the plugin will wait)
        // to receive headers from the remote server.
        env->PutString("CurlDefaultHeaderTimeout", "");
        env->ImportString("CurlDefaultHeaderTimeout", "XRD_CURLDEFAULTHEADERTIMEOUT");

        // The number of pending operations allowed in the global work queue.
        env->PutInt("CurlMaxPendingOps", XrdClCurl::HandlerQueue::GetDefaultMaxPendingOps());
        env->ImportInt("CurlMaxPendingOps", "XRD_CURLMAXPENDINGOPS");
        int max_pending = XrdClCurl::HandlerQueue::GetDefaultMaxPendingOps();
        if (env->GetInt("CurlMaxPendingOps", max_pending)) {
            if (max_pending <= 0 || max_pending > 10'000'000) {
                m_log->Error(kLogXrdClCurl,
                    "Invalid value for the maximum number of pending operations in the global work queue (%d); using default value of %d",
                    max_pending,
                    XrdClCurl::HandlerQueue::GetDefaultMaxPendingOps());
                max_pending = XrdClCurl::HandlerQueue::GetDefaultMaxPendingOps();
                env->PutInt("CurlMaxPendingOps", max_pending);
            }
            m_log->Debug(kLogXrdClCurl, "Using %d pending operations in the global work queue", max_pending);
        }
        m_queue.reset(new XrdClCurl::HandlerQueue(max_pending));

        // The number of threads to use for curl operations.
        env->PutInt("CurlNumThreads", m_poll_threads);
        env->ImportInt("CurlNumThreads", "XRD_CURLNUMTHREADS");
        int num_threads = m_poll_threads;
        if (env->GetInt("CurlNumThreads", num_threads)) {
            if (num_threads <= 0 || num_threads > 1'000) {
                m_log->Error(kLogXrdClCurl, "Invalid value for the number of threads to use for curl operations (%d); using default value of %d", num_threads, m_poll_threads);
                num_threads = m_poll_threads;
                env->PutInt("CurlNumThreads", num_threads);
            }
            m_log->Debug(kLogXrdClCurl, "Using %d threads for curl operations", num_threads);
        }

        // The maximum number of in-flight curl operations per worker
        // thread.  We read this BEFORE the fair-scheduler configuration
        // because the scheduler sizes its per-tag caps as percentages of
        // (num_threads * max_ops_per_worker); the compile-time default
        // is 50 which is far larger than most test setups want.
        env->PutInt("CurlMaxOpsPerWorker", XrdClCurl::CurlWorker::m_default_max_ops);
        env->ImportInt("CurlMaxOpsPerWorker", "XRD_CURLMAXOPSPERWORKER");
        int max_ops_per_worker = XrdClCurl::CurlWorker::m_default_max_ops;
        if (env->GetInt("CurlMaxOpsPerWorker", max_ops_per_worker)) {
            if (max_ops_per_worker <= 0 || max_ops_per_worker > 10'000) {
                m_log->Error(kLogXrdClCurl, "Invalid value for the maximum number of in-flight ops per worker (%d); using default value of %d", max_ops_per_worker, XrdClCurl::CurlWorker::m_default_max_ops);
                max_ops_per_worker = XrdClCurl::CurlWorker::m_default_max_ops;
                env->PutInt("CurlMaxOpsPerWorker", max_ops_per_worker);
            }
            m_log->Debug(kLogXrdClCurl, "Using %d in-flight ops per worker", max_ops_per_worker);
        }
        XrdClCurl::CurlWorker::SetMaxOps(max_ops_per_worker);

        // Per-tag fair-scheduler configuration.  Setting
        // CurlPendingBuffer to 0 disables the scheduler entirely
        // (the queue reverts to its historical FIFO behaviour).
        env->PutInt("CurlPendingBuffer", 200);
        env->ImportInt("CurlPendingBuffer", "XRD_CURLPENDINGBUFFER");
        env->PutInt("CurlPendingPerOrigin", 50);
        env->ImportInt("CurlPendingPerOrigin", "XRD_CURLPENDINGPERORIGIN");
        env->PutInt("CurlStarvingPercent", 25);
        env->ImportInt("CurlStarvingPercent", "XRD_CURLSTARVINGPERCENT");
        env->PutInt("CurlActivePercent", 90);
        env->ImportInt("CurlActivePercent", "XRD_CURLACTIVEPERCENT");
        env->PutInt("CurlEmaSeconds", 30);
        env->ImportInt("CurlEmaSeconds", "XRD_CURLEMASECONDS");
        int sched_buffer = 200, sched_per_origin = 50, sched_starving = 25,
            sched_active = 90, sched_ema = 30;
        env->GetInt("CurlPendingBuffer",     sched_buffer);
        env->GetInt("CurlPendingPerOrigin",  sched_per_origin);
        env->GetInt("CurlStarvingPercent",   sched_starving);
        env->GetInt("CurlActivePercent",     sched_active);
        env->GetInt("CurlEmaSeconds",        sched_ema);
        if (sched_buffer > 0) {
            XrdClCurl::TagScheduler::Config sched_cfg;
            sched_cfg.per_tag_starving_percent = sched_starving;
            sched_cfg.per_tag_active_percent   = sched_active;
            sched_cfg.pending_buffer_size      = sched_buffer;
            sched_cfg.per_tag_pending_size     = sched_per_origin;
            sched_cfg.ema_window               = std::chrono::seconds(sched_ema);
            unsigned pool_size = static_cast<unsigned>(num_threads)
                * static_cast<unsigned>(max_ops_per_worker);
            m_scheduler.reset(new XrdClCurl::TagScheduler(pool_size, sched_cfg, m_log));
            m_queue->SetScheduler(m_scheduler.get());
            m_log->Debug(kLogXrdClCurl,
                "TagScheduler enabled: pool=%u, starving=%d%%, active=%d%%, "
                "pending_buffer=%d, per_origin=%d, ema=%ds",
                pool_size, sched_starving, sched_active,
                sched_buffer, sched_per_origin, sched_ema);
        } else {
            m_log->Debug(kLogXrdClCurl,
                "CurlPendingBuffer==0; TagScheduler disabled, using historical FIFO");
        }

        // The stall timeout to use for transfer operations.
        env->PutInt("CurlStallTimeout", XrdClCurl::CurlOperation::GetDefaultStallTimeout());
        env->ImportInt("CurlStallTimeout", "XRD_CURLSTALLTIMEOUT");
        int stall_timeout = XrdClCurl::CurlOperation::GetDefaultStallTimeout();
        if (env->GetInt("CurlStallTimeout", stall_timeout)) {
            if (stall_timeout < 0 || stall_timeout > 86'400) {
                m_log->Error(kLogXrdClCurl, "Invalid value for the stall timeout (%d); using default value of %d", stall_timeout, XrdClCurl::CurlOperation::GetDefaultStallTimeout());
                stall_timeout = XrdClCurl::CurlOperation::GetDefaultStallTimeout();
                env->PutInt("CurlStallTimeout", stall_timeout);
            }
            m_log->Debug(kLogXrdClCurl, "Using %d seconds for the stall timeout", stall_timeout);
        }
        XrdClCurl::CurlOperation::SetStallTimeout(stall_timeout);

        // The slow transfer rate, in bytes per second, for timing out slow uploads/downloads.
        env->PutInt("CurlSlowRateBytesSec", XrdClCurl::CurlOperation::GetDefaultSlowRateBytesSec());
        env->ImportInt("CurlSlowRateBytesSec", "XRD_CURLSLOWRATEBYTESSEC");
        int slow_xfer_rate = XrdClCurl::CurlOperation::GetDefaultSlowRateBytesSec();
        if (env->GetInt("CurlSlowRateBytesSec", slow_xfer_rate)) {
            if (slow_xfer_rate < 0 || slow_xfer_rate > (1024 * 1024 * 1024)) {
                m_log->Error(kLogXrdClCurl, "Invalid value for the slow transfer rate threshold (%d); using default value of %d", stall_timeout, XrdClCurl::CurlOperation::GetDefaultSlowRateBytesSec());
                slow_xfer_rate = XrdClCurl::CurlOperation::GetDefaultSlowRateBytesSec();
                env->PutInt("CurlSlowRateBytesSec", slow_xfer_rate);
            }
            m_log->Debug(kLogXrdClCurl, "Using %d bytes/sec for the slow transfer rate threshold", slow_xfer_rate);
        }
        XrdClCurl::CurlOperation::SetSlowRateBytesSec(slow_xfer_rate);

        // Determine the minimum header timeout.  It's somewhat arbitrarily defaulted to 2s; below
        // that and timeouts could be caused by OS scheduling noise.  If the client has unreasonable
        // expectations of the origin, we don't want to cause it to generate lots of origin-side load.
        std::string val;
        struct timespec mct{2, 0};
        if (env->GetString("CurlMinimumHeaderTimeout", val) && !val.empty()) {
            std::string errmsg;
            if (!ParseTimeout(val, mct, errmsg)) {
                m_log->Error(kLogXrdClCurl, "Failed to parse the minimum client timeout (%s): %s", val.c_str(), errmsg.c_str());
            }
        }
        XrdClCurl::File::SetMinimumHeaderTimeout(mct);

        struct timespec dht{9, 500'000'000};
        if (env->GetString("CurlDefaultHeaderTimeout", val) && !val.empty()) {
            std::string errmsg;
            if (!ParseTimeout(val, dht, errmsg)) {
                m_log->Error(kLogXrdClCurl, "Failed to parse the default header timeout (%s): %s", val.c_str(), errmsg.c_str());
            }
        }
        XrdClCurl::File::SetDefaultHeaderTimeout(dht);

        // Start up the cache for the OPTIONS response
        auto &cache = XrdClCurl::VerbsCache::Instance();

        // Startup curl workers after we've set the configs to avoid race conditions
        for (int idx=0; idx<num_threads; idx++) {
            auto wk = std::make_unique<XrdClCurl::CurlWorker>(m_queue, cache, m_log);
            auto wkp = wk.get();
            std::thread t(XrdClCurl::CurlWorker::RunStatic, wkp);
            wkp->Start(std::move(wk), std::move(t));
        }

        std::thread t([this]{Monitor();});
        m_monitor_tid = std::move(t);

        m_initialized = true;
    });
}

void
Factory::Monitor()
{
    // This function is run in a separate thread to monitor the XrdClCurl statistics.
    // It periodically logs the statistics to the log file (and to the g-stream monitoring
    // if available).

    XrdXrootdGStream *gstream = nullptr;
#if XrdMajorVNUM(x) > 5
    auto env = XrdCl::DefaultEnv::GetEnv();
    void *gstream_void = nullptr;
    env->GetPtr("pfc.gStream*", gstream_void);
    gstream = gstream_void;
#endif

    while (true) {
        {
            std::unique_lock lock(m_shutdown_lock);
            m_shutdown_requested_cv.wait_for(
                lock,
                std::chrono::seconds(5),
                []{return m_shutdown_requested;}
            );
            if (m_shutdown_requested) {
                break;
            }
        }

        auto now = std::chrono::system_clock::now();

        // Advance the TagScheduler's EMA snapshot on the same cadence
        // we publish monitoring, so no separate ticker thread is
        // needed for it.
        if (m_scheduler) {
            m_scheduler->OnMonitorTick();
        }

        std::string monitoring = "{\"event\": \"xrdclcurl\", "
            "\"start\": " + std::to_string(std::chrono::duration<double>(m_start.time_since_epoch()).count()) + ","
            "\"now\": " + std::to_string(std::chrono::duration<double>(now.time_since_epoch()).count()) + ","
            "\"file\": " + File::GetMonitoringJson() + ","
            "\"workers\": " + CurlWorker::GetMonitoringJson() + ","
            "\"queues\": " + HandlerQueue::GetMonitoringJson() +
            (m_scheduler
                ? (",\"scheduler\": " + m_scheduler->GetMonitoringJson())
                : std::string{}) +
            " }";
        m_log->Info(kLogXrdClCurl, "Client monitoring statistics: %s", monitoring.c_str());
        if (gstream) {
            gstream->Insert(monitoring.data(), monitoring.size() + 1);
        }
        if (!m_stats_location.empty())
        {
            auto stats_tmp = m_stats_location + ".XXXXXX";
            std::vector<char> stats_vector(stats_tmp.size() + 1, '\0');
            memcpy(&stats_vector[0], stats_tmp.data(), stats_tmp.size() + 1);
            auto fd = mkstemp(&stats_vector[0]);
            if (fd == -1) {
                m_log->Warning(kLogXrdClCurl, "Failed to create temporary stats file %s: %s", m_stats_location.c_str(), strerror(errno));
                continue;
            }
            auto nb = write(fd, monitoring.data(), monitoring.size());
            if (nb != static_cast<ssize_t>(monitoring.size())) {
                if (nb == -1) m_log->Warning(kLogXrdClCurl, "Failed to write statistics into temporary file %s: %s", &stats_vector[0], strerror(errno));
                else m_log->Warning(kLogXrdClCurl, "Failed to write statistics into temporary file %s: short write", &stats_vector[0]);
                close(fd);
                continue;
            }
            close(fd);
            auto rv = rename(&stats_vector[0], m_stats_location.c_str());
            if (rv) {
                m_log->Warning(kLogXrdClCurl, "Failed to atomically rename stats file to final destination %s: %s", m_stats_location.c_str(), strerror(errno));
            }
        }
    }
}

namespace {

void SetIfEmpty(XrdCl::Env *env, XrdCl::Log &log, const std::string &optName, const std::string &envName) {
    if (!env) return;

    std::string val;
    if (!env->GetString(optName, val) || val.empty()) {
        env->PutString(optName, "");
        env->ImportString(optName, envName);
    }
    if (env->GetString(optName, val) && !val.empty()) {
        log.Info(kLogXrdClCurl, "Setting %s to value '%s'", optName.c_str(), val.c_str());
    }
}

} // namespace

void
Factory::SetupX509() {

    auto env = XrdCl::DefaultEnv::GetEnv();
    SetIfEmpty(env, *m_log, "CurlCertFile", "XRD_CURLCERTFILE");
    SetIfEmpty(env, *m_log, "CurlCertDir", "XRD_CURLCERTDIR");
    SetIfEmpty(env, *m_log, "CurlClientCertFile", "XRD_CURLCLIENTCERTFILE");
    SetIfEmpty(env, *m_log, "CurlClientKeyFile", "XRD_CURLCLIENTKEYFILE");

    int disable_proxy = 0;
    env->PutInt("CurlDisableX509", 0);
    env->ImportInt("CurlDisableX509", "XRD_CURLDISABLEX509");

    std::string filename;
    char *filename_char;
    if (!disable_proxy && (!env->GetString("CurlClientCertFile", filename) || filename.empty())) {
        if ((filename_char = getenv("X509_USER_PROXY"))) {
            filename = filename_char;
        }
        if (filename.empty()) {
            filename = "/tmp/x509up_u" + std::to_string(geteuid());
        }
        if (access(filename.c_str(), R_OK) == 0) {
            m_log->Debug(kLogXrdClCurl, "Using X509 proxy file found at %s for TLS client credential", filename.c_str());
            env->PutString("CurlClientCertFile", filename);
            env->PutString("CurlClientKeyFile", filename);
        }
    }
    if ((!env->GetString("CurlCertDir", filename) || filename.empty()) && (filename_char = getenv("X509_CERT_DIR"))) {
        env->PutString("CurlCertDir", filename_char);
    }
}

void
Factory::Shutdown()
{
    {
        std::unique_lock lock(m_shutdown_lock);
        m_shutdown_requested = true;
        m_shutdown_requested_cv.notify_one();
    }
    // Ask the scheduler to wake any parked Consume() so workers can
    // exit. Detach from the HandlerQueue first so no new Consume()
    // call can re-park inside the scheduler after we've released it.
    if (m_scheduler && m_queue) {
        m_queue->SetScheduler(nullptr);
        m_scheduler->Shutdown();
    }
    if (m_monitor_tid.joinable()) {
      m_monitor_tid.join();
    }
}

void
Factory::Produce(std::unique_ptr<XrdClCurl::CurlOperation> operation)
{
    m_queue->Produce(std::move(operation));
}

XrdCl::FilePlugIn *
Factory::CreateFile(const std::string & /*url*/) {
    Initialize();
    if (!m_initialized) {return nullptr;}
    return new File(m_queue, m_log);
}

XrdCl::FileSystemPlugIn *
Factory::CreateFileSystem(const std::string & url) {
    Initialize();
    if (!m_initialized) {return nullptr;}
    return new Filesystem(url, m_queue, m_log);
}

extern "C"
{
    void *XrdClGetPlugIn(const void*)
    {
        return static_cast<void*>(new Factory());
    }
}
