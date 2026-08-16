/***************************************************************
 *
 * Copyright (C) 2026, Pelican Project, Morgridge Institute for Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***************************************************************/

// Regression tests for a stat that reports the size of the PROPFIND response
// instead of the size of the object.
//
// Observed against an OSDF cache: an object of 66,939,740 bytes was served by
// the cache as exactly 755 bytes -- the byte length of that object's own
// PROPFIND multistatus document -- and the 755 bytes returned were the real
// leading bytes of the object, so the data path was fine and only the size
// was wrong. The cache had recorded the object's size from the Content-Length
// of a 207 response.
//
// How a stat ends up reading a 207 while believing it issued a HEAD:
// CURLOPT_CUSTOMREQUEST persists on a libcurl easy handle. CurlStatOp sets it
// to PROPFIND when the verbs cache says the target supports it, and the
// fallback branches (a redirect to a target that does not, or m_force_head)
// used to clear m_is_propfind and set CURLOPT_NOBODY without clearing
// CUSTOMREQUEST -- so the request still went out as a PROPFIND. The handle was
// then released without clearing it either, because ReleaseHandle() only did
// so when m_is_propfind was true, leaving the next operation to reuse a handle
// that issues PROPFIND for everything.

#include "XrdClCurl/XrdClCurlOps.hh"

#include <XrdCl/XrdClDefaultEnv.hh>
#include <XrdCl/XrdClLog.hh>

#include <gtest/gtest.h>

#include <string>

namespace {

// Drives CurlStatOp's response handling directly. The interesting state
// (m_headers, m_is_propfind, m_response) is protected, so the subclass exposes
// just enough to stage a reply and read back what the operation concluded.
class TestStatOp final : public XrdClCurl::CurlStatOp {
public:
    TestStatOp(const std::string &url, XrdCl::Log *log)
        : XrdClCurl::CurlStatOp(nullptr, url, timespec{30, 0}, log, false, nullptr, nullptr)
    {
        m_curl.reset(curl_easy_init());
    }

    // Stage the reply a server sent, the way libcurl's header callback would.
    void InjectResponse(int status, int64_t contentLength, const std::string &body = "") {
        m_headers = XrdClCurl::HeaderParser();
        m_headers.Parse("HTTP/1.1 " + std::to_string(status) + " Reply\r\n");
        m_headers.Parse("Content-Length: " + std::to_string(contentLength) + "\r\n");
        m_headers.Parse("\r\n");
        m_response = body;
    }

    void SetPropfind(bool value) {m_is_propfind = value;}
    bool IsPropfind() const {return m_is_propfind;}
};

XrdCl::Log *GetLog() {return XrdCl::DefaultEnv::GetLog();}

// A multistatus document for a large object. Its own length is nothing like
// the object's size, which is the whole point.
const char *kMultistatus =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<D:multistatus xmlns:D=\"DAV:\"><D:response>"
    "<D:href>/ns/big.bin</D:href><D:propstat><D:prop>"
    "<D:resourcetype></D:resourcetype>"
    "<D:getcontentlength>66939740</D:getcontentlength>"
    "</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>"
    "</D:response></D:multistatus>";

} // namespace

// The production symptom: a 207 answered a leg that thought it was a HEAD.
// The object's size must come from the multistatus body, never from the
// Content-Length describing that body.
TEST(StatVerb, MultistatusReplyToAHeadDoesNotYieldTheDocumentSize) {
    TestStatOp op("https://cache.example:8443/ns/big.bin", GetLog());

    const int64_t documentLength = static_cast<int64_t>(strlen(kMultistatus));
    op.SetPropfind(false);                                  // this leg believes it sent a HEAD
    op.InjectResponse(207, documentLength, kMultistatus);    // ... but a PROPFIND was answered

    auto [length, isDir] = op.GetStatInfo();
    EXPECT_FALSE(isDir);
    EXPECT_NE(length, documentLength)
        << "stat reported the multistatus document's length as the object size";
    EXPECT_EQ(length, 66939740)
        << "stat must read the object size out of the multistatus body";
}

// The ordinary HEAD path is unchanged: a 200 reply's Content-Length is the
// object's size and must still be used as-is.
TEST(StatVerb, HeadReplyStillUsesContentLength) {
    TestStatOp op("https://origin.example:8443/ns/big.bin", GetLog());

    op.SetPropfind(false);
    op.InjectResponse(200, 66939740);

    auto [length, isDir] = op.GetStatInfo();
    EXPECT_FALSE(isDir);
    EXPECT_EQ(length, 66939740);
}

// A genuine PROPFIND leg keeps parsing its body, as it always did.
TEST(StatVerb, PropfindReplyParsesTheBody) {
    TestStatOp op("https://origin.example:8443/ns/big.bin", GetLog());

    op.SetPropfind(true);
    op.InjectResponse(207, static_cast<int64_t>(strlen(kMultistatus)), kMultistatus);

    auto [length, isDir] = op.GetStatInfo();
    EXPECT_FALSE(isDir);
    EXPECT_EQ(length, 66939740);
}
