
Name: xrdcl-pelican
Version: 1.0.1
Release: 1%{?dist}
Summary: A Pelican-specific backend for the XRootD client

Group: System Environment/Daemons
License: BSD
URL: https://github.com/pelicanplatform/xrdcl-pelican
# Generated from:
# git archive v%%{version} --prefix=xrdcl-pelican-%%{version}/ | gzip -7 > ~/rpmbuild/SOURCES/xrdcl-pelican-%%{version}.tar.gz
Source0: %{name}-%{version}.tar.gz

%define xrootd_current_major 5
%define xrootd_current_minor 6
%define xrootd_next_major 6

%if 0%{?rhel} > 8
%global __cmake_in_source_build 1
%endif

BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildRequires: xrootd-devel >= 1:%{xrootd_current_major}
BuildRequires: xrootd-devel <  1:%{xrootd_next_major}
BuildRequires: xrootd-client-devel >= 1:%{xrootd_current_major}
BuildRequires: xrootd-client-devel <  1:%{xrootd_next_major}
BuildRequires: xrootd-server-devel >= 1:%{xrootd_current_major}
BuildRequires: xrootd-server-devel <  1:%{xrootd_next_major}
%if 0%{?rhel} > 8
BuildRequires: gcc-c++
BuildRequires: cmake
%else
BuildRequires: cmake3
%endif
%if 0%{?rhel} == 7
BuildRequires: devtoolset-11-toolchain
%endif
BuildRequires: curl-devel
%{?systemd_requires}
# For %%{_unitdir} macro
BuildRequires: systemd
BuildRequires: openssl-devel
BuildRequires: tinyxml2-devel
# nlohmann-json-devel is available from the OSG repos
BuildRequires: nlohmann-json-devel

Requires: xrootd-client >= 1:%{xrootd_current_major}.%{xrootd_current_minor}
Requires: xrootd-client <  1:%{xrootd_next_major}.0.0-1

%description
%{summary}

%prep
%setup -q

%build
%if 0%{?rhel} == 7
. /opt/rh/devtoolset-11/enable
%endif

%cmake3 -DCMAKE_BUILD_TYPE=RelWithDebInfo -DXROOTD_EXTERNAL_TINYXML2=1 -DXROOTD_EXTERNAL_JSON=1 .
make VERBOSE=1 %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_libdir}/libXrdClPelican-*.so
%{_sysconfdir}/xrootd/client.plugins.d/pelican-plugin.conf
%{_sysconfdir}/xrootd/client.plugins.d/pelican-plugin-http.conf

%changelog
* Fri Jan 3 2025 Brian Bockelman <bbockelman@morgridge.org> - 1.0.1-1
- Fix build issues on RHEL9

* Thu Jan 2 2025 Brian Bockelman <bbockelman@morgridge.org> - 1.0.0-1
- Switch to using PROPFIND for stat, preventing opening a directory as a file
- Implement directory listings at the cache
- Cache the results of the director response, skipping director lookup when not needed
- Forward pelican.timeout header to the remote origin
- Fix a bug that invoked a callback twice, potentially segfaulting the process
- Add unit tests to the project

* Tue Sep 17 2024 Justin Hiemstra <jhiemstra@wisc.edu> - 0.9.4-1
- Provide error codes on  metadata lookup failure
- Allow the plugin to use X.509 authentication

* Thu Feb 8 2024 Mátyás Selmeci <matyas@cs.wisc.edu> - 0.9.3-2
- Add /etc/xrootd/client.plugins.d/pelican-plugin-http.conf

* Wed Feb 7 2024 Brian Bockelman <bbockelman@morgridge.org> - 0.9.3-1
- Add support for requesting reversed connections from the Pelican connection broker.
- Plugin no longer drops query parameters when `pelican://` is used; fixes
  issues with missing authorization from URL.

* Wed Jan 24 2024 Brian Bockelman <bbockelman@morgridge.org> - 0.9.2-1
- Add support for the `pelican://` protocol, allowing XCache to consume
  the federation metadata directly.

* Fri Jan 19 2024 Mátyás Selmeci <matyas@cs.wisc.edu> - 0.9.1-2
- Fix packaging to build on RHEL8 and RHEL9 as well

* Wed Dec 20 2023 Brian Bockelman <brian.bockleman@cern.ch> - 0.9.1-1
- Fix some undefined behavior on RHEL7 that could lead to a deadlock

* Sun Dec 10 2023 Brian Bockelman <brian.bockelman@cern.ch> - 0.9.0-1
- Initial packaging of the Pelican client

