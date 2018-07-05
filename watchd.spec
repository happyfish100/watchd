%define CommitVersion %(echo $COMMIT_VERSION)

Name: watchd
Version: 1.0.10
Release: 1%{?dist}
Summary: No Summary
License: GPL
Group: Arch/Tech
URL:  http://github.com/happyfish100/watchd/
Source: http://github.com/happyfish100/watchd/%{name}-%{version}.tar.gz

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n) 

BuildRequires: libfastcommon-devel >= 1.0.38
Requires: %__cp %__mv %__chmod %__grep %__mkdir %__install %__id
Requires: libfastcommon >= 1.0.38

%description
watch daemon
commit version: %{CommitVersion}

%prep
%setup -q

%build
./make.sh

%install
rm -rf %{buildroot}
DESTDIR=$RPM_BUILD_ROOT ./make.sh install

%post

%preun

%postun

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
/usr/bin/*

%changelog
* Mon Jun 23 2014
- first RPM release (1.0)
