%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}

Name:          python-synergy-scheduler-manager
Version:       0.2
Release:       2%{?dist}
Summary:       Advanced scheduling capability for OpenStack.
Source:        %name-%version.tar.bz2

License:       ASL 2.0

BuildArch:     noarch
BuildRequires: python-devel
BuildRequires: python-setuptools
Requires:      python-pbr
Requires:      python-synergy-service
Requires:      python-oslo-config
Requires:      python-oslo-messaging
Requires:      python-sqlalchemy


%description
The Scheduler Manager provides advanced scheduling (fairshare) capability for
OpenStack.  In particular it aims to address the resource utilization issues
coming from the static allocation model inherent in the Cloud paradigm, by
adopting the dynamic partitioning strategy implemented by the advanced batch
schedulers.


%prep
%setup -q


%build
%{__python} setup.py build


%install
rm -rf $RPM_BUILD_ROOT
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT


%files
%doc README.rst
%{python_sitelib}/*


%changelog
* Fri Jun 03 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 0.2-2
- Make unit test pass

