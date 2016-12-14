%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}

Name:          python-synergy-scheduler-manager
Version:       2.2.2
Release:       1%{?dist}
Summary:       Advanced scheduling capability for OpenStack.
Source:        %name-%version.tar.bz2

License:       ASL 2.0

BuildArch:     noarch
BuildRequires: python-devel
BuildRequires: python-setuptools
Requires:      python-nova >= 12.0
Requires:      python-oslo-config
Requires:      python-oslo-messaging
Requires:      python-oslo-serialization
Requires:      python-oslo-versionedobjects
Requires:      python-pbr
Requires:      python-requests
Requires:      python-sqlalchemy
Requires:      python-synergy-service >= 1.1


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
* Wed Dec 14 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 2.2.2-1
- Make SchedulerManager handle ERROR notifications

* Mon Dec 12 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 2.2.1-1
- fix: update required version of synergy-service

* Mon Dec 12 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 2.2.0-1
- [packaging] make docker aware of PKG_VERSION
- Synergy releases in advance the VMs that are going to be destroyed
- Queue.updatePriority() takes much time if the queue is large
- Add the new clock_skew parameter for KeystoneManager
- Invalid input for field/attribute quota_class_set
- KeystoneManager.authenticate() uses a wrong domain attribute
- Add a backfill_depth parameter
- Item readjustment fixed in PriorityQueue
- The FairshareManager should not use the Manager.condition var
- Fix scheduling when shared quota is disabled
- Update changelogs and system package versions

* Fri Dec 09 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 2.1.0-1
- CLI command "synergy usage show" enhanced
- Clean up oslo imports

* Wed Nov 09 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 2.0.0-1
- Synergy doesn't rely anymore on nova.conf
- Method deserialize() fixed
- The QuotaCommand shows a wrong value (%) for the field 'share'

* Wed Nov 09 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 1.2.0-1
- Scheduler managers enhanced
- Remove versions for required packages
- fix git and pbr when packaging with docker
- fix synergy-service version in spec file (rpm)
- fix required packages when packaging
- fix to get the synergy version when packaging

* Wed Nov 09 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 1.1.0-1
- use pbr fully for easier package building
- fix connection URL to RabbitMQ
- Added Queue class to synergy_scheduler_manager/common/queue.py
- Common objects and relative test units added
- Destroy() method fixed
- Fix requirement version pinning
- Use dependency pinning
- fix OpenStack CentOS repo for docker packaging
- Cleanup tox.ini: Remove obsolete constraints

* Mon Aug 22 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 1.0.2-1
- Add python-nova dependency

* Tue Jul 26 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr - 1.0.1-1
- Fix broken link in README

* Fri Jun 17 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 1.0.0-1
- First public release of Synergy, full set of functionalities.

* Fri Jun 03 2016 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 0.2-2
- Make unit test pass

