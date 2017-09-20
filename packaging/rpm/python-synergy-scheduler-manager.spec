%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}

Name:          python-synergy-scheduler-manager
Version:       2.6.0
Release:       1%{?dist}
Summary:       Advanced scheduling capability for OpenStack.
Source:        %name-%version.tar.bz2

License:       ASL 2.0

BuildArch:     noarch
BuildRequires: python-devel
BuildRequires: python-setuptools
Requires:      python-nova >= 14.0
Requires:      python2-oslo-config
Requires:      python2-oslo-messaging
Requires:      python2-oslo-serialization
Requires:      python2-oslo-versionedobjects
Requires:      python2-pbr
Requires:      python2-requests
Requires:      python-sqlalchemy
Requires:      python-synergy-service >= 1.5


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
install -D -m0644 config/synergy_scheduler.conf       %{buildroot}%{_sysconfdir}/synergy/synergy_scheduler.conf
install -D -m0644 config/policy.json                  %{buildroot}%{_sysconfdir}/synergy/policy.json


%files
%doc README.rst
%{python_sitelib}/*
%{_sysconfdir}/synergy
%dir %attr(0755, synergy, synergy) %{_sysconfdir}/synergy/
%config(noreplace) %{_sysconfdir}/synergy/synergy_scheduler.conf
%config(noreplace) %{_sysconfdir}/synergy/policy.json
%attr(0644, synergy, synergy) %{_sysconfdir}/synergy/policy.json


%changelog
* Wed Sep 20 2017 Ervin Konomi <ervin.konomi@pd.infn.it> - 2.6.0-1
- Restored setQuotaTypeServer()
- Restore() incorrectly increases the queue size
- Synergy should scale up the oldest user requests from the queue
- Mechanism that performs user actions before Synergy deletes the VMs
- Configuration parameters updated
- Termination of expired servers

* Mon Aug 21 2017 Vincent Llorens <vincent.llorens@cc.in2p3.fr> - 2.5.1-1
- Update some python requirements to python2-* names
- fix small typo preventing scheduler initialization

* Fri Aug 11 2017 Ervin Konomi <ervin.konomi@pd.infn.it> - 2.5.0-1
- Added support for policy.json in packaging process
- Project manager problems concerning the adding and removing projects
- Authorization policies changed
- Added queue usage to project
- Missing security support
- Synergy should never raise Exception
- Added support for OpenStack Ocata to NovaManager
- Partition Director requires configuring Synergy through RESTful API
- Added support for notifications to KeystoneManager
- MessagingAPI should be implemented as common module
- The SchedulerManager doesn't receive all the compute.instance.delete.end notifications

* Mon Mar 20 2017 Ervin Konomi <ervin.konomi@pd.infn.it> - 2.4.0-1
- Add new "synergy_topic" parameter
- Fix a possible shared quota consistency issue
- Fix the bug concerning the user which is still showed after deletion
- simplify packaging with docker
- Fix the share percentage
- Remove unused logging import
- Fix the error: Arguments already parsed
- Fix the TypeError: not all arguments converted during string formatting
- Added configuration file

* Mon Jan 30 2017 Ervin Konomi <ervin.konomi@pd.infn.it> - 2.3.0-1
- Update of the link to the Synergy documentation
- Fix a bug related to NUMA topology
- Removed logging message
- Time data does not match format '%Y-%m-%dT%H:%M:%S.%fZ'
- Retry mechanism improved and fixed
- Add support for Keystone domains
- Heat and Synergy interfere on the management of the users trust
- Synergy and Ceilometer compete for consuming AMQP notifications
- Add support for the automatic recycle of DB connections
- Enable SSL for OpenStack Trust
- Add support for AMQP HA to NovaManager
- Use API v3 for KeystoneManager.getUsers()
- QuotaManager: private quota shrinking must not be always allowed
- NovaManager and KeystoneManager are not SSL-enabled

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

