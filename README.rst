------------------------------
 SYNERGY SCHEDULER MANAGER
------------------------------

The Scheduler Manager

Synergy is as a new extensible general purpose management OpenStack service.
Its capabilities are implemented by a collection of managers which are specific
and independent pluggable tasks, executed periodically or interactively. The
managers can interact with each other in a loosely coupled way.
The Scheduler Manager provides advanced scheduling (fairshare) capability for
OpenStack.  In particular it aims to address the resource utilization issues
coming from the static allocation model inherent in the Cloud paradigm, by
adopting the dynamic partitioning strategy implemented by the advanced batch
schedulers.


* Free software: Apache license
* Documentation: https://indigo-dc.gitbooks.io/synergy-doc/content/
* Source: http://git.openstack.org/cgit/openstack/synergy-scheduler-manager
* Bugs: http://bugs.launchpad.net/synergy-scheduler-manager
