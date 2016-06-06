FROM centos:7
MAINTAINER Vincent Llorens <vincent.llorens@cc.in2p3.fr>
RUN yum install -y https://repos.fedorapeople.org/repos/openstack/openstack-liberty/rdo-release-liberty-3.noarch.rpm \
                   rpm-build \
                   python-devel \
                   python-setuptools \
    && yum update -y
RUN mkdir /tmp/python-synergy-scheduler-manager
RUN useradd -m -p pkger pkger
USER pkger
COPY build.sh /home/pkger/
WORKDIR /home/pkger/
CMD bash build.sh
