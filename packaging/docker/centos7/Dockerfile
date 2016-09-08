FROM centos:7
MAINTAINER Vincent Llorens <vincent.llorens@cc.in2p3.fr>
RUN yum install -y centos-release-openstack-liberty \
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
