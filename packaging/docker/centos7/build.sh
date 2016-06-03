#!/usr/bin/env bash
set -e -x

RPMBUILD=/home/pkger/rpmbuild
PKG_DIR=/tmp/python-synergy-scheduler-manager


function get_version() {
    local file=$PKG_DIR/setup.cfg
    export PKG_VERSION=$(grep -e "version = " $file | sed -r "s/version = ()/\1/")
}

function setup() {
    mkdir -p /home/pkger/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
    cd $RPMBUILD/SOURCES/
    cp -r $PKG_DIR python-synergy-scheduler-manager-$PKG_VERSION
    rm -r python-synergy-scheduler-manager-$PKG_VERSION/{.tox,.testrepository,build,dist} || true
    tar cjf python-synergy-scheduler-manager-${PKG_VERSION}.tar.bz2 python-synergy-scheduler-manager-$PKG_VERSION
    cp $PKG_DIR/packaging/rpm/python-synergy-scheduler-manager.spec $RPMBUILD/SPECS/python-synergy-scheduler-manager.spec
}

function build() {
    cd $RPMBUILD/SPECS
    export PBR_VERSION=$PKG_VERSION
    rpmbuild -ba python-synergy-scheduler-manager.spec
    mkdir -p $PKG_DIR/build/
    cp -i $RPMBUILD/RPMS/noarch/python-synergy-scheduler-manager-*.rpm $PKG_DIR/build/
}

function clean() {
    rm -rf $RPMBUILD
}

clean || true
get_version
setup
build
