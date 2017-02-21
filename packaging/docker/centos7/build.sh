#!/usr/bin/env bash
set -e -x

RPMBUILD=/home/pkger/rpmbuild
PKG_DIR=/tmp/synergy


function get_version() {
    if [[ -z $PKG_VERSION ]]; then
        cd $PKG_DIR
        export PKG_VERSION=$(git tag -l "*.*.*" | sort -V | tail -1)
    fi
}

function setup() {
    mkdir -p /home/pkger/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
    cd $RPMBUILD/SOURCES/
    cp -r $PKG_DIR python-synergy-scheduler-manager-$PKG_VERSION
    rm -r python-synergy-scheduler-manager-$PKG_VERSION/{.eggs,.tox,.testrepository,build,dist} || true
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
