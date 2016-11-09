#!/usr/bin/env bash
set -e -x

PKG_DIR=/tmp/python-synergy-scheduler-manager


function copy_source() {
    cd /home/pkger
    cp -r $PKG_DIR python-synergy-scheduler-manager
    rm -r python-synergy-scheduler-manager/{.eggs,.tox,.testrepository,build,dist} || true
}

function get_version() {
    # The version is taken from the tag on the current commit.
    # There *must be* a tag on the current commit to continue building the
    # package.
    cd $PKG_DIR
    export PKG_VERSION=$(git tag --points-at HEAD)
}

function setup() {
    cd /home/pkger
    tar cjf python-synergy-scheduler-manager_${PKG_VERSION}.orig.tar.bz2 python-synergy-scheduler-manager
    mv python-synergy-scheduler-manager/packaging/debian python-synergy-scheduler-manager/debian
}

function build() {
    cd /home/pkger/python-synergy-scheduler-manager
    debuild -us -uc
    mkdir -p $PKG_DIR/build
    cp -i /home/pkger/*.deb $PKG_DIR/build
}

function clean() {
    rm -r /home/pkger/python-synergy-scheduler-manager{,_${PKG_VERSION}.orig.tar.bz2}
}

clean || true  # no cleaning to do on a fresh install
copy_source
get_version
setup
build
