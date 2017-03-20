#!/usr/bin/env bash
set -e -x

PKG_DIR=/tmp/synergy


function copy_source() {
    cd /home/pkger
    cp -r $PKG_DIR python-synergy-scheduler-manager
    rm -r python-synergy-scheduler-manager/{.eggs,.tox,.testrepository,build,dist} || true
}

function get_version() {
    if [[ -z $PKG_VERSION ]]; then
        cd $PKG_DIR
        export PKG_VERSION=$(git tag -l "*.*.*" | sort -V | tail -1)
    fi
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
