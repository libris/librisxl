#!/bin/bash
set -e

defsdir=$1
shift
buildargs=$@

if [[ "$defsdir" == "" ]]; then
    echo "Please provide your local definitions directory."
    exit 1
fi

ensure_venv_with_reqs() {
    rm -rf .venv
    python3 -m venv .venv
    .venv/bin/pip install wheel
    .venv/bin/pip install -r requirements.txt
}

builddefs() {
    ensure_venv_with_reqs
    .venv/bin/python datasets.py $buildargs
}

if [[ ! -d "$defsdir" ]]; then
    mkdir -p $(dirname $defsdir)
    pushd $(dirname $defsdir)
        git clone https://github.com/libris/definitions
    popd
    pushd $defsdir
        builddefs
    popd
else
    pushd $defsdir
        git fetch
        local=$(git rev-parse HEAD)
        remote=$(git rev-parse "@{upstream}")
        if [[ $local != $remote ]]; then
            git pull
            builddefs
        fi
    popd
fi
