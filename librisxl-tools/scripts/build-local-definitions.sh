#!/bin/bash
set -e

defsdir=$1
shift
buildargs=$@

if [[ "$defsdir" == "" ]]; then
    echo "Please provide your local definitions directory."
    exit 1
fi

ensurevenv() {
    rm -rf .venv
    python3 -m venv .venv
    .venv/bin/pip install wheel
}

updatereqs() {
    ensurevenv
    .venv/bin/pip install -r requirements.txt
}

builddefs() {
    ensurevenv
    .venv/bin/python datasets.py $buildargs
}

if [[ ! -d "$defsdir" ]]; then
    mkdir -p $(dirname $defsdir)
    pushd $(dirname $defsdir)
        git clone https://github.com/libris/definitions
    popd
    pushd $defsdir
        updatereqs
        builddefs
    popd
else
    pushd $defsdir
        git fetch
        local=$(git rev-parse HEAD)
        remote=$(git rev-parse "@{upstream}")
        if [[ $local != $remote ]]; then
            git pull
            if [[ requirements.txt -nt .venv && -d .venv ]]; then
                touch -r requirements.txt .venv
                updatereqs
            elif [[ ! -d .venv ]]; then
                updatereqs
            fi
            builddefs
        fi
    popd
fi
