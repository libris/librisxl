#!/bin/bash
set -euo pipefail
cat $1 | grep '^<' | sed -E "s!<.+/([^/]+)#it>.*!'\1'!" | tr '\n' ',' | sed 's/,$//'
