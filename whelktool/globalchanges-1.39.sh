#!/bin/bash
set -euxo pipefail
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) scripts/cleanups/2025/05/fmt-329-TextInstance-to-Print.groovy