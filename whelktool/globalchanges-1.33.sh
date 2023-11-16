#!/bin/bash
set -euxo pipefail
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --skip-index scripts/2023/08/lxl-4243-move-out-solitary-contentType-from-hasPart.groovy
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --skip-index scripts/cleanups/2023/05/gf-cleanup.groovy
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --skip-index scripts/cleanups/2020/08/lxl-3294-move-bearer-like-gfs-from-work-to-instance.groovy
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --skip-index scripts/2023/10/elib-unspecified-contributor.groovy
