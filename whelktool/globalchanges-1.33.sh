#!/bin/bash
set -euxo pipefail
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) scripts/2023/08/lxl-4243-move-out-solitary-contentType-from-hasPart.groovy
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) scripts/cleanups/2023/07/lxl-4221-move-illustrativecontent-to-instance.groovy
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) scripts/2023/10/elib-unspecified-contributor.groovy
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) scripts/2023/05/lxl-2512-move-contribution-by-relator-domain/script.groovy
