#!/bin/bash
set -euxo pipefail
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --skip-index scripts/2023/01/lxl-3908-romanized-to-byLang/bib880.groovy
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --skip-index scripts/2023/03/lxl-3880-shuffle-work-titles/expressionOf.groovy
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --skip-index scripts/2023/03/lxl-3880-shuffle-work-titles/translationOf-into-hasPart.groovy
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --skip-index scripts/2023/03/lxl-3880-shuffle-work-titles/work-title-to-translationOf.groovy
