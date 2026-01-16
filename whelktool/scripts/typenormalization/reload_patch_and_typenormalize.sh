#!/bin/bash
set -euxo pipefail

# Set variables
ENV=qa

# Prepare the "Under construction" page for Libris landing pages
# Start in the devops repository
pushd $HOME/devops

# Stop services - user interfaces become unavailable
# Disable nightly exports
# Recreate storage and import data from prod
# Redeploy definitions
fab xl_qa xl_stop_services app.lxlweb.stop app.dataexporttooling.disable_nightly_exports app.whelk.recreate_storage app.whelk.import_prod_data app.whelk.reload_syscore app.whelk.reload_common app.whelk.reload_docs app.whelk.load_datasets:i18n

# Move into whelktool
pushd $HOME/globalchanges-QA/librisxl/whelktool

# Patch SAOGF
time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -Drdfdata=$HOME/devops/repositories/$ENV/definitions/source/saogf/saogf-patch-wip.ttl -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --skip-index --allow-loud --validation SKIP_AND_LOG --dataset-validation SKIP_AND_LOG scripts/examples/apply-rdf-patch.groovy

# Fix lddb__dependencies (so that reverse relationships can be displayed)
time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) scripts/cleanups/2025/11/auth-deps.groovy

# Run typenormalization
time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dtypenormalization=simple-types-algorithm -DaddCategory=true -DreplaceIssuanceTypes=false -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --validation OFF --dataset-validation SKIP_AND_LOG --skip-index scripts/typenormalization/main.groovy

# Go back to previous repository
popd

# Reindex
# Restart services - when this step is completed, changes are visible in user interfaces
# Reload SPARQL
fab xl_qa app.whelk.reindex xl_start_services app.lxlweb.start app.whelk.reload_sparql