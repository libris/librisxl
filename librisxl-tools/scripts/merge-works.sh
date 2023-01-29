#!/bin/bash

# Run from whelktool dir

if ! [[ "$1" =~ ^(local|dev|dev2|qa|stg|prod)$ ]]; then
    echo "Missing or invalid environment"
    exit 1
fi

ENV=$1
#UPDATED_RECORDS=TODO: Provide id list of recently updated records
REPORT_DIR=reports/merge-works/$ENV-$(date +%Y%m%d)

mkdir -p $REPORT_DIR/{clusters,normalizations,merged-works}

CLUSTERS_DIR=$REPORT_DIR/clusters
NORMALIZATIONS_DIR=$REPORT_DIR/normalizations

# Clustring step 1 TODO: run only on recently updated records after first run
#time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dids=$UPDATED_RECORDS -jar build/libs/whelktool.jar \
#  scripts/analysis/find-work-clusters.groovy >$CLUSTERS_DIR/1-all.tsv
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar \
  scripts/analysis/find-work-clusters.groovy >$CLUSTERS_DIR/1-all.tsv

# Clustring step 2
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$CLUSTERS_DIR/1-all.tsv -jar build/libs/whelktool.jar \
  scripts/analysis/merge-clusters.groovy >$CLUSTERS_DIR/2-merged.tsv

# Clustring step 3
time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool \
  -tc $CLUSTERS_DIR/2-merged.tsv >$CLUSTERS_DIR/3-title-clusters.tsv

# Filter: Swedish fiction
time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool \
  --dry-run -f $CLUSTERS_DIR/3-title-clusters.tsv >$CLUSTERS_DIR/4-swedish-fiction.tsv

# Normalization step 1 (Probably not necessary after first run, titles with language doesn't seem to occur in newer records)
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$CLUSTERS_DIR/4-swedish-fiction.tsv -jar build/libs/whelktool.jar \
  --report $NORMALIZATIONS_DIR/1-titles-with-language src/main/groovy/datatool/scripts/mergeworks/normalize/language-in-work-title.groovy

# Normalization step 2
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$CLUSTERS_DIR/4-swedish-fiction.tsv -jar build/libs/whelktool.jar \
  --report $NORMALIZATIONS_DIR/2-link-contribution src/main/groovy/datatool/scripts/mergeworks/normalize/link-contribution.groovy

# Normalization step 3
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$CLUSTERS_DIR/4-swedish-fiction.tsv -jar build/libs/whelktool.jar \
  --report $NORMALIZATIONS_DIR/3-responsibilityStatement src/main/groovy/datatool/scripts/mergeworks/normalize/fetch-contribution-from-respStatement.groovy

# Normalization step 4
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$CLUSTERS_DIR/4-swedish-fiction.tsv -jar build/libs/whelktool.jar \
  --report $NORMALIZATIONS_DIR/4-illustrators src/main/groovy/datatool/scripts/mergeworks/normalize/add-9pu-to-illustrators.groovy

# Filter: Drop translations without translator
time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool \
  --dry-run -tr $CLUSTERS_DIR/4-swedish-fiction.tsv >$CLUSTERS_DIR/5-no-translations-without-translators.tsv

# Merge
time java -Xmx4G -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool \
  -m $CLUSTERS_DIR/5-no-translations-without-translators.tsv $REPORT_DIR/merged-works


