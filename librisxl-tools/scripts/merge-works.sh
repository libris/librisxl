#!/bin/bash

# Run from whelktool dir

count_lines() {
  if [ -f $1 ]; then
    wc -l $1 | cut -d ' ' -f 1
  else
    echo 0
  fi
}

if ! [[ "$1" =~ ^(local|dev|dev2|qa|stg|prod)$ ]]; then
  echo "Missing or invalid environment"
  exit 1
fi

ENV=$1
ARGS="${@:2}"
NUM_CLUSTERS=0

REPORT_DIR=reports/merge-works/$ENV-$(date +%Y%m%d)

mkdir -p $REPORT_DIR/{clusters,normalizations,merged-works}

CLUSTERS_DIR=$REPORT_DIR/clusters
NORMALIZATIONS_DIR=$REPORT_DIR/normalizations

FIND_CLUSTERS=$CLUSTERS_DIR/find-clusters
ALL_CLUSTERS=$CLUSTERS_DIR/1-all.tsv
MERGED_CLUSTERS=$CLUSTERS_DIR/2-merged.tsv
TITLE_CLUSTERS=$CLUSTERS_DIR/3-title-clusters.tsv
SWEDISH_FICTION=$CLUSTERS_DIR/4-swedish-fiction.tsv
#ANONYMOUS_TRANSLATIONS=$CLUSTERS_DIR/5-anonymous-translations.tsv

LANGUAGE_IN_TITLE=$NORMALIZATIONS_DIR/1-titles-with-language
LINK_CONTRIBUTION=$NORMALIZATIONS_DIR/2-link-contribution
RESP_STATEMENT=$NORMALIZATIONS_DIR/3-responsibilityStatement
ILLUSTRATORS=$NORMALIZATIONS_DIR/4-illustrators
TRANSLATION_OF=$NORMALIZATIONS_DIR/5-translationOf

# Clustring step 1 TODO: run only on recently updated records after first run
echo "Finding new clusters..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar \
  $ARGS --report $FIND_CLUSTERS scripts/analysis/find-work-clusters.groovy >$ALL_CLUSTERS 2>/dev/null
NUM_CLUSTERS=$(count_lines $ALL_CLUSTERS)
echo "$NUM_CLUSTERS clusters found"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Clustring step 2
echo
echo "Merging clusters..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$ALL_CLUSTERS -jar build/libs/whelktool.jar \
  $ARGS scripts/analysis/merge-clusters.groovy >$MERGED_CLUSTERS 2>/dev/null
NUM_CLUSTERS=$(count_lines $MERGED_CLUSTERS)
echo "Merged into $NUM_CLUSTERS clusters"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Clustring step 3
echo
echo "Finding title clusters..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool \
  $ARGS -tc $MERGED_CLUSTERS >$TITLE_CLUSTERS
NUM_CLUSTERS=$(count_lines $TITLE_CLUSTERS)
echo "$NUM_CLUSTERS title clusters found"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Filter: Swedish fiction
echo
echo "Filtering on Swedish fiction..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool \
  $ARGS -f $TITLE_CLUSTERS >$SWEDISH_FICTION
NUM_CLUSTERS=$(count_lines $SWEDISH_FICTION)
echo "Found $NUM_CLUSTERS title clusters with Swedish fiction"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Normalization step 1 (Probably not necessary after first run, titles with language doesn't seem to occur in newer records)
echo
echo "Removing language from work titles..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION -jar build/libs/whelktool.jar \
  $ARGS --report $LANGUAGE_IN_TITLE src/main/groovy/datatool/scripts/mergeworks/normalize/language-in-work-title.groovy 2>/dev/null
echo "$(count_lines $LANGUAGE_IN_TITLE/MODIFIED.txt) records affected, report in $LANGUAGE_IN_TITLE"

# Normalization step 2
echo
echo "Linking contribution..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION -jar build/libs/whelktool.jar \
  $ARGS --report $LINK_CONTRIBUTION src/main/groovy/datatool/scripts/mergeworks/normalize/link-contribution.groovy 2>/dev/null
echo "$(count_lines $LINK_CONTRIBUTION/MODIFIED.txt) records affected, report in $LINK_CONTRIBUTION"

# Normalization step 3
echo
echo "Adding contributions found in responsibilityStatement..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION -jar build/libs/whelktool.jar \
  $ARGS --report $RESP_STATEMENT src/main/groovy/datatool/scripts/mergeworks/normalize/fetch-contribution-from-respStatement.groovy 2>/dev/null
echo "$(count_lines $RESP_STATEMENT/MODIFIED.txt) records affected, report in $RESP_STATEMENT"

# Normalization step 4
echo
echo "Adding 9pu code to illustrators..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION -jar build/libs/whelktool.jar \
  $ARGS --report $ILLUSTRATORS src/main/groovy/datatool/scripts/mergeworks/normalize/add-9pu-to-illustrators.groovy 2>/dev/null
echo "$(count_lines $ILLUSTRATORS/MODIFIED.txt) records affected, report in $ILLUSTRATORS"

# Normalization step 5
echo
echo "Adding missing translationOf..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION -jar build/libs/whelktool.jar \
  $ARGS --report $TRANSLATION_OF src/main/groovy/datatool/scripts/mergeworks/normalize/add-missing-translationOf.groovy 2>/dev/null
echo "$(count_lines $TRANSLATION_OF/MODIFIED.txt) records affected, report in $TRANSLATION_OF"

# Filter: Drop translations without translator // TODO: Decide what to do with these, in the meantime don't "hide" them
#echo "Filtering out translations without translator..."
#time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool \
#  $ARGS -tr $SWEDISH_FICTION >$ANONYMOUS_TRANSLATIONS
#NUM_CLUSTERS=$(count_lines $ANONYMOUS_TRANSLATIONS)
#echo "$NUM_CLUSTERS clusters ready for merge"
#if [ $NUM_CLUSTERS == 0 ]; then
#  exit 0
#fi

# Merge
echo
echo "Merging..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -cp build/libs/whelktool.jar datatool.WorkTool \
  $ARGS -r $REPORT_DIR/merged-works -m $SWEDISH_FICTION