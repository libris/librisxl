#!/bin/bash
set -eu

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

JAR_FILE=build/libs/librisworks.jar

WHELKTOOL_REPORT=whelktool-report
CLUSTER_TSV=clusters.tsv

SCRIPTS_DIR=scripts
REPORT_DIR=reports/merge-works/$ENV-$(date +%Y%m%d)

CLUSTERS_DIR=$REPORT_DIR/clusters
NORMALIZATIONS_DIR=$REPORT_DIR/normalizations
MERGED_WORKS_DIR=$REPORT_DIR/merged-works

ALL=$CLUSTERS_DIR/1-all
TITLES=$CLUSTERS_DIR/2-titles
MERGED=$CLUSTERS_DIR/3-merged
SWEDISH_FICTION=$CLUSTERS_DIR/4-swedish-fiction
NO_ANONYMOUS_TRANSLATIONS=$CLUSTERS_DIR/5-no-anonymous-translations

mkdir -p $CLUSTERS_DIR $NORMALIZATIONS_DIR $MERGED_WORKS_DIR $ALL $MERGED $TITLES $SWEDISH_FICTION $NO_ANONYMOUS_TRANSLATIONS

LANGUAGE_IN_TITLE=$NORMALIZATIONS_DIR/1-titles-with-language
ILL_CONTENT=$NORMALIZATIONS_DIR/2-illustrative-content
DEDUPLICATE_CONTRIBUTIONS=$NORMALIZATIONS_DIR/3-deduplicate-contributions
ADD_MISSING_CONTRIBUTION_DATA=$NORMALIZATIONS_DIR/4-add-missing-contribution-data
ROLES_TO_INSTANCE=$NORMALIZATIONS_DIR/5-roles-to-instance

# Clustering TODO: run only on recently updated records after first run
echo "Finding new clusters..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar $JAR_FILE \
  $ARGS --report $ALL/$WHELKTOOL_REPORT $SCRIPTS_DIR/find-work-clusters.groovy >$ALL/$CLUSTER_TSV 2>/dev/null
NUM_CLUSTERS=$(count_lines $ALL/$CLUSTER_TSV)
echo "$NUM_CLUSTERS clusters found"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Filter out duplicates
sort -uo $ALL/$CLUSTER_TSV $ALL/$CLUSTER_TSV

echo
echo "Finding title clusters..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$ALL/$CLUSTER_TSV -jar $JAR_FILE \
  $ARGS --report $TITLES/$WHELKTOOL_REPORT $SCRIPTS_DIR/title-clusters.groovy >$TITLES/$CLUSTER_TSV 2>/dev/null
NUM_CLUSTERS=$(count_lines $TITLES/$CLUSTER_TSV)
echo "$NUM_CLUSTERS title clusters found"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

echo
echo "Merging clusters..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$TITLES/$CLUSTER_TSV -jar $JAR_FILE \
  $ARGS --report $MERGED/$WHELKTOOL_REPORT $SCRIPTS_DIR/merge-clusters.groovy >$MERGED/$CLUSTER_TSV 2>/dev/null
NUM_CLUSTERS=$(count_lines $MERGED/$CLUSTER_TSV)
echo "Merged into $NUM_CLUSTERS clusters"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Filter: Swedish fiction
echo
echo "Filtering on Swedish fiction..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$MERGED/$CLUSTER_TSV -jar $JAR_FILE \
  $ARGS --report $SWEDISH_FICTION/$WHELKTOOL_REPORT $SCRIPTS_DIR/swedish-fiction.groovy >$SWEDISH_FICTION/$CLUSTER_TSV 2>/dev/null
NUM_CLUSTERS=$(count_lines $SWEDISH_FICTION/$CLUSTER_TSV)
echo "Found $NUM_CLUSTERS title clusters with Swedish fiction"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Normalization
echo
echo "Removing language from work titles..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION/$CLUSTER_TSV -jar $JAR_FILE \
  $ARGS --report $LANGUAGE_IN_TITLE $SCRIPTS_DIR/language-in-work-title.groovy 2>/dev/null
echo "$(count_lines $LANGUAGE_IN_TITLE/MODIFIED.txt) records affected, report in $LANGUAGE_IN_TITLE"

echo
echo "Moving illustrativeContent to instance..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar $JAR_FILE \
  $ARGS --report $ILL_CONTENT $SCRIPTS_DIR/lxl-4221-move-illustrativecontent-to-instance.groovy 2>/dev/null
echo "$(count_lines $ILL_CONTENT/MODIFIED.txt) records affected, report in $ILL_CONTENT"

echo
echo "Merging contribution objects with same agent..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION/$CLUSTER_TSV -jar $JAR_FILE \
  $ARGS --report $DEDUPLICATE_CONTRIBUTIONS $SCRIPTS_DIR/lxl-4150-deduplicate-contribution.groovy 2>/dev/null
echo "$(count_lines $DEDUPLICATE_CONTRIBUTIONS/MODIFIED.txt) records affected, report in $DEDUPLICATE_CONTRIBUTIONS"

echo
echo "Adding missing contribution data..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION/$CLUSTER_TSV -jar $JAR_FILE \
  $ARGS --report $ADD_MISSING_CONTRIBUTION_DATA $SCRIPTS_DIR/add-missing-contribution-data.groovy 2>/dev/null
echo "$(count_lines $ADD_MISSING_CONTRIBUTION_DATA/MODIFIED.txt) records affected, report in $ADD_MISSING_CONTRIBUTION_DATA"

echo
echo "Moving roles to instance..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION/$CLUSTER_TSV -jar $JAR_FILE \
  $ARGS --report $ROLES_TO_INSTANCE $SCRIPTS_DIR/contributions-to-instance.groovy 2>/dev/null
echo "$(count_lines $ROLES_TO_INSTANCE/MODIFIED.txt) records affected, report in $ROLES_TO_INSTANCE"

# Filter: Drop anonymous translations
echo "Filtering out anonymous translations..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION/$CLUSTER_TSV -jar $JAR_FILE \
  $ARGS --report $NO_ANONYMOUS_TRANSLATIONS/$WHELKTOOL_REPORT $SCRIPTS_DIR/drop-anonymous-translations.groovy \
  >$NO_ANONYMOUS_TRANSLATIONS/$CLUSTER_TSV 2>/dev/null
NUM_CLUSTERS=$(count_lines $NO_ANONYMOUS_TRANSLATIONS/$CLUSTER_TSV)
echo "$NUM_CLUSTERS clusters ready for merge"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Merge
echo
echo "Merging..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$NO_ANONYMOUS_TRANSLATIONS/$CLUSTER_TSV -jar $JAR_FILE \
  $ARGS --report $MERGED_WORKS_DIR $SCRIPTS_DIR/merge-works.groovy 2>/dev/null
echo "Done! See reports in $MERGED_WORKS_DIR"