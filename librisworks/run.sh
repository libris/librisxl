#!/bin/bash

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

SCRIPTS_DIR=scripts
REPORT_DIR=reports/merge-works/$ENV-$(date +%Y%m%d)

mkdir -p $REPORT_DIR/{clusters,normalizations,merged-works}

CLUSTERS_DIR=$REPORT_DIR/clusters
NORMALIZATIONS_DIR=$REPORT_DIR/normalizations

FIND_CLUSTERS=$CLUSTERS_DIR/find-clusters
ALL_CLUSTERS=$CLUSTERS_DIR/1-all.tsv
MERGED_CLUSTERS=$CLUSTERS_DIR/2-merged.tsv
TITLE_CLUSTERS=$CLUSTERS_DIR/3-title-clusters.tsv
SWEDISH_FICTION=$CLUSTERS_DIR/4-swedish-fiction.tsv
NO_ANONYMOUS_TRANSLATIONS=$CLUSTERS_DIR/5-no-anonymous-translations.tsv

LANGUAGE_IN_TITLE=$NORMALIZATIONS_DIR/1-titles-with-language
ELIB_DESIGNERS=$NORMALIZATIONS_DIR/2-elib-cover-designer
CONTRIBUTION=$NORMALIZATIONS_DIR/3-contribution
ROLES_TO_INSTANCE=$NORMALIZATIONS_DIR/4-roles-to-instance

# Clustring step 1 TODO: run only on recently updated records after first run
echo "Finding new clusters..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar $JAR_FILE \
  $ARGS --report $FIND_CLUSTERS $SCRIPTS_DIR/find-work-clusters.groovy >$ALL_CLUSTERS 2>/dev/null
NUM_CLUSTERS=$(count_lines $ALL_CLUSTERS)
echo "$NUM_CLUSTERS clusters found"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Clustring step 2
echo
echo "Merging clusters..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$ALL_CLUSTERS -jar $JAR_FILE \
  $ARGS $SCRIPTS_DIR/merge-clusters.groovy >$MERGED_CLUSTERS 2>/dev/null
NUM_CLUSTERS=$(count_lines $MERGED_CLUSTERS)
echo "Merged into $NUM_CLUSTERS clusters"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Clustring step 3
echo
echo "Finding title clusters..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$MERGED_CLUSTERS -jar $JAR_FILE \
  $ARGS $SCRIPTS_DIR/title-clusters.groovy >$TITLE_CLUSTERS 2>/dev/null
NUM_CLUSTERS=$(count_lines $TITLE_CLUSTERS)
echo "$NUM_CLUSTERS title clusters found"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Filter: Swedish fiction
echo
echo "Filtering on Swedish fiction..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$TITLE_CLUSTERS -jar $JAR_FILE \
  $ARGS $SCRIPTS_DIR/swedish-fiction.groovy >$SWEDISH_FICTION 2>/dev/null
NUM_CLUSTERS=$(count_lines $SWEDISH_FICTION)
echo "Found $NUM_CLUSTERS title clusters with Swedish fiction"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Normalization
echo
echo "Removing language from work titles..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION -jar $JAR_FILE \
  $ARGS --report $LANGUAGE_IN_TITLE $SCRIPTS_DIR/language-in-work-title.groovy 2>/dev/null
echo "$(count_lines $LANGUAGE_IN_TITLE/MODIFIED.txt) records affected, report in $LANGUAGE_IN_TITLE"

echo
echo "Specifying designer roles in Elib records..." # NOTE: Not dependent on clustring, can be run anytime after ContributionByRoleStep has been deployed.
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar $JAR_FILE \
  $ARGS --report $ELIB_DESIGNERS $SCRIPTS_DIR/elib-unspecified-contributor.groovy 2>/dev/null
echo "$(count_lines $ELIB_DESIGNERS/MODIFIED.txt) records affected, report in $ELIB_DESIGNERS"

echo
echo "Normalizing contribution..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION -jar $JAR_FILE \
  $ARGS --report $CONTRIBUTION $SCRIPTS_DIR/normalize-contribution.groovy 2>/dev/null
echo "$(count_lines $CONTRIBUTION/MODIFIED.txt) records affected, report in $CONTRIBUTION"

echo
echo "Moving roles to instance..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION -jar $JAR_FILE \
  $ARGS --report $ROLES_TO_INSTANCE $SCRIPTS_DIR/contributions-to-instance.groovy 2>/dev/null
echo "$(count_lines $ROLES_TO_INSTANCE/MODIFIED.txt) records affected, report in $ROLES_TO_INSTANCE"

# Filter: Drop anonymous translations
echo "Filtering out anonymous translations..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$SWEDISH_FICTION -jar $JAR_FILE \
  $ARGS $SCRIPTS_DIR/drop-anonymous-translations.groovy >$NO_ANONYMOUS_TRANSLATIONS 2>/dev/null
NUM_CLUSTERS=$(count_lines $NO_ANONYMOUS_TRANSLATIONS)
echo "$NUM_CLUSTERS clusters ready for merge"
if [ $NUM_CLUSTERS == 0 ]; then
  exit 0
fi

# Merge
echo
echo "Merging..."
time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters=$NO_ANONYMOUS_TRANSLATIONS -jar $JAR_FILE \
  $ARGS --report $REPORT_DIR/merged-works $SCRIPTS_DIR/merge-works.groovy 2>/dev/null