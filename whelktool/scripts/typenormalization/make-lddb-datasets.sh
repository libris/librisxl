#!/bin/bash
set -euo pipefail

# Prerequisite: Generated files located a given BASEPATH (see ./dump-lddb-excerpts.sh)
BASEPATH=$1

# 1. Create *self-described* dataset-filea (with relative id:s)
(echo '{"@id": "https://libris.kb.se/dataset/works", "@type": "Dataset", "label": "Works", "created": "2025-02-21T13:37:00Z"}'; zcat $BASEPATH-works.jsonl.gz) | sed 's!https://libris-stg.kb.se/!!g' > $BASEPATH-works-dataset.json.lines
(echo '{"@id": "https://libris.kb.se/dataset/instances", "@type": "Dataset", "label": "Instances", "created": "2025-02-21T13:37:00Z"}'; zcat $BASEPATH-instances.jsonl.gz) | sed 's!https://libris-stg.kb.se/!!g' > $BASEPATH-instances-dataset.json.lines

# 2. Optionally copy files to a separate devops machine:
#scp $BASEPATH-{works,instances}-dataset.json.lines $DEVOPS_HOST:/var/tmp/lddb-datasets

# 3. Goto importers (and build the xlimporter.jar if needed):
#  $ cd ../../../importers
# 4. Run (assuming a set source BASENAME and target XLPROPS):
#  $ java -Dxl.secret.properties=$XLPROPS -jar build/libs/xlimporter.jar dataset /var/tmp/lddb-datasets/$BASENAME-works.jsonld.lines https://libris.kb.se/dataset/works
#  $ java -Dxl.secret.properties=$XLPROPS -jar build/libs/xlimporter.jar dataset /var/tmp/lddb-datasets/$BASENAME-instances.jsonld.lines https://libris.kb.se/dataset/instances

# 0. To DROP a dataset (if nothing links to it):
#  #$ java -jar build/libs/xlimporter.jar dropDataset https://libris.kb.se/dataset/instances
