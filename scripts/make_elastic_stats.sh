ES_HOST=$1
TYPE=$2
OUT_DIR=$3
curl -s http://$ES_HOST/libris/_mapping | python -mjson.tool > /tmp/es_mapping.json
python scripts/elastic_stats.py /tmp/es_mapping.json ext-libris/src/main/resources/marcframe.json -f $TYPE -s $ES_HOST | bash > $OUT_DIR/es-stats-$TYPE.json
