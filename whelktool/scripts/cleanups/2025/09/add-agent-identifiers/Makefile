build/candidates-nonsw-1.json: chunkedjson.py build/candidates-nonsw-1.csv
	python3 $^ > $@

build/candidates-nonsw-1.csv: get-candidates-for-isni-enrichment.rq
	mkdir -p build
	./get-candidates-for-isni-enrichment.sh > $@

build/candidates-nonsw-2.csv: get-candidates-for-isni-enrichment.rq
	./get-candidates-for-isni-enrichment.sh 10000 > $@
