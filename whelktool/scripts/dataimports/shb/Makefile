ROOT=../../../../
STORE=$(HOME)/Downloads

build/shb_alnum-reshaped.jsonld.lines: reshape_shb.py cache/shb_alnum-raw.jsonld.lines
	mkdir -p build
	python3 $^ cache/register_shb_240502-*.csv > $@

cache/shb_alnum-raw.jsonld.lines: ConvertSHB.java $(STORE)/shb_alfanum_marc
	mkdir -p cache
	java -cp $(ROOT)importers/build/libs/xlimporter.jar -Dxl.secret.properties=$(ROOT)/DEV2-secret.properties $^ > $@

build/shb-sample.trig: reshape_shb.py cache/shb_alnum-raw.jsonld.lines
	mkdir -p build
	python3 $^ cache/register_shb_240502-*.csv --sample-pretty-with $(ROOT)/../definitions/build/sys/context/kbv.jsonld | trld -ijsonld -o trig > $@

# See <https://wiki.documentfoundation.org/ReleaseNotes/7.2#Document_Conversion>
cache/register_shb_240502-*.csv: $(STORE)/register_shb_240502.xslx
	mkdir -p cache
	libreoffice --headless --convert-to csv:"Text - txt - csv (StarCalc)":"44,34,UTF8,1,,0,false,true,false,false,false,-1" $^ --outdir cache
