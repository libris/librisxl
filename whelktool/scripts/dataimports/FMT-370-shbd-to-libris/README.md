# SHBd to Libris - overview
A set of scripts used to transform SHBd MARC data records, created from digitized print volumes, into KBV entities and import them into Libris, either matching and updating existing entities in Libris or creating new ones.  

## Running the full transformation and import process
### 1. Transform data from MARC to KBV
Use ConvertSHB.java to convert the SHBd source file (MARC) into a JSONLD file containing KBV entities (named graphs).
KBV entities contain an instance (with an embedded work) and an admin metadata record.

The result is saved to the designated results folder.

```
java -cp importers/build/libs/xlimporter.jar -Dxl.secret.properties={ENV}-secret.properties whelktool/scripts/dataimports/FMT-370-shbd-to-libris/ConvertSHB.java $HOME/data/shb.mrc > $HOME/data/shb.jsonld.lines
```

### 2. Improve the KBV data
Use reshape_shb.py to parse unstructured data and enrich as well as reshape the KBV instances and records. 

```
python3 whelktool/scripts/dataimports/FMT-370-shbd-to-libris/reshape_shb.py $HOME/data/joined_shb.jsonld.lines "$HOME/data/SHB-SAB_SAO-mapping.xlsx" $HOME/data/results $HOME/data/reports
```

The xlsx file contains a mapping from SHB section headings to SAOGF terms and SAB codes.


### 3. Find out what's already in Libris
Use identify_matches.py to match the enriched KBV instances against instances in Libris. It uses the Libris search API, as well as a custom match scoring function, to identify "good enough" matches.

In the designated results folder, the script creates
- One file of KBV instances which will be used to create new instances in Libris
- One file of KBV instances which will be used to enrich existing instances and works in Libris

```
SEARCH_CODES=title ; time python3 whelktool/scripts/dataimports/shb/identify_matches.py prod $HOME/data/shb-test-set-cleaned-with-subjects.jsonld.lines $HOME/data/results $HOME/data/reports $SEARCH_CODES
```
The argument search_codes specifiecs which values should be queried with a search code (for example *title = (Pippi Långstrump)*) rather than as free text. `SEARCH_CODES=title` seems to return a result that is sufficiently broad but not too noisy.

### 4. Create/update in Libris
Use CreateAndUpdateFromSHBd.groovy to create and update entities in Libris.

```
time java -Xmx2G -Dxl.secret.properties=../secret.properties-{ENV} -DdataFolder=$HOME/data/import_test_data -jar build/libs/whelktool.jar --report reports/fmt-370_$(date +%Y%m%d) --allow-loud scripts/dataimports/FMT-370-shbd-to-libris/CreateAndUpdateFromSHBd.groovy
```

The --allow-loud flag is needed to create new instances.

## Other files
### Testing
- Tests covering typical SHB syntaxes and some special cases
    - test_reshape_shb.py


- Scripts evaluating comparison between SHB and Libris data
    - test_matching_logic.py
    - test_matching_logic_cases.py


### The data folder
Contains a set of test records, good for debugging, and reports from manual analyses.