# SHB to Libris - overview

## Running the full transformation and import process
### 1. MARC -> KBV
Use ConvertSHB.java to convert the SHBd source file (MARC) into a JSONLD file containing KBV entities (named graphs).
KBV entities contain an instance (with an embedded work) and an admin metadata record.

The result is saved to the designated results folder.

```
java -cp importers/build/libs/xlimporter.jar -Dxl.secret.properties={ENV}-secret.properties whelktool/scripts/dataimports/shb/ConvertSHB.java $HOME/data/shb.mrc > $HOME/data/shb.jsonld.lines
```

### 2. Improve the KBV data
Use reshape_shb.py to parse unstructured data and enrich as well as reshape the KBV instances and records. 

```
python3 whelktool/scripts/dataimports/shb/reshape_shb.py $HOME/data/joined_shb.jsonld.lines "$HOME/code/libris/data/register SHB 2026_08_25_SAO_SAB.xlsx" $HOME/data/results $HOME/data/reports
```

The xlsx file contains a mapping from SHB section headings to SAOGF terms and SAB codes.


### 3. Find out what's already in Libris
Use identify_matches.py to match the enriched KBV instances against instances in Libris. 

In the designated results folder, the script creates
- One file of KBV instances which will be used to create new instances in Libris
- One file of KBV instances which will be used to enrich existing instances and works in Libris

```
SEARCH_CODES=title ; time python3 whelktool/scripts/dataimports/shb/identify_matches.py prod $HOME/data/shb-test-set-cleaned-with-subjects.jsonld.lines $HOME/data/results $HOME/data/reports $SEARCH_CODES
```
The argument search_codes specifiecs which values should be queried with a search code (for example *title = (Pippi Långstrump)*) rather than as free text. `SEARCH_CODES=title` seems to return a result that is sufficiently broad but not too noisy.

### 4. Create/update in Libris
Use create_and_update_libris_records.groovy to create and update entities in Libris.

## Other files
### Testing
- Tests covering typical SHB syntaxes and some special cases
    - reshape_parser_tests.py


- Scripts evaluating comparison between SHB and Libris data
    - evaluate_matching.py
    - evaluate_matching_test_cases.py

### Exploratory analysis
analyze_shb.ipynb

### The data folder
Contains a set of test records, good for debugging, and reports from manual analyses.