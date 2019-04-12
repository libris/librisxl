# LDDB

## Introduction

LDDB ("Linked Data Data Base") is the core data storage in XL.

It represents a set of conventions and methods, mostly about storing and
querying [JSON in PostgreSQL 9+](https://www.postgresql.org/docs/current/static/functions-json.html).

## Design

1. Use compact, flat JSON-LD, with PNames only in keys and `@type` values.
2. Use an outer object with a `@graph` array. This represents a "record" in
   LDDB, and is logically equivalent to a named graph (albeit it is represented
   as a default graph).
3. Put the description of the named graph as item 0 in the array, and its
   `mainEntity` as item 1.
4. Use `sameAs` as record and entity aliases.

## Crunching JSON Lines from PSQL

### In General

If the built-in JSON support in PSQL doesn't cut it, read on here.

Use `psql` and `COPY (...) TO STDOUT`:

```bash
$ psql -h $HOST -U $USER -tc "COPY (SELECT ...) TO STDOUT;" | sed 's/\\\\/\\/g'
```

(The `sed` part converts escaped `psql` output to valid JSON.)

For repetitive processing, consider redirecting the output to a file and processing it locally.

```bash
$ psql -h $HOST -Uwhelk -tc "COPY (SELECT data FROM lddb WHERE collection = 'bib' AND deleted = false) TO stdout;" | sed 's/\\\\/\\/g' > stg-lddb-bib.json.lines
$ cat stg-lddb-bib.json.lines |  ...
```

### Using JQ (and AWK)

[JQ](https://stedolan.github.io/jq/) is a command line tool to process JSON.
Most package managers provide it.

(Here also using `time` and [AWK](https://en.wikipedia.org/wiki/AWK) for
convenience...)

#### Examples:

Find and count all authorized ("pre-coordinated") ComplexSubjects:

```bash
$ time psql -h $HOST -Uwhelk -tc "COPY (SELECT data FROM lddb WHERE collection = 'auth' AND deleted = false) TO STDOUT;" | sed 's/\\\\/\\/g' |
    jq '.["@graph"][1] | select(.["@type"] == "ComplexSubject") | .prefLabel' |
    awk '{print NR" "$0}'

1 "Varumärken--juridik och lagstiftning"
2 "Räkenskaper--historia"
3 "Skiften--juridik och lagstiftning"
4 "Substitutionsprincipen--miljöaspekter"
...
```

Count all usages of anonymous ("post-coordinated") ComplexSubjects:

```bash
$ cat stg-lddb-bib.json.lines |
    jq '.["@graph"][2].subject[]? | select(.["@type"] == "ComplexSubject") | .prefLabel' |
    awk '{printf "\r%s", NR}'

1234...
```

Find all ISBN values containing punctuation:

```bash
$ time cat stg-lddb-bib.json.lines |
    jq '.["@graph"][1]?.identifiedBy[]? |
        select(.["@type"] == "ISBN" and .value)? |
        .value | match(".+([^ ] ?[;:,]$|^[;:,])")? |
        .captures[0].string' |
    awk '{ a[$0]++ }
         END { for (k in a) print k": " a[k] }'

"6 ;": 1
") :": 6
") ;": 5
"1 ;": 1
...
```

Count the types of `_marcUncompleted` (including none):

```bash
$ time cat stg-lddb-bib.json.lines |
    jq -c '.["@graph"][]|._marcUncompleted?|type' |
    awk '{a[$0]++; printf "\r"; for (k in a) printf "%s %s; ", a[k], k }'

28 "array"; 214680 "null"; 27 "object";  ...
```

Find and count all `_marcUncompleted` patterns (fields and subfields):

```bash
$ time cat stg-lddb-bib.json.lines |
    jq -c '.["@graph"][] | select(has("_marcUncompleted"))? |
           ._marcUncompleted |
           if type == "object" then [.] else . end |
           .[] | [keys, ._unhandled]' |
    awk '{ a[$0]++ } END { for (k in a) print a[k]": "k }' |
    sort -nr

31: [["773","_unhandled"],["o"]]
28: [["945"],null]
1: [["586","_unhandled"],["ind1"]]
1: [["024","_unhandled"],"ind1"]
...
```

### Using Python

Example stub:

```python
import json
import sys

for i, l in enumerate(sys.stdin):
    if not l.rstrip():
        continue
    l = l.replace(b'\\\\"', b'\\"')
    if i % 100000 == 0:
        print("At line", i, file=sys.stderr)
    try:
        data = json.loads(l)

        # PROCESS DATA HERE

    except ValueError as e:
        print("ERROR at", i, "in data:", file=sys.stderr)
        print(l, file=sys.stderr)
        print(e, file=sys.stderr)
```

### Create Json-shapes with statistics

1. See instructions under "In General" to create a local output stream of bib, auth or hold.
2. Run the `lddb_json_shape.py` script.

Example for auth collection:

```bash
$ psql -h $HOST -Uwhelk -tc "COPY (SELECT data FROM lddb WHERE collection = 'auth' AND deleted = false) TO stdout;" | sed 's/\\\\/\\/g' > stg-lddb-auth.json.lines
$ cat stg-lddb-auth.json.lines |  ...
$ cat stg-lddb-auth.json.lines | pypy librisxl-tools/scripts/lddb_json_shape.py > shapes-for-your-selection.json
```

When crunching lots of data, use [PyPy](http://pypy.org/) for speed.
