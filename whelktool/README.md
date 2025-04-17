# Whelktool

A tool for running scripted mass updates on XL data.

## Building
```
../gradlew jar
```

## Running
Minimal example
```
java -Xmx2G -Dxl.secret.properties=<secret.properties> -jar build/libs/whelktool.jar <path/to/script.groovy>
```

Example of using a timestamp based report dir
```
 ENV=qa && time java -Xmx2G -Dxl.secret.properties=$HOME/secret.properties-$ENV -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) scripts/examples/statistics.groovy
```

Options
```
$ java -jar build/libs/whelktool.jar --help
11 actionable tasks: 11 up-to-date
usage: whelktool [options] <SCRIPT>
 -a,--allow-loud                    Allow scripts to do loud
                                    modifications.
 -d,--dry-run                       Do not save any modifications.
 -dv,--dataset-validation <MODE>    [UNSAFE] Set read-only dataset
                                    validation mode. Defaults to ON.
                                    Possible values: ON/OFF/SKIP_AND_LOG
 -h,--help                          Print this help message and exit.
 -idchg,--allow-id-removal          [UNSAFE] Allow script to remove
                                    document ids, e.g. sameAs.
 -l,--limit <LIMIT>                 Amount of documents to process.
 -n,--stats-num-ids <arg>           Number of ids to print per entry in
                                    STATISTICS.txt.
 -p,--parameters <PARAMETER-FILE>   Path to JSON file with parameters to
                                    script
 -r,--report <REPORT-DIR>           Directory where reports are written
                                    (defaults to "reports").
 -s,--step                          Change one document at a time,
                                    prompting to continue.
 -T,--no-threads                    Do not use threads to parallellize
                                    batch processing.
 -t,--num-threads <N>               Override default number of threads
                                    (48).
 -v,--validation <MODE>             [UNSAFE] Set JSON-LD validation mode.
                                    Defaults to ON. Possible values:
                                    ON/OFF/SKIP_AND_LOG
```

* Use `--dry-run` to test your script without writing any changes.
* Skip indexing to elasticsearch with `--skip-index`. This speeds up execution and can be used when scripts are run
  before a full ES reindex.
* Properties updated on all created/modified documents
    * `meta.generationProcess` - script name
    * `meta.generationDate` - modification timestamp
* "loud" modifications update the `meta.modified` timestamp which means that documents will be included
  as updated in e.g. marc_export and OAI-PMH. Specify inside the script (see below) and run with `--allow-loud`.
* If the script throws an exception, the execution will be halted and the exception written to ERRORS.txt
* The following reports are automatically created. See below for how to create custom reports.
    * MAIN.txt - Console output of whelktool
    * ERRORS.txt - Exceptions thrown by the script
    * MODIFIED.txt - ids of updated documents
    * CREATED.txt - ids of created documents
    * DELETED.txt - ids of deleted instances
    * STATISTICS.txt - things you counted with the default `Statistics` instance. See below.

Example output
```
==> reports/qa-20201102-142402/MAIN.txt <==
Running Whelk against:
  PostgreSQL:
    url:     jdbc:postgresql://pgsql01-qa.libris.kb.se/whelk
  ElasticSearch:
    hosts:   [http://kblxes01.kb.local:9200, http://kblxes02.kb.local:9200, http://kblxes03.kb.local:9200]
    cluster: kblxes_cluster
    index:   libris_qa
Using script: scripts/cleanups/2020/10/lxl-3416-inScheme-fast.groovy
  dryRun

Select by 2042 IDs
Processed batch 1 (read: 599, processed: 500, modified: 496, deleted: 0, new saved: 0 (at 20.35 docs/s))
Processed batch 2 (read: 758, processed: 758, modified: 753, deleted: 0, new saved: 0 (at 29.57 docs/s))
Processed selection: read: 758, processed: 758, modified: 753, deleted: 0, new saved: 0 (at 29.57 docs/s). Done in 25.637 s.
```

## Writing scripts
Whelktool scripts are groovy scripts that have access to a couple of methods for accessing and manipulating XL data.

A minimal script
```groovy
selectByIds(['j2vzn03v08lfhrm']) {
    it.graph[1].responsibilityStatement = 'Hello, world!'
    it.scheduleSave()
}
```

Some example scripts can be found [here](scripts/examples).

### Selecting documents to work on
All `selectBy...` methods take a closure which is executed once per matching document.

By entity id (IRI)
```groovy
Collection<String> ids = [
  'https://libris-qa.kb.se/cwp44h7p4hwkrxr',
  'https://libris-qa.kb.se/j2vzn03v08lfhrm',
  'https://id.kb.se/term/sao/Sverige--Lappland--Stora%20Lulevatten'
]
selectByIds(ids) { d ->
  // process
}
```

By system ids
```groovy
def ids = [
  'cwp44h7p4hwkrxr',
  'j2vzn03v08lfhrm',
  'dxq67p5q0wglcpr',
]
selectByIds(ids) { d ->
  // process
}
```

By legacy MARC collection (`bib`, `auth`, `hold`, `definitions` or `none`)
```groovy
selectByCollection('auth') { auth ->
  // process
}
```

By SQL

Defined as the `WHERE` clause to `SELECT data FROM lddb WHERE ...`
```groovy
String where = "collection = 'bib' AND deleted = false AND data#>>'{@graph,0,_marcUncompleted}' LIKE '%\"024\"%'"
selectBySqlWhere(where) { d ->
  // process
}
```
```
String where = """
        data#>>'{@graph,1,instanceOf,language,0,@id}' = 'https://id.kb.se/language/und' 
        and data#>>'{@graph,1,instanceOf,language,1,@id}' = 'https://id.kb.se/language/swe' 
        and not data#>>'{@graph,0,bibliography,0,sigel}' = 'EPLK' 
        and deleted = false
        """
        
selectBySqlWhere(where) { d ->
  // process
}
```

### Manipulating data
The object passed to the closure is a `DocumentItem` which is a wrapper around a `whelk.Document`. The
document is accessed through the property `doc`.

Document::data contains the JSON-LD document mapped to Java/Groovy objects (Map, List, String, etc).
```groovy
selectByIds(ids) { d ->
    Map thing = d.doc.data['@graph'][1]
}
```

Using Groovy multiple assignment
```groovy
selectByIds(ids) { d ->
    def (record, thing) = d.doc.data['@graph']
}
```

`DocumentItem` has a shorthand property `graph` for `doc.data['@graph']`
```groovy
selectByIds(ids) { d ->
    def (record, thing) = d.graph
}
```

Call `scheduleSave()` on `DocumentItem` to save changes (you need to do this inside the scope of the closure).
```groovy
selectByIds(ids) { d ->
    def (record, thing) = d.graph
    thing['foo'] = 'bar'
    d.scheduleSave()
}
```

Doing "loud" updates. Has to be executed with `--allow-loud`, see [Running](#Running).
```groovy
d.scheduleSave(loud: true)
```

>**NB!** The closure passed to `selectBy...` is normally executed in parallell on multiple threads.
This needs to be considered if manipulating data outside the closure.

>**NB!** If the document is scheduled for save but has been changed (by another thread/process/user)
before it is written to storage, the closure is automatically re-run on the changed document. This
means that the closure should be a pure function of the document data. (reports/statistics might be
skewed, but this is normally OK)

### Creating new documents
[create.groovy](scripts/examples/create.groovy)

Create `DocumentItem` with `create` then call `scheduleSave` inside a `selectFromIterable`.
```groovy
def data =
        [ "@graph": [
                [
                        "@id": "TEMPID",
                        "mainEntity" : ["@id": "TEMPID#it"]
                ],
                [
                        "@id": "TEMPID#it",
                        "@type": "Item",
                        "heldBy": ["@id": "https://libris.kb.se/library/Utb1"],
                        "itemOf": ["@id": "http://libris.kb.se.localhost:5000/wf7mw1h74fkt88r#it"]
                ]
        ]]

def item = create(data)
def itemList = [item]
selectFromIterable(itemList, { newlyCreatedItem ->
    newlyCreatedItem.scheduleSave()
})
```
### Reporting
Reports are written to the directory specified when running whelktool.
```groovy
PrintWriter myReport = getReportWriter("my-report.txt")
myReport.println("Hello, world!")
```

Because of weird scoping in Groovy scripts, if the report writer is used inside a method it has to
be specified like this (no type or `def`).
```groovy
myReport = getReportWriter("my-report.txt")

void myMethod() {
  myReport.println("Hello, world!")
}
```
another way
```groovy
class Context {
  static PrintWriter myReport
}
myReport = getReportWriter("my-report.txt")

void myMethod() {
  Context.myReport.println("Hello, world!")
}
```
### Counting things
`void incrementStats(String category, Object name, Object example = null)`


```groovy
incrementStats('category', 'name', 'example')

// "example" is automatically set to doc system ID inside selectBy...
selectByCollection('auth') {
    incrementStats('type', it.graph[1]['@type'])
}
```
Output in STATISTICS.txt
```
STATISTICS
========================
category (1)
------------
1 name [example]

type (331265)
-------------
245388 Person            [x8fshfd5v1gw685c, 6ht83p314vjv68qd, cpv7x71w9nxzcr6r]
 40344 Organization      [x8g4r5brvbmfnj3m, 20dhlg3l369t6cm, xv8b81rg586skx8]
 33130 Topic             [64jllxcq2bvmvm9, fcrv1l1z5lhlwd1, sq466p4b2mv4vnb]
  5745 Jurisdiction      [q2f29wl5nptg0qt7, dbqspw9x3zn3jf4, 64jlhp3q1rhbnm2]
  2626 GenreForm         [wt7bkk8f257kg16, zw9dlc9h4x4llxl, gdsw3vx05dwj0hk]
  1833 ComplexSubject    [t6wqn2s1r0hfhv36, gtm7831zdl398q0j, khwzz32347v1qsg]
  1480 Geographic        [rp354jn925v0crl, pm132gl747m4np1, wt7982sf52b12tc]
   369 Meeting           [1zcgmh3k4mn6lqf, pm149t871phxwqp, 20dhlm9l3rfk32q]
   178 Temporal          [64jlkvlq3glppcn, mkz29s153255c15, gdsw4ms00bc464l]
    77 Family            [53hls0pp5pvlf7x, rp36csv95ddg5g9, b8nrx38v2g61f4j]
    47 TopicSubdivision  [gdsvv6302xp38b8, wt799mjf4m2l3v0, fcrtt51z554zch3]
    26 ShelfMarkSequence [dr5ht274brzd6tql, x9p1cjvnvz5jzd14, 2ft5hppq0qdg2sfk]
    16 Work              [jgvz3fg2132bkdv, hftx1l712vthpt5, ljx152445346cdm]
     4 Text              [6kgtwh0s4xhjjxj8, w85jlbcrtmmxkcz8, q18gs4w6ngkm6tvh]
     2 Dataset           [zcxkkhnwwbc5s7lt, r5qcd2bnpc3ztpm8]
```

### Querying ElasticSearch
Query parameters are specified as a Map<String, List<String>> with the same syntax as in the [`/find` API](../rest/API.md)
```groovy
def q = [
    'foo'  : ['bar', 'baz'],
    'quux' : ['quuz'],
    '_sort': ["@id"]
]

Iterable<String> ids = queryIds(q)
Iterable<Map> docs = queryDocs(q) // as in search API result "items"
```

Combining with `selectById`
```groovy
def q = [
    "@type": ["Language"],
    '_sort': ["@id"]
]

ConcurrentLinkedQueue<String> languageLabels = new ConcurrentLinkedQueue<>()
selectByIds(queryIds(q).collect()) { languageLabels.add(it.graph[1]['prefLabelByLang']['sv']) }
```

### Tips
Any exception thrown inside `selectBy...` will halt script execution. Sometimes this is what you want, sometimes
it is easier to catch exceptions from edge cases and write them to an error report (with doc id). For example
during development of a script that takes a long time to run.
```groovy
errors = getReportWriter("errors.txt")
selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        errors.println("${bib.doc.shortId} $e")
        e.printStackTrace(errors)
        e.printStackTrace()
    }
}
```

### Utils
TODO...

`DocumentUtil` contains methods for traversing and manipulating the JSON structure.
Values can be replaced or removed.
```groovy
     /**
     * Traverse a JSON-LD structure in depth-first order
     *
     * @param data JSON-LD structure
     * @param visitor function to call for every value
     * @return true if data was changed
     */
    static boolean traverse(data, Visitor visitor)
    
    interface Visitor {
        Operation visitElement(def value, List path)
    }
```
Examples
```groovy
import whelk.util.DocumentUtil.Remove
import whelk.util.DocumentUtil.Replace

import static whelk.util.DocumentUtil.NOP

...
// the supplied visitor is called for each "node" with 
// path (e.g ['@graph', 1, 'instanceOf', 'translationOf', 'language']) and value (e.g. a Map, List or String) 
// and can return an operation for the node

// Replacing
DocumentUtil.traverse(d.graph, { Object value, List path ->
  (path && path.last() == 'propertyName') ? new Replace('new value') : NOP
})

// Removing
// If last entry in list or object is removed, the parent property is also removed
DocumentUtil.traverse(d.graph, { value, path ->
  (value == "Value") ? new Remove() : NOP
})

// Not returning anything from visitor is also NOP
DocumentUtil.traverse(d.graph, { value, path ->
  if (value instanceOf String) {
    println("$path $value")
  }
})
```
See also [DocumentUtilSpec.groovy](../whelk-core/src/test/groovy/whelk/util/DocumentUtilSpec.groovy)

----

You can use `asList` to deal with properties being objects or lists or non-existing.
```groovy
> asList([a:1])
Result: [[a:1]]

> asList('str')
Result: [str]

> asList([1, 2])
Result: [1, 2]

> asList(null)
Result: []

> asList(['is', 'a', 'list'])
Result: [is, a, list]

> asList(null)
Result: []
```

----
You can use `getAtPath(data, path, defaultTo=null)` for safe access to nested properties.
If the path is not valid the specified default value is returned.
```groovy
selectByIds(ids) { d ->
    def subjects = getAtPath(d.doc.data, ['@graph', 1, 'instanceOf', 'subject'], [])
}
```

Use `'*'` as a wildcard for getting all values in a list (set).
When using wildcards, `defaultTo` will always be an empty list.
```groovy
selectByIds(ids) { d ->
    def agentIds = getAtPath(d.graph[1], ['instanceOf', 'contribution', '*', 'agent', '@id'])
    def partTitles = getAtPath(d.graph[1], ['instanceOf', 'hasPart', '*', 'hasTitle', '*', 'mainTitle'])
}
```
