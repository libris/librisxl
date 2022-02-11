/* 
Clean up newspaper (dagstidningar + tidskrifter) shapes.
Link digitized newspaper monographs (issues) to their series. That is, replace supplementTo with hasSeries.

Don't touch "Projects" and "Channel records" in supplementTo for now.

Example
Before
...
bf2:title [
    a bf2:Title ;
    bf2:mainTitle "DAGENS NYHETER  1900-05-28"
    ] ;
...
bf2:supplementTo [
    a bf2:Instance ;
    bf2:instanceOf [
      a bf2:Work ;
      bf2:contribution [
        a bflc:PrimaryContribution ;
        bf2:agent [
          a bf2:Agent ;
          sdo:name "DAGENS NYHETER"
          ]
        ]
      ] ;
    :describedBy [
      a :Record ;
      :controlNumber "13991099"
      ] ;
    bf2:identifiedBy [
      a bf2:Issn ;
      rdf:value "1101-2447"
      ] , [
      a bf2:Strn ;
      rdf:value "http://libris.kb.se/resource/bib/13991099"
      ]
    ] ;
...

After
...
bf2:title [
    a bf2:Title ;
    bf2:mainTitle "DAGENS NYHETER  1900-05-28"
    ] ;
...
bf2:hasSeries <https://libris.kb.se/m5z2w4lz3m2zxpk#it> ;
...


Shapes encountered in dry-run

supplementTo shape (1762029)
----------------------------
1634199 [@type, hasTitle, describedBy]                 [2g7kwvwg0gxr0gbm, w92drwg0t5p9q64b, 2g7kx37w0svpm1sx]
  87473 [part, @type, hasTitle, describedBy]           [1kcg9k9c119d6x1, 2ldh2q8d1qhxq0n, 4ngk8vrg4mk4jwh]
  37834 [@type, instanceOf, describedBy, identifiedBy] [8slqsctl4r093pp, xg8dkhz81q4fv2l, 7rknhtxk26tbh3h]
   2523 [@type, hasTitle, describedBy, identifiedBy]   [m5z5p3tz4llsjv9, 6qjq8ncj07twbcz, xg8g0d482lxdlf0]
   
Examples
[@type, hasTitle, describedBy]
{@type=Instance, hasTitle=[{@type=Title, mainTitle=DAGENS NYHETER}], describedBy=[{@type=Record, controlNumber=13991099}]} [7m92hxhq5zj1bm8g, n26zztr6l6xc2498, 8nskj3cx60gc12vl]

[part, @type, hasTitle, describedBy]
{part=[2010-11-21], @type=Instance, hasTitle=[{@type=Title, mainTitle=SVENSKA DAGBLADET}], describedBy=[{@type=Record, controlNumber=13434192}]} [0jbf6m1b0hx1cgh]

[@type, instanceOf, describedBy, identifiedBy]
{@type=Instance, instanceOf={@type=Work, contribution=[{@type=PrimaryContribution, agent={@type=Agent, label=[DAGENS NYHETER]}}]}, describedBy=[{@type=Record, controlNumber=13991099}], identifiedBy=[{@type=ISSN, value=1101-2447}, {@type=STRN, value=http://libris.kb.se/resource/bib/13991099}]} [1kch914c1r5cst3, 1kch915c2bj59gb, 3mfkc3gf4g1rmfv]

[@type, hasTitle, describedBy, identifiedBy]
{@type=Instance, hasTitle=[{@type=Title, mainTitle=SVENSKA DAGBLADET}], describedBy=[{@type=Record, controlNumber=13434192}], identifiedBy=[{@type=ISSN, value=2001-3868}]} [0jbj4c5b264549m, 3mfm5p5f1xm7xsv, q828thb23j7cmqr]


There were no supplementTo with multiple controlnumbers (refering to newspaper serials)
*/
import groovy.transform.Memoized

def where = """
    data#>>'{@graph,1,supplementTo}' IS NOT NULL
    AND collection = 'bib'
    AND deleted = 'false'
"""

selectBySqlWhere(where) { bib ->
    def (record, thing) = bib.graph
    
    if (!thing.supplementTo) {
        return
    }

    def hasSeries = asList(thing.hasSeries) as Set
    def i = ((List) thing.supplementTo).iterator()
    while (i.hasNext()) {
        Map supplementTo = (Map) i.next()
        def serials = tidningSerialThings(supplementTo)
        if (serials) {
            incrementStats('supplementTo', supplementTo)
            incrementStats('supplementTo shape', supplementTo.keySet())
            
            i.remove()
            hasSeries.addAll(serials.collect{['@id': it.'@id']})
            bib.scheduleSave()
        }
    }
    
    if (hasSeries) {
        thing.hasSeries = hasSeries as List
        if (!thing.supplementTo) {
            thing.remove('supplementTo')
        }
    }
}

List tidningSerialThings(Map supplementTo) {
    List controlNumbers = getAtPath(supplementTo, ['describedBy', '*', 'controlNumber'], [])
    def result = controlNumbers.collect{ tidningSerialThings(it) }.flatten()
    if (result && controlNumbers.size() > 1) {
        incrementStats('multiple controlnumbers', supplementTo)
        return []
    }
    return result
}

@Memoized
List tidningSerialThings(String controlNumber) {
    def thing = loadThing(controlNumberToId(controlNumber))
    isTidningSerial(thing) 
            ? [thing]
            : []
}

static boolean isTidningSerial(Map thing) {
    thing.issuanceType == 'Serial' && getAtPath(thing, ['instanceOf', 'genreForm', '*', '@id'], [])
            .any { it == 'https://id.kb.se/term/saogf/Dagstidningar' || it == 'https://id.kb.se/term/saogf/Tidskrifter' }
}

static def controlNumberToId(String controlNumber) {
    def isXlId = controlNumber.size() > 14
    isXlId
        ? controlNumber
        : 'http://libris.kb.se/resource/bib/' + controlNumber
}

// --------------------------------------------------------------------------------------

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

static Object getAtPath(item, Iterable path, defaultTo = null) {
    if(!item) {
        return defaultTo
    }

    for (int i = 0 ; i < path.size(); i++) {
        def p = path[i]
        if (p == '*' && item instanceof Collection) {
            return item.collect { getAtPath(it, path.drop(i + 1), []) }.flatten()
        }
        else if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}

static List asList(o) {
    return (o instanceof List) ? (List) o : o != null ? [o] : []
}