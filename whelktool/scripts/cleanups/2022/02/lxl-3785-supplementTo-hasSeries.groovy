/* 
Analysis of shapes for:

Link digitized newspaper (dagstidningar + tidskrifter) monographs (issues) to their series
Replace supplementTo with hasSeries

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

*/
import groovy.transform.Memoized

def where = """
    data#>>'{@graph,1,supplementTo}' IS NOT NULL
    AND collection = 'bib'
    AND deleted = 'false'
"""

selectBySqlWhere(where) { bib ->
    def (record, thing) = bib.graph
    
    thing.supplementTo?.each { Map s ->
        if (tidningSerialReferences(s)) {
            incrementStats('supplementTo', s)
            incrementStats('supplementTo shape', s.keySet())
        }
        
    }
}

List tidningSerialReferences(Map supplementTo) {
    List controlNumbers = getAtPath(supplementTo, ['describedBy', '*', 'controlNumber'], [])
    def result = controlNumbers.collect{ tidningSerialReferences(it) }.flatten()
    if (result && controlNumbers.size() > 1) {
        incrementStats('multiple controlnumbers', supplementTo)
    }
    return result
}

@Memoized
List tidningSerialReferences(String controlNumber) {
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