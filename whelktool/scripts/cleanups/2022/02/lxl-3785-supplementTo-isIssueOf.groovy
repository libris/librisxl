/* 
In order to only check new records specify file where timestamp of last run is saved with:
-Dlast-run-file=</path/to/file>

(Schedule this script to run continuously until Mimer MODS conversion is fixed)

***********************************************************************************************************************

Clean up newspaper (dagstidningar + tidskrifter) shapes.
Link digitized newspaper monographs (issues) to their series. That is, replace supplementTo and/or isPartOf with isIssueOf.

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
kbv:isIssueOf <https://libris.kb.se/m5z2w4lz3m2zxpk#it> ;
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

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

noMarcGf = getReportWriter("no-marc-gf.txt")
otherMarcGf = getReportWriter("other-marc-gf.txt")
notMimer = getReportWriter("not-mimer.txt")

String now = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

String lastRunOpt = System.getProperty('last-run-file', '')
def lastRunTimestamp = new File(lastRunOpt).with { if (it.exists()) it.readLines()?.first() }
println("Using lastRunTimestamp: ${lastRunTimestamp ?: '<NO>'}")

def where = """
    collection = 'bib'
    AND deleted = 'false'
    AND data#>>'{@graph,1,@type}' = 'Electronic'
    AND (data#>>'{@graph,1,supplementTo}' IS NOT NULL OR data#>>'{@graph,1,isPartOf}' IS NOT NULL)
    ${lastRunTimestamp ? "AND created >= '$lastRunTimestamp'" : ''}
"""

selectBySqlWhere(where) { bib ->
    def (record, thing) = bib.graph
    
    if (!thing.supplementTo && !thing.isPartOf) {
        return
    }

    if (!isMimerRecord(thing.'@id')) {
        notMimer.println(thing.'@id')
        return
    }
    
    if(marcGf(thing).with { it && it.any {l -> l != 'issue'}}) {
        otherMarcGf.println("${thing.'@id'}\t${marcGf(thing)}")
        return
    }
    
    def isIssueOf = asList(thing.isIssueOf) as Set
    
    def i = ((List) thing.supplementTo).iterator()
    while (i.hasNext()) {
        Map supplementTo = (Map) i.next()
        def serials = tidningSerialThings(supplementTo)
        serials = verifyTitle(thing, serials, supplementTo)
        if (serials) {
            incrementStats('supplementTo', supplementTo)
            incrementStats('supplementTo shape', supplementTo.keySet())
            
            i.remove()
            isIssueOf.addAll(serials.collect{['@id': it.'@id']})
            serials.each {
                incrementStats('supplementTo linked', "${serialTitle(it.'@id')} - ${issueTitlesNoDate(thing).join(' · ')}")
            }
            incrementStats('isMarcGfIssue', isMarcGfIssue(thing))
            if (!isMarcGfIssue(thing)) {
                noMarcGf.println("${bib.doc.shortId}\t${issueTitlesNoDate(thing)}")
            }
            bib.scheduleSave()
        }
    }

    i = ((List) thing.isPartOf).iterator()
    while (i.hasNext()) {
        Map isPartOf = (Map) i.next()
        def serials = tidningSerialThings(isPartOf)
        serials = verifyTitle(thing, serials, isPartOf)
        if (serials) {
            incrementStats('isPartOf', isPartOf)
            incrementStats('isPartOf shape', isPartOf.keySet())

            i.remove()
            isIssueOf.addAll(serials.collect{['@id': it.'@id']})
            serials.each {
                incrementStats('isPartOf linked', "${serialTitle(it.'@id')} - ${issueTitlesNoDate(thing).join(' · ')}")
            }
            incrementStats('isMarcGfIssue', isMarcGfIssue(thing))
            if (!isMarcGfIssue(thing)) {
                noMarcGf.println("${bib.doc.shortId}\t${issueTitlesNoDate(thing)}")
            }
            bib.scheduleSave()
        }
    }
    
    
    if (isIssueOf) {
        thing.isIssueOf = isIssueOf as List
        if (!thing.supplementTo) {
            thing.remove('supplementTo')
        }
        if (!thing.isPartOf) {
            thing.remove('isPartOf')
        }
    }
}

if (lastRunOpt) {
    new File(lastRunOpt).withWriter { writer ->
        writer.write(now)
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
    def tidningGf = [
            'https://id.kb.se/term/saogf/Dagstidningar',
            'https://id.kb.se/term/saogf/Periodika',
            'https://id.kb.se/marc/Newspaper',
            'https://id.kb.se/marc/Periodical'
    ]
    
    thing.issuanceType == 'Serial' && getAtPath(thing, ['instanceOf', 'genreForm', '*', '@id'], [])
            .any { String gf -> gf in tidningGf }
}

static def controlNumberToId(String controlNumber) {
    def isXlId = controlNumber.size() > 14
    isXlId
        ? controlNumber
        : 'http://libris.kb.se/resource/bib/' + controlNumber
}

static boolean isMarcGfIssue(Map thing) {
    getAtPath(thing, ['instanceOf', 'genreForm', '*', 'prefLabel'], []).any{ it == 'issue'}
}

static List<String> marcGf(Map thing) {
    getAtPath(thing, ['instanceOf', 'genreForm', '*'], []).findAll {
        getAtPath(it, ['inScheme', '@id']) == 'https://id.kb.se/term/marcgt'
    }.collect{ it.prefLabel }
}

List verifyTitle(Map thing, List<Map> serials, Map reference) {
    def titles = { Map t ->
        getAtPath(t, ['hasTitle', '*', 'mainTitle'], []).collect { String title -> title.toLowerCase() }
    }
    
    def referenceTitles = titles(reference)
    
    if (!referenceTitles) {
        return serials
    }

    // A lot of records have the "issue title" here 
    // e.g. MARIESTADSTIDNINGEN 2022-01-21 supplementTo.hasTitle.mainTitle MARIESTADSTIDNINGEN
    def issueTitles = issueTitlesNoDate(thing).collect { String title -> title.toLowerCase() }
    return serials.findAll {
        def serialTitles = titles(it) + issueTitles
        if (serialTitles.intersect(referenceTitles)) {
            return true
        }
        else {
            incrementStats('bad title', "$referenceTitles -> $serialTitles")
            return false
        }
    }
}

@Memoized
String serialTitle(String id) {
    def thing = loadThing(id)
    def title = getAtPath(thing, ['hasTitle', '*', 'mainTitle'], []).join(' · ')
    def shortId = id.split('/').last().split('#').first()
    return "$title ($shortId)"
}

List<String> issueTitlesNoDate(Map thing) {
    getAtPath(thing, ['hasTitle', '*', 'mainTitle']).collect(this::stripDate)
}

def stripDate(String title) {
    def PATTERN = /^(.*)\s+(\d\d\d\d-\d\d\-\d\d|\(\d\d\d\d\):\d+)$/

    def stripped = (title =~ PATTERN).with { matches() ? it[0][1] : title }
    return stripped
}

boolean isMimerRecord(String id) {
    def where = """
        data #>> '{@graph,1,itemOf,@id}' = '$id' 
        AND data #>> '{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/APIS'
    """
    
    boolean result = false
    selectBySqlWhere(where) {
        result = true
    }
    return result
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