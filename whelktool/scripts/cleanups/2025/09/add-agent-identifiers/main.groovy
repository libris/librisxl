import groovy.xml.XmlSlurper
import whelk.util.Jackson
import whelk.util.Unicode

Map isniData(var record) {
    // TODO? remove '..' etc
    var names = [] as Set
    record.ISNIMetadata.identity.personOrFiction.personalName.each {
        names.add([
                givenName: it.forename.text(),
                familyName: it.surname.text()
        ])
    }
    record.ISNIMetadata.identity.personOrFiction.personalNameVariant.each {
        names.add([
                givenName: it.forename.text(),
                familyName: it.surname.text()
        ])
    }

    var lifeSpans = record.ISNIMetadata.identity.personOrFiction.personalName.marcDate*.text() as Set
    lifeSpans += record.ISNIMetadata.identity.personOrFiction.personalNameVariant.marcDate*.text() as Set

    // TODO subTitle ??
    var titles = record.ISNIMetadata.identity.personOrFiction.creativeActivity.titleOfWork.title*.text() as Set
    // @ is used for "non-filing chars"
    // e.g. "Le @mole St. Nicolas dans l'isle de St. Domingue, vu du mouillage"
    titles = titles.collect { it.contains("@") ? it.split("@")[1] : it }.collect { it.replace("\\s+", " ") }.toSorted()

    var subTitles = record.ISNIMetadata.identity.personOrFiction.creativeActivity.titleOfWork.subtitle*.text() as Set

    var isbns = record.ISNIMetadata.identity.personOrFiction.creativeActivity.identifier.findAll { it.identifierType.text() == 'ISBN' }.identifierValue*.text() as Set

    ///srw:searchRetrieveResponse/srw:records/srw:record/srw:recordData/responseRecord/ISNIAssigned/ISNIMetadata/sources/reference/URI
    // kan hämta birthDate / deatDate från VIAF RDF
    var uris = record.ISNIMetadata.sources.reference.URI*.text()

    return [
            isni: record.isniUnformatted,
            isniUri: record.isniURI,
            names: names,
            lifeSpans: lifeSpans,
            titles: titles,
            isbns: isbns,
            uris: uris,
    ]
}

List<Map> fromIsni(var fileOrUrl) {
    def slurp = new XmlSlurper(false, false)
    var root = slurp.parse(fileOrUrl)
    // TODO check num results
    var records = root['srw:records']['*']['srw:recordData']['responseRecord']['ISNIAssigned']
    return records.collect{ isniData(it) }
}

void fromViaf(var fileOrUrl, var id) {
    def slurp = new XmlSlurper(false, false)
    var root = slurp.parse(fileOrUrl)
    root = root.declareNamespace([rdf:''])
    var descriptions = root.'*'.findAll {it.attributes()['rdf:about']?.startsWith(id)}

    var titles = descriptions['dbo:notableWork']['rdf:Description']['rdfs:label']*.text() as Set
    titles.each { println(it) }

    var sameAs = descriptions['schema:sameAs']['rdf:Description'].collect{ it.attributes()['rdf:about'] }
    println(sameAs)
}

//var VIAF_XML = new File('/home/OloYli/.config/JetBrains/IntelliJIdea2025.2/scratches/scratch_121.xml')
//fromViaf(VIAF_XML, 'http://viaf.org/viaf/307306320')

//fromViaf(new File('/home/OloYli/.config/JetBrains/IntelliJIdea2025.2/scratches/scratch_122.xml'), 'http://viaf.org/viaf/110510837')

static boolean allWordsMatch(String a, String b) {
    var words = (String s) -> Unicode.removeDiacritics(s.toLowerCase()).split("\\s").findAll{it != "" }.collect { Unicode.trimNoise(it) } as Set
    return words(a) == words(b)
}

boolean namesMatch(Map needle, Map haystack) {
    if (haystack.givenName) {
        allWordsMatch(haystack.givenName, needle.givenName) && allWordsMatch(haystack.familyName, needle.familyName)
    } else {
        allWordsMatch(haystack.familyName, (needle.familyName ?: "") + " " + (needle.givenName ?: ""))
    }
}

boolean titlesMatch(String needle, String haystack) {
    return allWordsMatch(haystack, needle)
}

def intersect(a, b, fn) {
    return a.findAll{ aa -> b.any{ bb -> fn(aa, bb) } }
}

enum Match {
    NO,
    NAME_AND_LIFESPAN,
    AND_WORK
}

def map(def agent) {
    var name = agent.names.first()
    var nameKeywords = ((name.givenName ?: "") + " " + (name.familyName ?: "")).replace(" ", "+")

    var yearOfBirth =  agent.lifeSpan.split('-').first()
    var ISNI_SRU="https://isni.oclc.org/sru/?version=1.1&operation=searchRetrieve&stylesheet=https%3A%2F%2Fisni.oclc.org%2Fcbs%2Fsru%2FDB%3D1.2%2F%3Fxsl%3DsearchRetrieveResponse&recordSchema=isni-b&maximumRecords=10&startRecord=1&recordPacking=xml&sortKeys=RLV%2Cpica%2C0%2C%2C&x-info-5-mg-requestGroupings=none"
    var query = "pica.nw+%3D+%22${nameKeywords}%22+and+pica.yob+%3D+%22${yearOfBirth}%22"
    //println(query)
    var isniSru = ISNI_SRU + "&query=" + query

    var candidates = fromIsni(isniSru)

    var matches = candidates.collect {c ->
        [
                c: c,
                matchingNames: intersect(c.names, agent.names, { a, b -> namesMatch(a, b) }),
                matchingLifeSpans: intersect(c.lifeSpans, [agent.lifeSpan], { a, b -> a == b }),
                matchingTitles: intersect(c.titles, agent.titles, this.&titlesMatch),
                matchingIsbns: intersect(c.isbns, agent.isbns, { a, b -> a == b }),
        ]
    }.each { m ->
        m.match = m.matchingNames.size() > 0 && m.matchingLifeSpans.size() > 0
                ? m.matchingTitles.size() > 0 || m.matchingIsbns.size()
                ? Match.AND_WORK
                : Match.NAME_AND_LIFESPAN
                : Match.NO
    }

    return matches
}

var json = new File(scriptDir, 'build/candidates-nonsw-1.json')
var agentsIn = Jackson.mapper.readValue(json, Map)

var toName = m -> [familyName: m.family ?: "", givenName: m.given ?: ""]
var agents = agentsIn.collect { k, v ->
    [
            id: k,
            names: [toName(v)] + asList(v.variant).collect{ toName(it) },
            lifeSpan: v.lifespan,
            titles: asList(v.work),
            isbns: asList(v.isbn),
    ]
}.findAll {
    it.names.size() > 0 && it.lifeSpan
}

for (var agent in agents) {
    try {
        println("")
        var matches = map(agent)
        println(agent.names.first())
        println(agent.id)
        matches.sorted { m1, m2 ->
            case (m1.match) {
                case NO -> -1
                case NAM
            }
        }.each {
            println("${it.match} ${it.c.isniUri}")
        }

        println("==============================================================")
        sleep(1000)
    }
    catch (Exception e) {
        e.printStackTrace()
    }
}


//map(agent)


/*
VIAF

< x-ratelimit-remaining-month: 9998
< ratelimit-limit: 1000
< x-ratelimit-limit-month: 10000
< x-ratelimit-remaining-day: 998
< ratelimit-remaining: 998
< x-ratelimit-limit-day: 1000

curl -v -L -H 'Accept: application/rdf+xml' 'https://viaf.org/viaf/308770766

re:redirect
curl -v -L -H 'Accept: application/rdf+xml' 'https://viaf.org/viaf/308770766'


dubblett i ISNI
https://isni.org/isni/0000000434649457
https://isni.org/isni/0000000429226361
 */