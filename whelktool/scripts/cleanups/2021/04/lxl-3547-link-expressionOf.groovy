/**
 * Try to replace local instanceOf.expressionOf with links to "uniformWorkTitle" works.
 * 
 * originDate, marc:version and title (mainTitle, partName, partNumber, marc:formSubheading) have to match.
 * expressionOf with language can link to work with same language or no language
 * expressionOf without language can link to work without language
 * 
 * See LXL-3547 for more info
 * 
 * TODO: decide which non-linkable expressionOf should be extracted to new works and linked, i.e. how many identical expressionOf have to exist 
 */
import org.apache.commons.lang3.StringUtils
import whelk.util.Unicode
import whelk.Document
import java.text.Normalizer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.regex.Pattern

PrintWriter linked = getReportWriter("linked.txt")
PrintWriter noLang = getReportWriter("no-lang.txt")
PrintWriter multiMatch = getReportWriter("multiple-matches.txt")
PrintWriter sameExpr = getReportWriter("same-expr.txt")

languageNames = loadLanguageNames()
uniformWorks = [:] //getUniformWorks()

def q = [ // this query doesn't find everything since expressionOf is normally not indexed
        '@type'                                            : ['Instance'],
        'exists-instanceOf.expressionOf.hasTitle.mainTitle': ['true'],
        'exists-instanceOf.expressionOf.@id'               : ['false'],
        '_sort'                                            : ['@id'],
]

def notLinkedExpr = new ConcurrentHashMap<Map, ConcurrentLinkedQueue<String>>()
selectByIds(queryIds(q).collect()) { bib -> 
//selectByCollection('bib') { bib ->
    Map work = getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf'])
    List<Map> expressionOf = asList(getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'expressionOf']))

    if (!work || !expressionOf) {
        return
    }
    
    boolean modified = false
    expressionOf.each { Map e -> 
        if (e['@id']) {
            return
        }

        incrementStats('shape', new TreeSet<>(e.keySet()))
        
        if (!e['hasTitle']) {
            return
        }

        // there is always exactly one title
        def cmpMap = Norm.cmpTitle(asList(e['hasTitle']).first(), languageNames) + Norm.cmpOther(e)
        getPrimaryContributionString(work)?.with {cmpMap += ['primaryContribution': it] }
        def works = uniformWorks.getOrDefault(cmpMap, Collections.emptyList())
        
        incrementStats('number of works found for expressionOf', works.size())
        
        if (works) {
            def worksLang = compatibleLanguages(e, works)
            incrementStats('number of works found for expressionOf, filtered on lang', worksLang.size())

            if (worksLang.size() == 0) {
                noLang.println("${bib.doc.id} ${toString(e)} ??? ${works.collect {it['@id'] + " " + toString(it)}}")
            }
            else if (worksLang.size() == 1) {
                linked.println("${bib.doc.id} ${toString(e)} --> ${worksLang[0].with {it['@id'] + " " + toString(it)}}")
                e.clear()
                e['@id'] = worksLang[0]['@id']
                modified |= true
            }
            else {
                multiMatch.println("${bib.doc.id} ${toString(e)} ??? ${worksLang.collect {it['@id'] + " " + toString(it)}}")
            }
        }
        else {
            Map ee = Document.deepCopy(e)
            getPrimaryContributionString(work)?.with {ee += ['primaryContribution': it] }
            notLinkedExpr.computeIfAbsent(ee, { new ConcurrentLinkedQueue<String>() })
            notLinkedExpr.get(ee).add(bib.doc.shortId)
        }
    }
    
    if (modified) {
        bib.scheduleSave()
    }
}

List<String> uniqueUnmatchedIds = []
notLinkedExpr.each {key, ids ->
    ids.each {
        incrementStats('same expr', toString(key), it)
    }
    sameExpr.println(ids.size() + " " + toString(key))
    if (ids.size() == 1) {
        uniqueUnmatchedIds.add(ids.poll())
    }
}



Collection<Map> compatibleLanguages(Map expressionOf, Collection<Map> works) {
    if (lang(expressionOf)) {
        def sameLang = works.findAll{ lang(it) == lang(expressionOf)}
        return sameLang ?: works.findAll{ lang(it) == Collections.emptySet() }
    }
    else {
        return works.findAll{ lang(it) == Collections.emptySet() }
    }
}


private Map<String, Collection<Map>> getUniformWorks() {
    def q = [
            'inCollection.@id': ['https://id.kb.se/term/uniformWorkTitle'], 
            '_sort': ['@id']
    ]
    
    def works = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Map>>()
    selectByIds(queryIds(q).collect()) { doc ->
        Map work = getPathSafe(doc.doc.data, ['@graph', 1])
        
        def titles = (asList(work.hasTitle) + asList(work.hasVariant).collect{ asList(it['hasTitle']) }.flatten())
        titles = titles.collect{ Norm.cmpTitle(it, languageNames) }
        
        def otherFields = Norm.cmpOther(work)
        getPrimaryContributionString(work)?.with {otherFields += ['primaryContribution': it] }
        
        def cmpMaps = titles.collect{otherFields + it} as Set
        cmpMaps.each { map ->
            works.computeIfAbsent(map, { new ConcurrentLinkedQueue<Map>() })
            works.get(map).add(work)
        }
        
    }
    return works
}

private Set<String> loadLanguageNames() {
    def q = [
            '@type': ['Language'],
            '_sort': ['@id']
    ]

    def languages = Collections.synchronizedSet(new HashSet<String>())
    selectByIds(queryIds(q).collect()) { d ->
        def (record, thing) = d.graph
        thing.prefLabelByLang?.sv?.with { String label ->
            languages.add(label.toLowerCase())
        }
    }
    return languages
}

private String toString(Map work) {
    def keys = (Norm.TITLE_KEYS.collect {['hasTitle', 0] + it } + Norm.CMP_KEYS) 
    def props = keys.collect {getPathSafe(work, asList(it)) }
    def langcode = lang(work).collect{ (it['@id'] ?: '').split('/').last() }
    def contribution = asList(work['primaryContribution']) + asList(getPrimaryContributionString(work))
    return (props + langcode + contribution).grep().join(' · ')
}

private Object getPathSafe(item, path, defaultTo = null) {
    for (p in path) {
        if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}

private Set lang(Map work) {
    (asList(work.language) + asList(work.associatedLanguage)) as Set
}

private String getPrimaryContributionString(Map work) {
    List<Map> a = asList(work.contribution).find { it['@type'] == 'PrimaryContribution' }?.with { return asList(it.agent) }
    if (a) {
        def agent = a.first()
        if (agent['@id']) {
            agent = loadThing(agent['@id'])
        }
        
        StringBuilder b = new StringBuilder()
        for (def k : ['name', 'givenName', 'familyName']) {
            if (agent.containsKey(k)) {
                b.append(agent[k]).append(' ')
            }
        }
        return (Norm.normalize(b.toString()))
    }
    return null
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

class Norm {
    static final Pattern UNICODE_MARK = Pattern.compile('\\p{M}')

    // these are the ones found in the data. ignore @type and marc:nonfilingChars
    static final Set TITLE_KEYS = ['mainTitle', 'partName', 'partNumber', 'marc:formSubheading'] as Set

    static final Set CMP_KEYS = ['originDate', 'marc:version'] as Set 
    
    static def noise = [",", '"', "'", '[', ']', ',', '.', '.', ':', ';', '-', '(', ')', ' the ', '-', '–', '+', '!', '?'].collectEntries { [it, ' '] }

    static Map cmpOther(Map work) { 
        normalize(work.findAll {it.key in CMP_KEYS})
    }
    
    static Map cmpTitle(Map title, Set<String> languageNames) {
        def t = normalize(title.findAll {it.key in TITLE_KEYS})
        
        // Some records have the language in the title e.g. "Tusen och en natt. Svenska" drop the language part
        t.mainTitle?.with { String m ->
            def s = m.split(' ')
            if (s.last() in languageNames) {
                t.mainTitle = normalize(s.dropRight(1).join(' '))
                println(m)
                println(t.mainTitle)
            }
        }
        
        return t
    }
    
    static Map normalize(Map map) {
        map
                .collectEntries {
                    [(it.key): it.value instanceof List
                            ? ((List) it.value).collect{ normalize(it) }.sort()
                            :  normalize(it.value)
                    ]
                }
    }
    
    static String normalize(String s) {
        s = " $s "
        s = toLowerCase(s)
        s = s.replace(noise)
        s = s.replace('æ', 'ae')
        s = StringUtils.normalizeSpace(s)
        s = Unicode.normalizeForSearch(s)
        s = asciiFold(s)
        return s
    }

    static String toLowerCase(String s) {
        // Don't touch abbreviations, e.g. LOU
        boolean allUpperCase = s.toUpperCase(Locale.ROOT) == s
        return allUpperCase ? s : s.toLowerCase(Locale.ROOT)
    }
    
    static String asciiFold(String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll(UNICODE_MARK, '')
    }
}

/*
broken, fix manually:
   1 [@type, language, mainTitle]                                 [p0bg9szbmd1sv3wq]
   1 [@type, mainTitle, qualifier, marc:nonfilingChars]           [nzjzj9mqln9q8mvl]
   1 [@type, subtitle, mainTitle, marc:formSubheading]            [2btjtjz00hlpvk19]
   1 [@type, subtitle, mainTitle]                                 [8m6b5z206blc8p4n]
 */