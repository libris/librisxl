/**
 *
 */
import org.apache.commons.lang3.StringUtils
import whelk.util.Unicode
import java.text.Normalizer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.regex.Pattern


PrintWriter linked = getReportWriter("linked.txt")
PrintWriter noLang = getReportWriter("no-lang.txt")
PrintWriter multiMatch = getReportWriter("multiple-matches.txt")
PrintWriter sameExpr = getReportWriter("same-expr.txt")

w = works()

def q = [
        '@type'                                            : ['Instance'],
        'exists-instanceOf.expressionOf.hasTitle.mainTitle': ['true'],
        'exists-instanceOf.expressionOf.@id'               : ['false'],
        '_sort'                                            : ['@id'],
]

def expr = new ConcurrentHashMap<Map, ConcurrentLinkedQueue<Map>>()
selectByIds(queryIds(q).collect()) { bib -> 
    List<Map> expressionOf = asList(getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'expressionOf']))

    if (!expressionOf) {
        return
    }
    
    expressionOf.each { Map e -> 
        if (e['@id']) {
            return
        }

        def expressionLang = asList(e['language']) as Set
        def workLang = asList(getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'language'])) as Set
        if (expressionLang != workLang) {
            //incrementStats(workLang.toString(), expressionLang.toString())
        }

        // there is always exactly one title
        def cmpMap = Norm.cmpTitle(asList(e['hasTitle']).first()) + Norm.cmpOther(e)
        def works = w.getOrDefault(cmpMap, Collections.emptyList())
        
        incrementStats('number of works found for expressionOf', works.size())
        
        if (works) {
            def worksLang = filterOnLang(e, works)
            incrementStats('number of works found for expressionOf, filtered on lang', worksLang.size())

            if (worksLang.size() == 0) {
                noLang.println("${bib.doc.id} ${asString(e)} ??? ${works.collect {it['@id'] + " " + asString(it)}}")
            }
            else if (worksLang.size() == 1) {
                linked.println("${bib.doc.id} ${asString(e)} --> ${worksLang[0].with {it['@id'] + " " + asString(it)}}")
            }
            else {
                multiMatch.println("${bib.doc.id} ${asString(e)} ??? ${worksLang.collect {it['@id'] + " " + asString(it)}}")
            }
        }
        else {
            def withLang = cmpMap + ['language': e['language']]
            expr.computeIfAbsent(withLang, { new ConcurrentLinkedQueue<Map>() })
            expr.get(withLang).add(bib.doc.shortId)
        }
    }
}

expr.each {key, value ->
    value.each {
        incrementStats('same expr', asString(key, true), it)
    }
    sameExpr.println(value.size() + " " + asString(key, true))
}

Collection<Map> filterOnLang(Map expr, Collection<Map> works) {
    if (lang(expr)) {
        def sameLang = works.findAll{ lang(it) == lang(expr)}
        return sameLang ?: works.findAll{ lang(it) == Collections.emptySet() }
    }
    else {
        return works.findAll{ lang(it) == Collections.emptySet() }
    }
}


private Map<String, Collection<Map>> works() {
    def works = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Map>>()
    
    selectByIds(queryIds(['inCollection.@id': ['https://id.kb.se/term/uniformWorkTitle'], '_sort': ['@id']]).collect()) { doc ->
        Map work = getPathSafe(doc.doc.data, ['@graph', 1])

        def titles = (asList(work.hasTitle) + asList(work.hasVariant).collect{asList(it['hasTitle'])}.flatten()).collect(Norm.&cmpTitle)
        def otherFields = Norm.cmpOther(work)
        def cmpMaps = titles.collect{otherFields + it} as Set
        cmpMaps.each { map ->
            works.computeIfAbsent(map, { new ConcurrentLinkedQueue<Map>() })
            works.get(map).add(work)
        }
    }
    return works
}

private String asString(Map work, flat = false) {
    (((flat ? Norm.TITLE_KEYS : Norm.TITLE_KEYS.collect {['hasTitle', 0] + it }) + Norm.CMP_KEYS).collect {
        getPathSafe(work, asList(it))
    } + lang(work).collect{ (it['@id'] ?: '').split('/').last() }).grep().join(' · ')
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

class Norm {
    static final Pattern UNICODE_MARK = Pattern.compile('\\p{M}')

    // these are the ones found in the data. ignore @type and marc:nonfilingChars
    static final Set TITLE_KEYS = ['mainTitle', 'partName', 'partNumber', 'marc:formSubheading'] as Set

    static final Set CMP_KEYS = ['originDate', 'marc:version'] as Set 
    
    static def noise = [",", '"', "'", '[', ']', ',', '.', '.', ':', ';', '-', '(', ')', ' the ', '-', '–', '+', '!', '?'].collectEntries { [it, ' '] }

    static Map cmpOther(Map work) { 
        normalize(work.findAll {it.key in CMP_KEYS})
    }
    
    static Map cmpTitle(Map title) {
        normalize(title.findAll {it.key in TITLE_KEYS})
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

    static String asciiFold(String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll(UNICODE_MARK, '')
    }
    
    static String normalize(String s) {
        return asciiFold(Unicode.normalizeForSearch(StringUtils.normalizeSpace(" $s ".toLowerCase().replace(noise))))
    }
}


/*
broken, fix manually:
   1 [@type, language, mainTitle]                                 [p0bg9szbmd1sv3wq]
   1 [@type, mainTitle, qualifier, marc:nonfilingChars]           [nzjzj9mqln9q8mvl]
   1 [@type, subtitle, mainTitle, marc:formSubheading]            [2btjtjz00hlpvk19]
   1 [@type, subtitle, mainTitle]                                 [8m6b5z206blc8p4n]
 */
