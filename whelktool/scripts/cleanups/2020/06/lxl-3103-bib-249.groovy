import whelk.util.DocumentUtil
import whelk.util.Statistics
import org.apache.commons.lang3.StringUtils

//s = new Statistics().printOnShutdown()
Map MAP_249 = [
        'marc:originalTitle': 'mainTitle',
        'marc:titleRemainder': 'subtitle',
        'marc:titleNumber': 'partNumber',
        'marc:titlePart': 'partName',
        'marc:nonfilingChars': 'marc:nonfilingChars'
]


selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println(e)
        e.printStackTrace()
    }

}

void process(bib) {
    def (_record, instance) = bib.graph
    def work = getWork(bib)

    if(!work) {
        return
    }

    def _249s = listify(instance['marc:hasBib249'])

    if(_249s) {
        println("${bib.doc.shortId}\t$_249s")
    }
}

String hasTitle(thing, work) {
    if (work.hasTitle) {
        isSameTitle(thing, work) ? "match" : "diff"
    }
    else {
        "no"
    }
}

boolean isSameTitle(def thing, def work) {



    String t = getPathSafe(thing, ['marc:hasBib249', 'marc:originalTitle'], "TT")
    String w = getPathSafe(work, ['hasTitle', 0, 'mainTitle'], "WT")
    trim(w.toLowerCase()) == trim(t.toLowerCase())
}

String comparisonTitle(Map title) {
    
}

Object maybeList(Object o, Closure c) {
    o instanceof List
            ? o.collect(c)
            : c(o)
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

private static List listify(Object o) {
    (o ?: []).with { it instanceof List ? it : [it] }
}

String normalize(String s) {
    StringUtils.normalizeSpace(s.replaceAll(/[^\p{IsAlphabetic}\p{Digit} ] /, '').toLowerCase().trim())
}

Map getWork(def bib) {
    def (record, thing, work) = bib.graph
    if(thing && thing['instanceOf'] && thing['instanceOf']['@type']) {
        return thing['instanceOf']
    }
    else if (work) {
        return work
    }
    return null
}