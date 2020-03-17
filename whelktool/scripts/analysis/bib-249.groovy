import whelk.util.DocumentUtil
import whelk.util.Statistics

s = new Statistics().printOnShutdown()

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
    def (record, thing, work) = bib.graph

    if(!work) {
        return
    }

    if(thing['marc:hasBib249']) {
        boolean marcTrl = work['marc:languageNote'] == "marc:ItemIsOrIncludesATranslation"

        String hasTitle = hasTitle(thing, work)

        if (hasTitle == "diff") {
            println ("""
            ${bib.doc.getURI()}
            ${thing['marc:hasBib249']}
            marc:ItemIsOrIncludesATranslation ${marcTrl}
            ${work.hasTitle}
            """.stripIndent())
        }

        s.increment('hasTitle', hasTitle)
        s.increment('shape', maybeList(thing['marc:hasBib249']) { map -> new TreeSet(map.keySet()) })
        s.increment('marc:ItemIsOrIncludesATranslation', "${marcTrl}")
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

String trim(String s) {
    // remove leading and trailing non-"alpha, digit or parentheses"
    def w = /\(\)\p{IsAlphabetic}\p{Digit}/
    def m = s =~ /[^${w}]*([${w}- ]*[${w}])[^${w}]*/
    return m.matches() ? m.group(1) : s
}