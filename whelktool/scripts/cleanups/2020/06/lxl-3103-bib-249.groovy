import whelk.util.DocumentUtil
import whelk.util.Statistics
import org.apache.commons.lang3.StringUtils

//s = new Statistics().printOnShutdown()
class Script {
    static Map MAP_249 = [
            'marc:originalTitle' : 'mainTitle',
            'marc:titleRemainder': 'subtitle',
            'marc:titleNumber'   : 'partNumber',
            'marc:titlePart'     : 'partName',
            'marc:nonfilingChars': 'marc:nonfilingChars'
    ]

    static List TITLE_COMPONENTS = ['mainTitle', 'titleRemainder', 'subtitle', 'hasPart', 'partNumber', 'partName', 'marc:parallelTitle', 'marc:equalTitle']
}
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

    List _249s = listify(instance['marc:hasBib249'])

    if(_249s) {
        StringBuilder msg = new StringBuilder()
        msg.append(bib.doc.shortId).append('\n')
        msg.append(_249s).append('\n')

        def converted = filterAndConvert(_249s, work, msg)

        if (converted.size() == 1) {
            def title = converted.first()
            work['hasTitle'] = listify(work['hasTitle'])
            if (work['hasTitle'].any{ it['@type'] == 'MainTitle' }) {
                title['@type'] = 'VariantTitle'
            }

            work['hasTitle'].add(title)

            msg.append("--> $title\n")
            msg.append("--> work['hasTitle']: ${work['hasTitle']}\n")
        }
        else if (converted.size() > 1) {
            if(work['hasPart']) {
                throw new RuntimeException('Already hasPart')
            }
            work['hasPart'] = converted.collect {
                [
                        '@type': 'Work',
                        'hasTitle': it
                ]
            }
            msg.append("work['hasPart']: ${work['hasPart']}\n")
        }

        msg.append('\n')
        println(msg.toString())
    }
}

List filterAndConvert(List _249s, Map work, StringBuilder msg) {
    def workTitles = [:]
    DocumentUtil.findKey(work, 'hasTitle', { titles, path ->
        listify(titles).eachWithIndex { title, i ->
            workTitles.put((path + i).join(', '), title)
        }
    })

    _249s.findResults { t ->
        def c = convert249(t)

        def matches = workTitles.findAll { path, title ->
            comparisonTitle(title) == comparisonTitle(c)
        }

        if (!matches.isEmpty()) {
            msg.append("$t ${comparisonTitle(c)}) already exists: $matches, dropping ")
            return null
        }
        else {
            return c
        }
    }
}

Map convert249(Map bib249) {
    Map<String, String> result = ['@type': 'MainTitle']
    Script.MAP_249.each { k, v ->
        if (bib249[k]) {
            result[v] = bib249[k]
        }
    }
    
    if (result['mainTitle'] && result['subtitle']) {
        result['mainTitle'] = stripSuffix(result['mainTitle'], ': ')
    }
    result['mainTitle'] = stripSuffix(result['mainTitle'], '.')

    if (result['marc:nonfilingChars'] == "0") {
        result.remove('marc:nonfilingChars')
    }
    return result
}

static String stripSuffix(String s, String suffix) {
    if (s.endsWith(suffix)) {
        s.substring(0, s.length() - suffix.length())
    }
    else {
        s
    }
}

String comparisonTitle(Map title) {
    normalize(Script.TITLE_COMPONENTS.findResults { title.get(it, null) }.join(' '))
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