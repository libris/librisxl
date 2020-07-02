import whelk.util.DocumentUtil
import whelk.util.Statistics
import org.apache.commons.lang3.StringUtils

import java.text.Normalizer

class Script {
    static Map MAP_249 = [
            'marc:originalTitle' : 'mainTitle',
            'marc:titleRemainder': 'subtitle',
            'marc:titleNumber'   : 'partNumber',
            'marc:titlePart'     : 'partName',
            'marc:nonfilingChars': 'marc:nonfilingChars'
    ]
    static Statistics s = new Statistics().printOnShutdown()
    static List TITLE_COMPONENTS = ['mainTitle', 'titleRemainder', 'subtitle', 'hasPart', 'partNumber', 'partName', 'marc:parallelTitle', 'marc:equalTitle']
  //  PrintWriter failedUpdating
}
//Script.failedUpdating = getReportWriter("failed-updates")


selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println("${bib.doc.shortId} $e")
        if (e.getMessage() != "Already hasPart") {
            e.printStackTrace()
        }
    }

}

void process(bib) {
    def (_record, instance) = bib.graph
    def work = getWork(bib)
    List bib249s = asList(instance['marc:hasBib249'])

    if(!work || !bib249s) {
        return
    }

    StringBuilder msg = new StringBuilder()
    msg.append(bib.doc.shortId).append('\n')
    msg.append(bib249s).append('\n')

    def converted = bib249s.findResults(this.&convert249)
    msg.append(' --> ').append(converted).append('\n')

    Map workTitles = allTitles(work)

    if (converted.size() == 1) {
        def ogTitle = converted.first()
        def matches = getExisting(ogTitle, workTitles, bib)
        if (!matches.isEmpty()) {
            msg.append("$ogTitle ${comparisonTitle(ogTitle)}) already exists, dropping it: $matches ")
            Script.s.increment('Single 249 already exists', matches.keySet())
        }
        else {
            work['hasTitle'] = asList(work['hasTitle'])
            if (work['hasTitle'].any{ it['@type'] == 'Title' }) {
                ogTitle['@type'] = 'VariantTitle'
            }

            work['hasTitle'].add(ogTitle)
            msg.append("--> work['hasTitle']: ${work['hasTitle']}\n")
            Script.s.increment('Single 249 to hasTitle', ogTitle['@type'])
        }
    }
    else if (converted.size() == 0) {
        msg.append("Broken 249, dropping\n")
        Script.s.increment('Broken', "total")
    }
    else {
        // TODO
        msg.append("MULTIPLE: $converted ${converted.collect { getExisting(it, workTitles, bib) }}\n")
        def matches = converted.collect{ getExisting(it, workTitles, bib) }
        if (matches.every{ !it.isEmpty() }) {
            msg.append("All titles already exist, dropping: $matches ")
            Script.s.increment('Multiple 249 - All existing', matches)
        }
        else if (matches.every{ it.isEmpty() }) {
            if (work['hasPart']) {
                throw new Exception("Already hasPart")
            }

            msg.append("Multiple 249 - No title already exists")
            Script.s.increment('Multiple 249', 'None existing')
            work['hasPart'] = converted.collect{
                [
                        '@type': 'Work',
                        'hasTitle': it
                ]
            }
        }
        else {
            msg.append("Multiple 249 - Some already exist")
            Script.s.increment('Multiple 249', "Some exist")
        }
    }

    instance.remove('marc:hasBib249')
    //bib.scheduleSave()

    msg.append('\n')
    println(msg.toString())
}

Map getExisting(Map ogTitle, Map workTitles, bib) {
    workTitles.findAll { path, title ->
        println ("CMP: ${bib.doc.shortId} ${comparisonTitle(ogTitle)} -- ${comparisonTitle(title)} (${ogTitle} == $path ${title})")
        comparisonTitle(ogTitle) == comparisonTitle(title)
    }
}

Map allTitles(Map work) {
    def workTitles = [:]
    DocumentUtil.findKey(work, 'hasTitle', { titles, path ->
        asList(titles).eachWithIndex { title, i ->
            workTitles.put((path + i).join(', '), title)
        }
        DocumentUtil.NOP
    })
    return workTitles
}

Map convert249(Map bib249) {
    Map<String, String> result = ['@type': 'Title']
    Script.MAP_249.each { k, v ->
        if (bib249[k] && StringUtils.isNotBlank(bib249[k])) {
            result[v] = bib249[k]
        }
    }

    if (result['mainTitle'] && result['subtitle']) {
        result['mainTitle'] = stripSuffix(result['mainTitle'], ': ')
    }
    if (result['mainTitle']) {
        result['mainTitle'] = stripSuffix(result['mainTitle'], '.')
    }
    if (result['marc:nonfilingChars'] == "0") {
        result.remove('marc:nonfilingChars')
    }

    return (result['mainTitle'] || result['subtitle'])
            ? result
            : null
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
    normalize(Script.TITLE_COMPONENTS.findResults { title.getOrDefault(it, null) }.join(' '))
}

private static List asList(Object o) {
    (o ?: []).with { it instanceof List ? it : [it] }
}

String normalize(String s) {
    StringUtils.normalizeSpace(asciiFold(s).replaceAll(/[^\p{L} ]/, '').toLowerCase().trim())
}

String asciiFold(String s) {
    def unicodeMark = /\p{M}/
    return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll(unicodeMark, '')
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