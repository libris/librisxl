/**
 * Convert and move "original title" (obsolete marc field bib:249) from instance to work.
 *
 * If one 249 and not found anywhere in work
 *  - If work hasTitle (@type: Title) move to VariantTitle else to Title
 *
 * If multiple 249
 *  - If all found in work then drop
 *  - If none found in work the move to hasPart unless work already contains hasPart
 *  - If some are found then do nothing and log
 *
 * See LXL-3103 for more information.
 */

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

    static PrintWriter singleToMainTitle
    static PrintWriter singleToVariantTitle
    static PrintWriter singleExists
    static PrintWriter broken
    static PrintWriter multipleAllExist
    static PrintWriter multipleNoneExist
    static PrintWriter multipleSomeExistHandled
    static PrintWriter multipleSomeExistUnhandled
    static PrintWriter alreadyHasPart
}
Script.singleToMainTitle = getReportWriter("single-to-main-title.txt")
Script.singleToVariantTitle = getReportWriter("single-to-variant-title.txt")
Script.singleExists = getReportWriter("single-already-exists.txt")
Script.broken = getReportWriter("all-broken.txt")
Script.multipleAllExist = getReportWriter("multiple-all-exist.txt")
Script.multipleNoneExist = getReportWriter("multiple-to-hasPart.txt")
Script.multipleSomeExistHandled = getReportWriter("multiple-some-exist-to-hasPart.txt")
Script.multipleSomeExistUnhandled = getReportWriter("multiple-some-exist-already-hasPart.txt")
Script.alreadyHasPart = getReportWriter("multiple-work-already-hasPart.txt")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println("${bib.doc.shortId} $e")
        e.printStackTrace()
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

    Map workTitles = pathsOfTitles(work)

    // ONE 249
    if (converted.size() == 1) {
        def ogTitle = converted.first()
        def matches = findMatchingTitles(ogTitle, workTitles)

        if (!matches.isEmpty()) {
            msg.append("Already exists: $matches\n")
            print(Script.singleExists, msg)
            Script.s.increment('Single 249 already exists (dropped)', matches.keySet())
        }
        else {
            work['hasTitle'] = asList(work['hasTitle'])
            boolean variant = work['hasTitle'].any{ it['@type'] == 'Title' }
            if (variant) {
                // TODO: handle (temporarily disabled - rule needs to be verified)
                return

                //ogTitle['@type'] = 'VariantTitle'
            }

            work['hasTitle'].add(ogTitle)

            msg.append(" --> work['hasTitle']: ${work['hasTitle']}\n")
            print(variant ? Script.singleToVariantTitle : Script.singleToMainTitle, msg)
            Script.s.increment('Single 249 to hasTitle', ogTitle['@type'])
        }
    }
    // ALL 249 BROKEN
    else if (converted.size() == 0) {
        print(Script.broken, msg)
        Script.s.increment('Broken', "total")
        // TODO: handle (temporarily disabled - rule needs to be verified)
        return
    }
    // MULTIPLE 249
    else {
        def matches = converted.collect{ findMatchingTitles(it, workTitles) }

        if (matches.every{ !it.isEmpty() }) {
            msg.append("Matches:\n  ${matches} \n")
            print(Script.multipleAllExist, msg)
            Script.s.increment('Multiple 249', 'All exist (dropped)')
        }
        else if (matches.every{ it.isEmpty() }) {
            if (work['hasPart']) {
                msg.append("Existing hasPart:\n  ${work['hasPart']} \n")
                print(Script.alreadyHasPart, msg)
                Script.s.increment('Multiple 249', 'Already hasPart (unhandled)')
                return
            }

            setHasPart(work, converted)

            print(Script.multipleNoneExist, msg)
            Script.s.increment('Multiple 249', 'None existing (to hasPart)')

            // TODO: handle (temporarily disabled - rule needs to be verified)
            return
        }
        else {
            msg.append("Matches:\n  ${matches} \n")

            if (work['hasPart']) {
                msg.append("Existing hasPart:\n  ${work['hasPart']} \n")
                print(Script.multipleSomeExistUnhandled, msg)
                Script.s.increment('Multiple 249', "Some exist (unhandled)")
                return
            }
            else {
                setHasPart(work, converted)
                print(Script.multipleSomeExistHandled, msg)
                Script.s.increment('Multiple 249', "Some exist (to hasPart)")

                // TODO: handle (temporarily disabled - rule needs to be verified)
                return
            }
        }
    }

    instance.remove('marc:hasBib249')

    bib.scheduleSave()
}

void setHasPart(Map work, List converted) {
    work['hasPart'] = converted.collect{
        [
                '@type': 'Work',
                'hasTitle': it
        ]
    }
}

Map findMatchingTitles(Map ogTitle, Map workTitles) {
    workTitles.findAll { path, title ->
        comparisonTitle(ogTitle) == comparisonTitle(title)
    }
}

Map pathsOfTitles(Map work) {
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
        if (bib249[k] && isNotEmpty(bib249[k])) {
            result[v] = bib249[k]
        }
    }

    if (result['mainTitle'] && result['subtitle']) {
        result['mainTitle'] = stripSuffix(result['mainTitle'].trim(), ':').trim()
    }
    if (result['mainTitle']) {
        result['mainTitle'] = stripSuffix(result['mainTitle'].trim(), '.').trim()
    }
    if (result['marc:nonfilingChars'] == "0") {
        result.remove('marc:nonfilingChars')
    }

    return (result['mainTitle'] || result['subtitle'])
            ? result
            : null
}

static boolean isNotEmpty(v) {
    if (v instanceof String) {
        StringUtils.isNotBlank(v)
    }
    else if (v instanceof Collection) {
        !v.isEmpty()
    }
    else {
        true
    }
}

static String stripSuffix(String s, String suffix) {
    if (s.endsWith(suffix)) {
        s.substring(0, s.length() - suffix.length())
    }
    else {
        s
    }
}

void print(PrintWriter p, StringBuilder msg) {
    p.println(msg.toString() + '\n')
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