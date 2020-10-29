/**
 * Try to link local subject with inScheme "KBslagord" to SAO.
 *
 * See LXL-3407 for more info.
 */

INSCHEME_SAO = 'https://id.kb.se/term/sao'

import java.util.concurrent.ConcurrentLinkedQueue
import whelk.util.Statistics

modifiedIdsReport = getReportWriter("modified-ids.txt")
modifiedReport = getReportWriter("modified.txt")
errorReport = getReportWriter("errors.txt")
notInSaoReport = getReportWriter("not-in-sao.txt")
statistics = new Statistics(5).printOnShutdown()

saoMap = saoLabelToSubject()

File ids = new File(scriptDir, 'ids.txt')
ids.exists() ? selectByIds(ids.readLines(), this.&handle) : selectByCollection('bib', this.&handle)

void handle(bib) {
    try {
        statistics.withContext(bib.doc.shortId) {
            process(bib)
        }
    }
    catch(Exception e) {
        errorReport.println("${bib.doc.shortId} $e")
        e.printStackTrace(errorReport)
        println("${bib.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(bib) {
    boolean modified = false

    asList(getPathSafe(bib.graph[1], ['instanceOf', 'subject'])).each { Map subject ->
        if (isKbSlagord(subject)) {
            Map sao = tryLink(subject) ?: tryMapComplex(subject)
            if(sao) {
                String msg = "${subject['prefLabel']} -> ${sao['prefLabel'] ?: sao['@id']}"
                modifiedReport.println("${bib.doc.shortId} $msg")
                statistics.increment("in SAO", msg)

                subject.clear()
                subject.putAll(sao)
                modified = true
            }
            else {
                statistics.increment("Not in SAO (${subject['@type']})", subject['prefLabel'])
                notInSaoReport.println("${bib.doc.shortId} ${subject['prefLabel']}")
            }
        }
    }

    if (modified) {
        modifiedIdsReport.println(bib.doc.shortId)
        bib.scheduleSave()
    }
}

boolean isKbSlagord(Map subject) {
    List inScheme = asList(subject['inScheme']) // sometimes list...
    inScheme && 'kbslagord' == normalize(inScheme.first()['code'])
}

String normalize(String s) {
    s?.trim()?.toLowerCase()
}

Map asLink(Map m) {
    m && m['@id']
            ? ['@id': m['@id']]
            : null
}

Map findInSao(Map subject) {
    saoMap[normalize(subject['prefLabel'])]
}

Map tryLink(Map subject) {
    asLink(findInSao(subject))
}

Map tryMapComplex(Map subject) {
    if (subject['@type'] == 'ComplexSubject' && subject.keySet() != ['@type', 'prefLabel', 'inScheme', 'termComponentList'] as Set) {
        statistics.increment("Bad shape", subject)
    }
    else if (subject['@type'] == 'ComplexSubject') {
        List ogTerms = subject['termComponentList']
        def mappedTerms = ogTerms.findResults {
            it['@type'] == 'Topic' ? findInSao(it) : tryMapSubdivision(it)
        }
        if (mappedTerms.size() == ogTerms.size()) {
            def complex = [
                    '@type'            : 'ComplexSubject',
                    'prefLabel'        : mappedTerms.collect{ it['prefLabel'] }.join('--'),
                    'inScheme'         : INSCHEME_SAO,
                    'termComponentList': mappedTerms.collect{ asLink(it) ?: it }
            ]
            return tryLink(complex) ?: complex
        }
    }

    return null
}

Map tryMapSubdivision(Map subdivision) {
    Map result = _tryMapSubdivision(subdivision)
    statistics.increment(result ? 'z subdivision found' : 'z subdivision not found', subdivision)
    return result
}

Map _tryMapSubdivision(Map subdivision) {
    if(subdivision.size() == 1 && subdivision['@id'] && saoMap[subdivision['@id']]) {
        return saoMap[subdivision['@id']]
    }
    if(subdivision.size() != 2 || !subdivision['@type'] || !subdivision['prefLabel']) {
        statistics.increment('bad subdivision', subdivision)
        return null
    }

    def inSao = findInSao(subdivision)
    if (inSao) {
        return inSao['@type'] == 'TopicSubdivision'
                ? inSao
                : ['@type': subdivision['@type'], 'prefLabel': inSao['prefLabel']]
    }
    else if (subdivision['@type'] in ['Temporal', 'TemporalSubdivision']) {
        return subdivision
    }
    else {
        return null
    }
}

Map saoLabelToSubject() {
    def q = [
            //'@type'       : ['Topic', 'ComplexSubject', 'Geographic', 'Temporal'],
            'inScheme.@id': [INSCHEME_SAO],
            'q'           : ['*'],
            '_sort'       : ['@id']
    ]

    ConcurrentLinkedQueue<Map> d = new ConcurrentLinkedQueue<>()
    selectByIds(queryIds(q).collect()) { d.add(it.graph[1]) }

    Map m = [:]
    d.forEach({subject ->
        m[subject['@id']] = subject

        asList(subject['prefLabel']).each {
            m[normalize(it)] = subject
        }

        asList(subject['altLabel']).each {
            m[normalize(it)] = subject
        }

        asList(subject['hasVariant']).each { variant ->
            m[normalize(variant['prefLabel'])] = subject
        }
    } )

    return m
}

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}


Object getPathSafe(item, path, defaultTo = null) {
    for (p in path) {
        if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}
