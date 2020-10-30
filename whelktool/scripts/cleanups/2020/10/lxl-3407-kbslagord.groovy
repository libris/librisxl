/**
 * Try to link local subject with inScheme "KBslagord" to SAO.
 *
 * ComplexSubject
 * The termComponents of post-coordinated ComplexSubjects cannot all be linked because as of now the MARC conversion
 * needs the type (TopicSubdivision, GeographicSubdivision, TemporalSubdivision) to set the correct subfield.
 * i.e linking GeographicSubdivision "Sverige" in Eldbegängelse--Sverige to sao/Sverige which is of type Geographic
 * breaks the conversion. Instead we just check that the labels exist in SAO.
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
saoSubdivisions = subdivisionLabelToSubject()

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
    saoMap.get(normalize(subject['prefLabel']))
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
    if(subdivision.size() == 1 && subdivision['@id'] && saoMap.get(subdivision['@id'])) {
        // already linked to SAO - get prefLabel
        return saoMap.get(subdivision['@id'])
    }
    if(subdivision.size() != 2 || !subdivision['@type'] || !subdivision['prefLabel']) {
        statistics.increment('bad subdivision', subdivision)
        return null
    }

    if (subdivision['@type'] == 'TopicSubdivision' && saoSubdivisions.get(normalize(subdivision['prefLabel']))) {
        // link to SAO
        return saoSubdivisions.get(normalize(subdivision['prefLabel']))
    }

    def inSao = findInSao(subdivision)
    if (inSao) {
        // map to prefLabel in SAO
        return inSao
    }
    else if (subdivision['@type'] in ['Temporal', 'TemporalSubdivision']) {
        // temporal is always OK
        return subdivision
    }
    else {
        return null
    }
}

PrefMap saoLabelToSubject() {
    labelToSubject(['Topic', 'ComplexSubject', 'Geographic', 'Temporal'])
}

PrefMap subdivisionLabelToSubject() {
    labelToSubject(['TopicSubdivision'])
}

PrefMap labelToSubject(types) {
    def q = [
            '@type'       : types,
            'inScheme.@id': [INSCHEME_SAO],
            'q'           : ['*'],
            '_sort'       : ['@id']
    ]

    ConcurrentLinkedQueue<Map> d = new ConcurrentLinkedQueue<>()
    selectByIds(queryIds(q).collect()) { d.add(it.graph[1]) }

    PrefMap m = new PrefMap()
    d.forEach({subject ->
        m.put(subject['@id'], subject)

        asList(subject['prefLabel']).each {
            m.putPref(normalize(it), subject)
        }

        asList(subject['altLabel']).each {
            m.put(normalize(it), subject)
        }

        asList(subject['hasVariant']).each { variant ->
            if (variant['prefLabel']) {
                m.put(normalize(variant['prefLabel']), subject)
            }
        }
    } )

    println("SAO ambigous labels")
    m.ambigous().collect{ k, v -> "$k ${v.collect{ it['@id'] }}" }.sort().each {println(it) }
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

class PrefMap {
    UniqueMap pref = new UniqueMap()
    UniqueMap notPref = new UniqueMap()

    Object put(k, v) {
        notPref[k] = v
    }

    Object putPref(k, v) {
        pref[k] = v
        notPref[k] = v
    }

    Object get(k) {
        return pref[k] ?: notPref[k]
    }

    Map ambigous() {
        return pref.ambigous() + (notPref.ambigous().findAll { !pref.containsKey(it.key)})
    }
}

class UniqueMap extends HashMap {
    Map ambigous = [:]

    @Override
    Object put(k, v) {
        if (ambigous.containsKey(k)) {
            ambigous[k] << v
        } else if (containsKey(k)) {
            ambigous[k] = [v, remove(k)]
        } else {
            super.put(k, v)
        }
        return null
    }

    Map ambigous() {
        return ambigous
    }
}