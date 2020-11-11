/**
 * Copy of ../10/lxl-3407-kblagord for holdings
 * 
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

// There are two "Historia" in SAO, one Topic and one TopicSubdivision. So we need to keep these separate.
saoMap = saoLabelToSubject()

File ids = new File(scriptDir, 'ids.txt')
ids.exists() ? selectByIds(ids.readLines(), this.&handle) : selectByCollection('hold', this.&handle)

void handle(hold) {
    try {
        statistics.withContext(hold.doc.shortId) {
            process(hold)
        }
    }
    catch(Exception e) {
        errorReport.println("${hold.doc.shortId} $e")
        e.printStackTrace(errorReport)
        println("${hold.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(hold) {
    boolean modified = false

    asList(hold.graph[1]['subject']).each { Map subject ->
        if (isKbSlagord(subject)) {
            Map sao = tryLink(subject)
            if(sao) {
                String msg = "${subject['prefLabel']} -> ${sao['prefLabel'] ?: sao['@id']}"
                modifiedReport.println("${hold.doc.shortId} $msg")
                statistics.increment("in SAO", msg)

                subject.clear()
                subject.putAll(sao)
                modified = true
            }
            else {
                statistics.increment("Not in SAO (${subject['@type']})", subject['prefLabel'])
                notInSaoReport.println("${hold.doc.shortId} ${subject['prefLabel']}")
            }
        }
    }

    if (modified) {
        hold.graph[1]['subject'] = asList(hold.graph[1]['subject'])
                .unique{ a, b -> a.toString() <=> b.toString() }

        modifiedIdsReport.println(hold.doc.shortId)
        hold.scheduleSave()
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

PrefMap saoLabelToSubject() {
    labelToSubject(['Topic', 'ComplexSubject', 'Geographic', 'Temporal'])
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

    println("SAO duplicate labels")
    m.duplicates().collect{ k, v -> "$k ${v.collect{ it['prefLabel'] }} ${v.collect{ it['@id'] }}" }.sort()
            .each {println(it) }

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

// if same key is added more than once, remove it
class UniqueMap extends HashMap {
    Map duplicates = [:]

    @Override
    Object put(k, v) {
        if (duplicates.containsKey(k)) {
            duplicates[k] << v
        } else if (containsKey(k)) {
            duplicates[k] = [v, remove(k)]
        } else {
            super.put(k, v)
        }
        return null
    }

    Map duplicates() {
        return duplicates
    }
}

// two-tier version of "UniqueMap".
// - if the same key is added with pref and putPref, get will return the value added with putPref
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

    Map duplicates() {
        return pref.duplicates() + (notPref.duplicates().findAll { !pref.containsKey(it.key)})
    }
}