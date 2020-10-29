import java.util.concurrent.ConcurrentLinkedQueue
import whelk.util.Statistics

modifiedReport = getReportWriter("modified.txt")
errorReport = getReportWriter("errors.txt")
statistics = new Statistics(5).printOnShutdown()

sao = saoLabelToId()

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
        e.printStackTrace()
    }
}

void process(bib) {
    boolean modified = false

    asList(getPathSafe(bib.graph[1], ['instanceOf', 'subject'])).each { Map subject ->
        if ('kbslagord' == getPathSafe(subject, ['inScheme', 'code'], '').toLowerCase().trim()) {
            String label = subject['prefLabel']
            String saoId = sao[normalize(label)]
            if (saoId) {
                statistics.increment('in SAO', "$label -> $saoId")
                subject.clear()
                subject['@id'] = saoId
                modified = true
            }
            else {
                statistics.increment('not in SAO', "$label")
            }
        }
    }

    if (modified) {
        modifiedReport.println(bib.doc.shortId)
        bib.scheduleSave()
    }
}

String normalize(String s) {
    s?.trim()?.toLowerCase()
}

Map saoLabelToId() {
    def q = [
            '@type'       : ['Topic', 'ComplexSubject'],
            'inScheme.@id': ['https://id.kb.se/term/sao'],
            'q'           : ['*'],
            '_sort'       : ['@id']
    ]

    ConcurrentLinkedQueue<Map> d = new ConcurrentLinkedQueue<>()
    selectByIds(queryIds(q).collect()) { d.add(it.graph[1]) }

    Map m = [:]
    d.forEach({subject ->
        m[normalize(subject['prefLabel'])] = subject['@id']

        asList(subject['altLabel']).each {
            m[normalize(it)] = subject['@id']
        }

        asList(subject['hasVariant']).each { variant ->
            m[normalize(variant['prefLabel'])] = subject['@id']
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
