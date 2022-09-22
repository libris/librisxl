/*
Re-export (save loud) any document that might have been exported with an ISO-639-3 language code instead of MARC21 code

See LXL-3886 for more information
*/

import java.util.concurrent.ConcurrentLinkedQueue
import static whelk.util.DocumentUtil.findKey

def LANGUAGES = [
        'https://id.kb.se/language/9ft',
        'https://id.kb.se/language/9mk',
        // 'https://id.kb.se/language/9sj',
        // 'https://id.kb.se/language/9su'
]

ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<>()
selectByIds(LANGUAGES) { l ->
    q.addAll((List) getAtPath(l.graph, [1, 'sameAs', '*', '@id'], []))
}
q.addAll(LANGUAGES)

Set<String> ids = q.collect() as Set
ids.each {
    println(it)
}

selectByCollection('bib') { bib ->
    findKey(bib.graph, '@id') { value, path ->
        if (value in ids) {
            // Links to sameAs are automatically replaced on save
            bib.scheduleSave(loud:true)
        }
    }
}

// =================================================================

static Object getAtPath(item, Iterable path, defaultTo = null) {
    if(!item) {
        return defaultTo
    }

    for (int i = 0 ; i < path.size(); i++) {
        def p = path[i]
        if (p == '*' && item instanceof Collection) {
            return item.collect { getAtPath(it, path.drop(i + 1), []) }.flatten()
        }
        else if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}