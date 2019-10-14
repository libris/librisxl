import datatool.util.Statistics
/*
 * This removes blank language nodes
 *
 * See LXL-2737 for more info.
 *
 */

OBSOLETE_CODES = ['9ss', '9sl']

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

languageMap = buildLanguageMap()
isBlank = { !it.containsKey('@id') }

stats = new Statistics().printOnShutdown()

substitutions = ['fra'             : 'fre',
                 'jap'             : 'jpn',
                 'taj'             : 'tgz',
                 'mongoliskt språk': 'mongoliska språk',
                 'mongoliska'      : 'mongoliska språk',
]

/*
substitutions = [
    'franska (fornfranska)'           : 'fornfranska',
    'tyska (medelhögtyska)'           : 'medelhögtyska',
    'engelska (medelengelska)'        : 'medelengelska',
    'engelska (fornengelska)'         : 'fornengelska',
    'tyska (lågtyska)'                : 'lågtyska',
    'nederländska (medelnederländska)': 'medelnederländska',
    'norska (nynorsk)'                : 'nynorsk',
    'franska (medelfranska)'          : 'medelfranska',
    'tyska (medellågtyska)'           : 'medellågtyska',
    'arabiska (judearabiska)'         : 'judearabiska',
    'samiska (nordsamiska)'           : 'nordsamiska',
    'samiska (lulesamiska)'           : 'lulesamiska'
]
*/

selectByCollection('bib') { bib ->
    try {
        def (record, thing, work) = bib.graph
        if (!((String) work['@id']).endsWith('#work')) {
            return
        }

        boolean modified = false
        new DFS().search(work) { path, value ->
            if (path && path.last().equals('language') && replaceBlankNode(value)) {
                modified = true
            }
        }

        if (modified) {
            scheduledForUpdate.println("${bib.doc.getURI()}")
            bib.scheduleSave()
        }
    }
    catch (Exception e) {
        println "failed ${bib.doc.getURI()} : ${e}"
        //e.printStackTrace()
    }
}

private boolean replaceBlankNode(List languages) {
    def toBeRemoved = []
    def toBeAdded = []

    languages.findAll(isBlank).each { blank ->
        Map copy = new HashMap(blank)
        if (replaceBlankNode(copy)) {
            toBeRemoved.add(blank)
            boolean alreadyLinked = copy['@id'] && languages.find { it['@id'] == copy['@id'] }
            stats.increment('alreadyLinked', alreadyLinked)
            if (!alreadyLinked) {
                toBeAdded.add(copy)
            }
        }
    }

    if (!toBeRemoved.isEmpty()) {
        languages.addAll(toBeAdded)
        languages.removeAll(toBeRemoved)
        return true
    } else {
        return false
    }
}

private boolean replaceBlankNode(Map language) {
    if (!isBlank(language)) {
        return false
    }

    stats.increment('shapes', new HashSet(language.keySet()))
    if (!((language['@type'] && language['label']) || (language['code'] && language['sameAs']) || language['code'])) {
        throw new RuntimeException("Unhandled shape: " + language.keySet())
    }

    String key = language['label'] ? 'label' : 'code'
    String labelOrCode = clean(language[key])
    String id = languageMap.get(labelOrCode)
    if (id) {
        stats.increment(key + ' - replaced ', labelOrCode)
        language.clear()
        language.put('@id', id)
        return true
    } else {
        stats.increment(key + ' - not replaced', labelOrCode)
    }

    if (language['sameAs'] && !languageMap.values().contains(language['sameAs'])) {
        stats.increment('sameAs 404 - removed', language['sameAs'])
        language.remove('sameAs')
        return true
    }

    return false
}

private String clean(String s) {
    s = s.toLowerCase()
    if (s.endsWith('.')) {
        s = s.substring(0, s.size() - 1)
    }
    if (substitutions.containsKey(s)) {
        s = substitutions[s]
    }
    return s
}

Map<String, String> buildLanguageMap() {
    def q = [
            "@type": ["Language"],
            "q"    : ["*"],
            '_sort': ["@id"]
    ]

    Map<String, String> m = [:]
    Set<String> ambiguousLabels = new HashSet<>()

    for (Map lang : queryDocs(q)) {
        (lang.prefLabelByLang as Map).values().each(maybeCollection({ String label ->
            String code = lang['code']
            if (OBSOLETE_CODES.contains(code)) {
                return
            }

            label = label.toLowerCase()
            String id = lang['@id']

            if (label != code) {
                m.put(lang['code'], id)
            }
            if (m.put(label, id) != null) {
                ambiguousLabels.add(label)
            }
        }))
    }

    println('Ambiguous labels, skipping: ' + ambiguousLabels)
    ambiguousLabels.each { m.remove(it) }
    println("${m.size()} languages")

    return m
}

Closure maybeCollection(Closure<?> c) {
    return { o ->
        if (o instanceof Collection) {
            o.each(c)
        } else {
            c.call(o)
        }
    }
}

class DFS {
    interface Callback {
        void node(List path, value)
    }

    Stack path
    Callback cb

    void search(obj, Callback callback) {
        cb = callback
        path = new Stack()
        node(obj)
    }

    private void node(obj) {
        cb.node(path, obj)
        if (obj instanceof Map) {
            descend(((Map) obj).entrySet().collect({ new Tuple2(it.value, it.key) }))
        } else if (obj instanceof List) {
            descend(((List) obj).withIndex())
        }
    }

    private void descend(List<Tuple2> nodes) {
        for (n in nodes) {
            path.push(n.second)
            node(n.first)
            path.pop()
        }
    }
}