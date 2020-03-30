package whelk.filter

import whelk.Whelk
import whelk.search.ESQuery
import whelk.util.DocumentUtil
import whelk.util.Statistics

import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY

class BlankNodeLinker implements DocumentUtil.Linker {
    String type
    Map map = [:]
    Map<String, List> ambiguousIdentifiers = [:]
    Map substitutions = [:]
    Statistics stats

    List<String> fields = []

    BlankNodeLinker(String type, List<String> fields, Statistics stats = null) {
        this.type = type
        this.fields = fields
        this.stats = stats
    }

    boolean linkAll(data, String key) {
        return DocumentUtil.findKey(data, key, DocumentUtil.link(this))
    }

    void loadDefinitions(Whelk whelk) {
        def q = [
                (TYPE_KEY): [type],
                "q"              : ["*"],
                '_sort'          : [ID_KEY]
        ]

        whelk.bulkLoad(new ESQuery(whelk).doQueryIds(q)).values().each { definition ->
            addDefinition(definition.data[GRAPH_KEY][1])
        }
    }

    void addDefinition(Map definition) {
        Set<String> identifiers = [] as Set
        for (String field in fields) {
            // FIXME: get from context
            if (field.endsWith('ByLang')) {
                def labels = definition[field] as Map ?: [:]
                labels.values().each(maybeCollection({ String label ->
                    identifiers.add(label.toLowerCase())
                }))
            }
            else {
                (definition[field] ?: []).with(maybeCollection( { String identifier ->
                    identifiers.add(identifier.toLowerCase())
                }))
            }
        }

        String id = definition[ID_KEY]
        identifiers.each { addMapping(it, id) }
    }

    void addMapping(String from, String to) {
        from = from.toLowerCase()
        if (ambiguousIdentifiers.containsKey(from)) {
            ambiguousIdentifiers[from] << to
        } else if (map.containsKey(from)) {
            ambiguousIdentifiers[from] = [to, map.remove(from)]
        } else {
            map[from] = to
        }
    }

    void addSubstitutions(Map s) {
        substitutions.putAll(s)
    }

    @Override
    List<Map> link(Map blank, List existingLinks) {
        if (blank[TYPE_KEY] && blank[TYPE_KEY] != type) {
            incrementCounter('unhandled type', blank[TYPE_KEY])
            return
        }

        if (!fields.any {blank.containsKey(it)}) {
            incrementCounter('unhandled shape', blank.keySet())
            throw new RuntimeException('unhandled shape: ' + blank.keySet())
        }

        for (String key : fields) {
            if (blank[key]) {
                List<String> links = findLinks(blank[key], existingLinks)
                if (links) {
                    incrementCounter('mapped', blank[key])
                    return links.collect { [(ID_KEY): it] }
                }
            }
        }

        for (String key : fields) {
            if (blank[key]) {
                incrementCounter('not mapped (canonized values)', canonize(blank[key].toString()))
            }
        }

        if (blank['sameAs'] && !blank['sameAs'].any { knownId(it[ID_KEY]) }) {
            incrementCounter('sameAs 404 - removed', blank['sameAs'])
            Map r = new HashMap(blank)
            r.remove('sameAs')
            return [r]
        }

        return null
    }

    List<Map> link(String blank) {
        incrementCounter('single value encountered', blank)

        List<String> links = findLinks(blank, [])
        if (links) {
            incrementCounter('mapped', blank)
            return links.collect { [(ID_KEY): it] }
        }
        else {
            incrementCounter('not mapped (canonized values)', canonize(blank))
        }
    }

    protected List<String> findLinks(def value, List existingLinks) {
        if (value instanceof String && findLink(value, existingLinks)) {
            return [findLink(value, existingLinks)]
        }

        List multiple = split(value)
        if (multiple && multiple.every { findLinks(it, existingLinks) != null }) {
            return multiple.collect { findLinks(it, existingLinks) }.flatten()
        }

        return null
    }

    protected String findLink(String s, List existingLinks) {
        s = canonize(s)
        if (map.containsKey(s)) {
            return map[s]
        }
        if (ambiguousIdentifiers.containsKey(s)) {
            for (String id : ambiguousIdentifiers[s]) {
                if (existingLinks.contains(id)) {
                    return id
                }
            }
        }
        return null
    }

    protected boolean knownId(String id) {
        return map.values().contains(id)
    }

    protected List split(value) {
        if (value instanceof List) {
            return value
        }

        return []
    }

    protected String canonize(String s) {
        s = trim(s.toLowerCase())
        if (substitutions.containsKey(s)) {
            s = substitutions[s]
        }
        return s
    }

    protected String trim(String s) {
        // remove leading and trailing non-"alpha, digit or parentheses"
        def w = /\(\)\p{IsAlphabetic}\p{Digit}/
        def m = s =~ /[^${w}]*([${w}- ]*[${w}])[^${w}]*/
        return m.matches() ? m.group(1) : s
    }

    protected void incrementCounter(String category, Object name) {
        if (stats) {
            stats.increment(category, name)
        }
    }

    static Closure maybeCollection(Closure<?> c) {
        return { o ->
            if (o instanceof Collection) {
                o.each(c)
            } else {
                c.call(o)
            }
        }
    }
}