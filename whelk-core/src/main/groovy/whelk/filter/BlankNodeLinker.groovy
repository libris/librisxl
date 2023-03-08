package whelk.filter

import com.google.common.collect.Iterables
import whelk.JsonLd
import whelk.Whelk
import whelk.search.ESQuery
import whelk.search.ElasticFind
import whelk.util.DocumentUtil
import whelk.util.Statistics

/*
Groovy compiler fails...
.../compileGroovy/groovy-java-stubs/whelk/filter/BlankNodeLinker.java:10: error: cannot find symbol

import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY
 */
import static whelk.util.DocumentUtil.NOP
import static whelk.util.DocumentUtil.Remove
import static whelk.util.DocumentUtil.findKey
import static whelk.util.DocumentUtil.link

class BlankNodeLinker implements DocumentUtil.Linker {
    static final String DELETE = '/dev/null'

    List<String> types
    Map map = [:]
    Map<String, List> ambiguousIdentifiers = [:]
    Map substitutions = [:]
    Statistics stats

    List<String> fields = []

    BlankNodeLinker(Collection<String> types, Collection<String> fields, Statistics stats = null) {
        this.types = types.collect()
        this.fields = fields.collect()
        this.stats = stats
    }

    BlankNodeLinker(String type, Collection<String> fields, Statistics stats = null) {
        this([type], fields, stats)
    }

    boolean linkAll(data, Collection<String> keys) {
        return findKey(data, keys, link(this)) && removeDeleted(data)
    }

    boolean linkAll(data, String key) {
        return findKey(data, key, link(this)) && removeDeleted(data)
    }

    static boolean removeDeleted(data) {
        // clean up blank nodes that have been deleted (marked by linking to DELETE)
        findKey(data, JsonLd.ID_KEY) { value, path ->
            value == DELETE
                    ? new Remove()
                    : NOP
        }
        return true
    }

    void loadDefinitions(Whelk whelk) {
        def finder = new ElasticFind(new ESQuery(whelk))

        types.each { type ->
            def q = [
                    (JsonLd.TYPE_KEY): [type],
                    "q"       : ["*"],
                    '_sort'   : [JsonLd.ID_KEY]
            ]

            Iterables.partition(finder.findIds(q), 100).each { List<String> i ->
                whelk.bulkLoad(i).each { id, doc ->
                    addDefinition(doc.data[JsonLd.GRAPH_KEY][1])
                }
            }
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
            } else {
                (definition[field] ?: []).with(maybeCollection({ String identifier ->
                    identifiers.add(identifier.toLowerCase())
                }))
            }
        }

        String id = definition.isReplacedBy ? definition.isReplacedBy[JsonLd.ID_KEY] : definition[JsonLd.ID_KEY]
        identifiers.each { addMapping(it, id) }
    }

    void addMapping(String from, String to) {
        from = from.toLowerCase()
        if (map[from] == to || (ambiguousIdentifiers[from] ?: []).contains(to)) {
            return // seen the exact same mapping before
        }

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

    void addDeletions(List<String> deletions) {
        deletions.each {
            addMapping(it, DELETE)
        }
    }

    @Override
    List<Map> link(Map blank, List existingLinks) {
        if (blank[JsonLd.TYPE_KEY] && !(blank[JsonLd.TYPE_KEY] in types)) {
            incrementCounter('unhandled type', blank[JsonLd.TYPE_KEY])
            return
        }

        if (!fields.any { blank.containsKey(it) }) {
            incrementCounter('unhandled shape', blank.keySet())
            throw new RuntimeException('unhandled shape: ' + blank.keySet())
        }

        for (String key : fields) {
            if (blank[key]) {
                List<String> links = findLinks(blank[key], existingLinks)
                if (links) {
                    incrementCounter('mapped', blank[key])
                    return links.collect { [(JsonLd.ID_KEY): it] }
                }
            }
        }

        for (String key : fields) {
            if (blank[key]) {
                incrementCounter('not mapped (canonized values)', canonize(blank[key].toString()))
            }
        }

        if (blank['sameAs'] && !blank['sameAs'].any { knownId(it[JsonLd.ID_KEY]) }) {
            incrementCounter('sameAs 404 - removed', blank['sameAs'])
            Map r = new HashMap(blank)
            r.remove('sameAs')
            return [r]
        }

        return null
    }

    List<Map> link(String blank, List existingLinks = []) {
        incrementCounter('single value encountered', blank)

        List<String> links = findLinks(blank, existingLinks)
        if (links) {
            incrementCounter('mapped', blank)
            return links.collect { [(JsonLd.ID_KEY): it] }
        } else {
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

    @Override
    String toString() {
        return "${JsonLd.TYPE_KEY}: $types (${map.size()} mappings)"
    }
}