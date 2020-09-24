package datatool.scripts.mergeworks

import datatool.scripts.mergeworks.compare.Classification
import datatool.scripts.mergeworks.compare.ContentType
import datatool.scripts.mergeworks.compare.FieldHandler
import datatool.scripts.mergeworks.compare.GenreForm
import datatool.scripts.mergeworks.compare.Default
import datatool.scripts.mergeworks.compare.StuffSet
import datatool.scripts.mergeworks.compare.WorkTitle
import datatool.util.DocumentComparator

class WorkComparator {
    Set<String> fields
    DocumentComparator c = new DocumentComparator()

    Map<String, FieldHandler> comparators = [
            'classification': new Classification(),
            'subject': new StuffSet(),
            'genreForm': new GenreForm(),
            'contentType': new ContentType('https://id.kb.se/term/rda/Text'),
            'hasTitle': new WorkTitle(),
    ]

    static FieldHandler DEFAULT = new Default()

    WorkComparator(Set<String> fields) {
        this.fields = new HashSet<>(fields)
    }

    boolean sameWork(Doc a, Doc b) {
        fields.every {compare(a, b, it).with {it == EQUAL || it == COMPATIBLE} }
    }

    FieldStatus compare(Doc a, Doc b, String field) {
        Object oa = a.getWork().get(field)
        Object ob = b.getWork().get(field)

        if (oa == null && ob == null) {
            return FieldStatus.EQUAL
        }

        compareExact(oa, ob, field) == FieldStatus.EQUAL
                ? FieldStatus.EQUAL
                : compareDiff(a, b, field)
    }

    Map merge(Collection<Doc> docs) {
        Map result = [:]
        fields.each {field ->
            FieldHandler h = comparators.getOrDefault(field, DEFAULT)
            Object value = docs.first().getWork().get(field)
            def rest = docs.drop(1)
            rest.each {
                value = h.merge(value, it.getWork().get(field))
            }
            if(value) {
                result[field] = value
            }
        }

        if (!result['hasTitle']) {
            result['hasTitle'] = bestTitle(docs)
        }

        return result
    }

    private FieldStatus compareDiff(Doc a, Doc b, String field) {
        comparators.getOrDefault(field, DEFAULT).isCompatible(a.getWork().get(field), b.getWork().get(field))
                ? FieldStatus.COMPATIBLE
                : FieldStatus.DIFF
    }

    private FieldStatus compareExact(Object oa, Object ob, String field) {
        c.isEqual([(field): oa], [(field): ob]) ? FieldStatus.EQUAL : FieldStatus.DIFF
    }

    static Map<FieldStatus, List<String>> compare(Collection<Doc> cluster) {
        WorkComparator c = new WorkComparator(allFields(cluster))

        Map<FieldStatus, List<String>> result = [:]
        c.fieldStatuses(cluster).each {f, s -> result.get(s, []) << f}
        return result
    }

    static Set<String> allFields(Collection<Doc> cluster) {
        Set<String> fields = new HashSet<>()
        cluster.each { fields.addAll(it.getWork().keySet()) }
        return fields
    }

    Map<String, FieldStatus> fieldStatuses(Collection<Doc> cluster) {
        fields.collectEntries {[it, fieldStatus(cluster, it)]}
    }

    FieldStatus fieldStatus(Collection<Doc> cluster, String field) {
        boolean anyCompat = false
        [cluster, cluster].combinations().findResult { List combination ->
            Doc a = combination.first()
            Doc b = combination.last()

            def c = compare(a, b, field)
            if (c == FieldStatus.COMPATIBLE) {
                anyCompat = true
            }
            c == FieldStatus.DIFF ? c : null
        } ?: (anyCompat ? FieldStatus.COMPATIBLE : FieldStatus.EQUAL)
    }

    //FIXME
    static Object bestTitle(Collection<Doc> docs) {
        def t = docs.findResult {it.encodingLevel() == 'marc:FullLevel' ? it.getInstance()['hasTitle'] : null }
        if(t) { return t }
        t = docs.findResult {it.encodingLevel() == 'marc:MinimalLevel' ? it.getInstance()['hasTitle'] : null }
        return t
    }
}