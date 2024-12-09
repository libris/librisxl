package se.kb.libris.mergeworks

import whelk.datatool.util.DocumentComparator
import se.kb.libris.mergeworks.compare.*

import static Util.bestTitle

class WorkComparator {
    Set<String> fields
    DocumentComparator c = new DocumentComparator()

    Map<String, FieldHandler> comparators = [
            'classification'  : new Classification(),
            'contentType'     : new ContentType(),
            'genreForm'       : new GenreForm(),
            'hasTitle'        : new WorkTitle(),
            'intendedAudience': new IntendedAudience(),
            '_numPages'       : new Extent(),
            'subject'         : new Subject(),
            'summary'         : new StuffSet(),
            'translationOf'   : new TranslationOf(),
            '@id'             : new Id()
    ]

    static Set<String> ignore = ['closeMatch']

    static FieldHandler DEFAULT = new Default()

    WorkComparator(Set<String> fields) {
        this.fields = new HashSet<>(fields)
    }

    boolean sameWork(Doc a, Doc b) {
        fields.every { compare(a, b, it).with { it == FieldStatus.EQUAL || it == COMPATIBLE } }
    }

    FieldStatus compare(Doc a, Doc b, String field) {
        Object oa = a.workData.get(field)
        Object ob = b.workData.get(field)

        if (oa == null && ob == null) {
            return FieldStatus.EQUAL
        }

        compareExact(oa, ob, field) == FieldStatus.EQUAL
                ? FieldStatus.EQUAL
                : compareDiff(a, b, field)
    }

    Map merge(Collection<Doc> docs) {
        Map result = [:]

        fields.each { field ->
            FieldHandler h = comparators.getOrDefault(field, DEFAULT)
            def value = h instanceof ValuePicker
                    ? h.pick(docs)
                    : mergeField(field, h, docs)

            if (value) {
                result[field] = value
            }
        }

        if (!result['hasTitle']) {
            def bestTitle = bestTitle(docs)
            if (bestTitle) {
                result['hasTitle'] = bestTitle
            }
        }

        Classification.moveAdditionalDewey(result, docs)

        return result
    }

    // TODO: preserve order? e.g. subject
    private Object mergeField(String field, FieldHandler h, Collection<Doc> docs) {
        Object value = docs.first().workData.get(field)
        def rest = docs.drop(1)
        rest.each {
            value = h.merge(value, it.workData.get(field))
        }
        return value
    }

    private FieldStatus compareDiff(Doc a, Doc b, String field) {
        comparators.getOrDefault(field, DEFAULT).isCompatible(a.workData.get(field), b.workData.get(field))
                ? FieldStatus.COMPATIBLE
                : FieldStatus.DIFF
    }

    private FieldStatus compareExact(Object oa, Object ob, String field) {
        c.isEqual([(field): oa], [(field): ob]) ? FieldStatus.EQUAL : FieldStatus.DIFF
    }

    static Map<FieldStatus, List<String>> compare(Collection<Doc> cluster) {
        WorkComparator c = new WorkComparator(allFields(cluster))

        Map<FieldStatus, List<String>> result = [:]
        c.fieldStatuses(cluster).each { f, s -> result.get(s, []) << f }
        return result
    }

    static Set<String> allFields(Collection<Doc> cluster) {
        Set<String> fields = new HashSet<>()
        cluster.each { fields.addAll(it.workData.keySet()) }
        return fields - ignore
    }

    Map<String, FieldStatus> fieldStatuses(Collection<Doc> cluster) {
        fields.collectEntries { [it, fieldStatus(cluster, it)] }
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

}