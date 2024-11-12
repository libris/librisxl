/**
 * Remove all uses of a certain TopicSubdivision within ComplexSubject
 * The TopicSubdivision itself is not removed, only the usages.
 *
 * Parameters:
 * bulk:deprecate - The subdivision(s) to be removed
 * bulk:keep - If specified, add this regular Topic to :subject instead
 */

import whelk.JsonLd
import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.RDF_VALUE

List deprecateUris = asList(parameters.get(DEPRECATE_KEY))
Map keepUri = parameters.get(KEEP_KEY)

deprecateUris.each { deprecate ->
    Map deprecateLink = [(ID_KEY): deprecate[RDF_VALUE]]
    selectByIds([deprecateLink[ID_KEY]]) { obsoleteSubdivision ->
        selectByIds(obsoleteSubdivision.getDependers()) { depender ->
            Map thing = depender.graph[1] as Map

            if (thing[JsonLd.TYPE_KEY] == 'ComplexSubject') {
                return
            }

            def modified = DocumentUtil.traverse(thing) { value, path ->
                if (value instanceof Map && value[JsonLd.TYPE_KEY] == 'ComplexSubject') {
                    var t = asList(value.get('termComponentList'))
                    if (deprecate in t) {
                        // TODO? add way to do this with an op? SplitReplace? [Replace, Insert]?
                        if (keepUri && path.size() > 1) {
                            var keepLink = [(ID_KEY): keepUri[RDF_VALUE]]
                            var parent = DocumentUtil.getAtPath(thing, path.dropRight(1))
                            if (parent instanceof List && !parent.contains(keepLink)) {
                                parent.add(keepLink)
                            }
                        }

                        return mapSubject(value, t, deprecateLink)
                    }
                }
                return DocumentUtil.NOP
            }

            if (modified) {
                depender.scheduleSave(loud: isLoudAllowed)
            }
        }
    }
}

static DocumentUtil.Operation mapSubject(Map subject, termComponentList, deprecateLink) {
    var t2 = termComponentList.findAll { it != deprecateLink }
    if (t2.size() == 0) {
        return new DocumentUtil.Remove()
    }
    if (t2.size() == 1) {
        return new DocumentUtil.Replace(t2.first())
    }

    Map result = new HashMap(subject)
    result.termComponentList = t2
    return new DocumentUtil.Replace(result)
}