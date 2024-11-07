import whelk.JsonLd
import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY

List deprecateLinks = asList(parameters.get(DEPRECATE_KEY))
Map keepLink = parameters.get(KEEP_KEY)

println(parameters)

deprecateLinks.each { deprecate ->
    selectByIds([deprecate[ID_KEY]]) { obsolete ->
        selectByIds(obsolete.getDependers()) { depender ->
            Map thing = depender.graph[1] as Map

            if (thing[JsonLd.TYPE_KEY] == 'ComplexSubject') {
                return
            }

            def modified = DocumentUtil.traverse(thing) { value, path ->
                if (value instanceof Map && value[JsonLd.TYPE_KEY] == 'ComplexSubject') {
                    var t = asList(value.get('termComponentList'))
                    if (deprecate in t) {
                        // TODO? add way to do this with an op? SplitReplace? [Replace, Insert]?
                        if (keepLink && path.size() > 1) {
                            var parent = DocumentUtil.getAtPath(thing, path.dropRight(1))
                            if (parent instanceof List && !parent.contains(keepLink)) {
                                parent.add(keepLink)
                            }
                        }

                        return mapSubject(value, t, deprecate)
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