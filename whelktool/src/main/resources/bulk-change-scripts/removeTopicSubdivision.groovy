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

Map deprecate = parameters.get(DEPRECATE_KEY)
Map addLink = parameters.get(KEEP_KEY)

if (!deprecate) return

def process = { doc ->
    Map thing = doc.graph[1] as Map

    if (thing[JsonLd.TYPE_KEY] == 'ComplexSubject') {
        return
    }

    List<List> modifiedListPaths = []
    def modified = DocumentUtil.traverse(thing) { value, path ->
        if (value instanceof Map && value[JsonLd.TYPE_KEY] == 'ComplexSubject') {
            var t = asList(value.get('termComponentList'))
            if (deprecate in t) {
                var parentPath = path.size() > 1 ? path.dropRight(1) : null
                if (parentPath) {
                    var parent = DocumentUtil.getAtPath(thing, parentPath)
                    if (parent instanceof List) {
                        modifiedListPaths.add(parentPath)
                        // TODO? add way to do this with an op? SplitReplace? [Replace, Insert]?
                        if (addLink && addLink[ID_KEY]) {
                            parent.add(addLink)
                        }
                    }
                }

                return mapSubject(value, t, deprecate)
            }
        }
        return DocumentUtil.NOP
    }

    // Remove duplicates
    modifiedListPaths.each {
        var obj = DocumentUtil.getAtPath(thing, it)
        if (obj instanceof List) {
            obj.unique(true)
        }
    }

    if (modified) {
        doc.scheduleSave(loud: isLoudAllowed)
    }
}

if (deprecate[ID_KEY]) {
    selectByIds([deprecate[ID_KEY]]) { obsoleteSubdivision ->
        selectByIds(obsoleteSubdivision.getDependers()) {
            process(it)
        }
    }
} else {
    // TODO
//    selectByForm(deprecate) {
//        process(it)
//    }
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