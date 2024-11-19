/**
 * Remove all uses of a certain Subdivision within ComplexSubject
 * The Subdivision itself is not removed, only the usages.
 *
 * Parameters:
 * bulk:removeSubdivision - The subdivision(s) to be removed
 * bulk:addSubject - If specified, add this regular Subject to :subject instead
 */


import whelk.JsonLd
import whelk.Whelk
import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.asList
import static whelk.converter.JsonLDTurtleConverter.toTurtleData
import static whelk.datatool.bulkchange.BulkJobDocument.ADD_SUBJECT_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.REMOVE_SUBDIVISION_KEY

String inScheme
List<Map> removeSubdivision = asList(parameters.get(REMOVE_SUBDIVISION_KEY)).collect {
    Map copy = new HashMap((Map) it)
    inScheme = copy.remove('inScheme')
    return copy
}
Map addSubject = parameters.get(ADD_SUBJECT_KEY)

def process = { doc ->
    Map thing = doc.graph[1] as Map

    if (thing[JsonLd.TYPE_KEY] == 'ComplexSubject') {
        return
    }

    Set<List> modifiedListPaths = [] as Set
    def modified = DocumentUtil.traverse(thing) { value, path ->
        if (value instanceof Map && value[JsonLd.TYPE_KEY] == 'ComplexSubject') {
            var t = asList(value.get('termComponentList'))
            if ((!inScheme || inScheme == value['inScheme']) && t.containsAll(removeSubdivision)) {
                var parentPath = path.size() > 1 ? path.dropRight(1) : null
                if (parentPath) {
                    var parent = DocumentUtil.getAtPath(thing, parentPath)
                    if (parent instanceof List) {
                        modifiedListPaths.add(parentPath)
                        if (addSubject) {
                            parent.add(addSubject)
                        }
                    }
                }

                return mapSubject(value, t, removeSubdivision)
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

Set<String> ids = [] as Set
removeSubdivision.each { subdivision ->
    if (subdivision[ID_KEY]) {
        selectByIds([subdivision[ID_KEY]]) { obsoleteSubdivision ->
            ids = ids.intersect(obsoleteSubdivision.getDependers()) as Set<String>
        }
    } else {
        Whelk whelk = getWhelk()
        ids = ids.intersect(whelk.sparqlQueryClient.queryIdsByPattern(toTurtleData((Map) subdivision, whelk.jsonld.context)))
    }
}

selectByIds(ids) {
    process(it)
}

static DocumentUtil.Operation mapSubject(Map complexSubject, termComponentList, removeSubdivision) {
    var t2 = termComponentList.findAll { !removeSubdivision.contains(it) }
    if (t2.size() == 0) {
        return new DocumentUtil.Remove()
    }
    if (t2.size() == 1) {
        def remaining = t2.first()
        if (complexSubject['inScheme']) {
            remaining['inScheme'] = complexSubject['inScheme']
        }
        return new DocumentUtil.Replace(remaining)
    }

    Map result = new HashMap(complexSubject)
    result.termComponentList = t2
    return new DocumentUtil.Replace(result)
}