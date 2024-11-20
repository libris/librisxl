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
def (linked, blank) = removeSubdivision.split { it[ID_KEY] }
linked.each { l ->
    selectByIds(linked.collect { it[ID_KEY] }) {
        def dependers = it.getDependers() as Set<String>
        if (ids.isEmpty()) {
            ids.addAll(it.getDependers())
        } else {
            ids = ids.intersect(dependers)
        }
    }
}
if (!blank.isEmpty()) {
    Whelk whelk = getWhelk()
    /*
    Querying records containing the given combination of blank subdivisions is very slow so we have to run a separate
    query for each subdivision. However the maximum number of results from a Sparql query is 100k so if we just take the
    intersection of each result we risk missing some records. Better to just save the result with least hits.
     */
    blank.collect { whelk.sparqlQueryClient.queryIdsByPattern(toTurtleData((Map) it, whelk.jsonld.context)) }
            .min { it.size() }
            .with {
                if (ids.isEmpty()) {
                    ids.addAll(it)
                } else {
                    ids = ids.intersect(it)
                }
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