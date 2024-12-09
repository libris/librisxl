/**
 * Remove all uses of a certain Subdivision within ComplexSubject
 * The Subdivision itself is not removed, only the usages.
 *
 * Parameters:
 * bulk:removeSubdivision - The subdivision(s) to be removed
 * bulk:addSubject - If specified, add this regular Subject to :subject instead
 */


import whelk.Whelk
import whelk.datatool.DocumentItem
import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.asList
import static whelk.converter.JsonLDTurtleConverter.toTurtleNoPrelude
import static whelk.datatool.bulkchange.BulkJobDocument.ADD_TERM_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.REMOVE_SUBDIVISION_KEY

Whelk whelk = getWhelk()

Map inScheme
List<Map> removeSubdivision = asList(parameters.get(REMOVE_SUBDIVISION_KEY)).collect {
    Map copy = new HashMap((Map) it)
    inScheme = (Map) copy.remove('inScheme')
    return copy
}
Map addTerm = parameters.get(ADD_TERM_KEY)
String addTermType = addTerm ? getType(addTerm) : null

def process = { DocumentItem doc ->
    Map thing = doc.graph[1] as Map

    if (thing[TYPE_KEY] == 'ComplexSubject') {
        return
    }
    Set<List> modifiedListPaths = [] as Set
    def modified = DocumentUtil.traverse(thing) { value, path ->
        if (value instanceof Map && value[TYPE_KEY] == 'ComplexSubject') {
            var t = asList(value.get('termComponentList'))
            if ((!inScheme || inScheme == value['inScheme']) && t.containsAll(removeSubdivision)) {
                var parentPath = path.size() > 1 ? path.dropRight(1) : null
                if (parentPath) {
                    var parent = DocumentUtil.getAtPath(thing, parentPath)
                    if (parent instanceof List) {
                        modifiedListPaths.add(parentPath)
                        if (whelk.jsonld.isSubClassOf(addTermType, 'Subject')) {
                            parent.add(addTerm)
                        } else if (whelk.jsonld.isSubClassOf(addTermType, 'GenreForm')) {
                            var grandParent = DocumentUtil.getAtPath(thing, parentPath.dropRight(1))
                            if (grandParent instanceof Map) {
                                def genreForm = asList(grandParent['genreForm'])
                                if (!genreForm.contains(addTerm)) {
                                    genreForm.add(addTerm)
                                }
                                grandParent['genreForm'] = genreForm
                            }
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
    /*
    Querying records containing the given combination of blank subdivisions is very slow so we have to run a separate
    query for each subdivision. However the maximum number of results from a Sparql query is 100k so if we just take the
    intersection of each result we risk missing some records. Better to just save the result with least hits.
     */
    blank.collect { whelk.sparqlQueryClient.queryIdsByPattern(toTurtleNoPrelude((Map) it, whelk.jsonld.context)) }
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
        if (complexSubject['inScheme'] && !remaining['inScheme'] && !remaining[ID_KEY]) {
            remaining['inScheme'] = complexSubject['inScheme']
        }
        return new DocumentUtil.Replace(remaining)
    }

    Map result = new HashMap(complexSubject)
    result.termComponentList = t2
    return new DocumentUtil.Replace(result)
}

String getType(Map term) {
    if (term[ID_KEY]) {
        String type
        selectByIds([term[ID_KEY]]) {
            type = it.doc.getThingType()
        }
        return type
    }
    return term[TYPE_KEY]
}

