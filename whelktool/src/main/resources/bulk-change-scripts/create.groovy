import whelk.Document
import whelk.component.PostgreSQLComponent.ConflictingHoldException
import whelk.util.DocumentUtil

import static java.util.Collections.synchronizedSet;
import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.RECORD_KEY
import static whelk.JsonLd.RECORD_TYPE
import static whelk.JsonLd.THING_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.TARGET_FORM_KEY
import static whelk.datatool.form.MatchForm.collectFormBNodeIdToPath
import static whelk.datatool.form.MatchForm.collectFormBNodeIdToResourceIds
import static whelk.util.DocumentUtil.traverse

PrintWriter conflictingHoldReport = getReportWriter("holding-conflict.txt")

Map targetForm = parameters.get(TARGET_FORM_KEY)

Map<String, Set<String>> nodeIdMappings = collectFormBNodeIdToResourceIds(targetForm, getWhelk())

if (nodeIdMappings.size() != 1) {
    // Allow only one id list
    return
}

def varyingNodeId = nodeIdMappings.keySet().find()
def varyingNodePath = collectFormBNodeIdToPath(targetForm)[varyingNodeId]
def ids = nodeIdMappings.values().find()

if (varyingNodePath == [] || varyingNodePath == [RECORD_KEY]) {
    // Ids must not apply to thing or record
    return
}

def verifiedUris = synchronizedSet([] as Set)
selectByIds(ids) {
    def (record, thing) = it.graph
    def recordId = record[ID_KEY]
    def thingId = thing[ID_KEY]
    verifiedUris.add(ids.contains(recordId) ? recordId : thingId)
}

clearBulkTerms(targetForm)

def docs = verifiedUris.collect { uri ->
    Map thing = Document.deepCopy(targetForm) as Map
    Map varyingNode = getAtPath(thing, varyingNodePath)
    varyingNode.clear()
    varyingNode[ID_KEY] = uri
    Map record = (Map) thing.remove(RECORD_KEY) ?: [(TYPE_KEY): RECORD_TYPE]
    thing[ID_KEY] = "TEMPID#it"
    record[ID_KEY] = "TEMPID"
    record[THING_KEY] = [(ID_KEY): "TEMPID#it"]
    return create([(GRAPH_KEY): [record, thing]])
}

selectFromIterable(docs) {
    it.scheduleSave(loud: true, onError: { e ->
        if (e instanceof ConflictingHoldException) {
            conflictingHoldReport.println("Failed to create document: ${e.getMessage()}")
        } else {
            throw e
        }
    })
}

static void clearBulkTerms(Map form) {
    traverse(form) { node, path ->
        if (node instanceof Map) {
            node.removeAll { ((String) it.key).startsWith("bulk:") }
            return new DocumentUtil.Nop()
        }
    }
}