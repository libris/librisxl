import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.JOB_TYPE
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY

Map deprecateLink = parameters.get(DEPRECATE_KEY)
Map keepLink = parameters.get(KEEP_KEY)

String deprecateId = deprecateLink[ID_KEY]
String keepId = keepLink[ID_KEY]

if (!deprecateId || !keepId) return

List<String> obsoleteThingUris = []

selectByIds([deprecateId]) { obsolete ->
    obsoleteThingUris = obsolete.doc.getThingIdentifiers()

    selectByIds(obsolete.getDependers()) { depender ->
        if (depender.doc.getThingType() == JOB_TYPE) {
            return
        }

        List<List> modifiedListPaths = []
        def modified = DocumentUtil.traverse(depender.graph) { value, path ->
            // TODO: What if there are links to a record uri?
            if (path && path.last() == ID_KEY && obsoleteThingUris.contains(value)) {
                path.dropRight(1).with {
                    if (it.last() instanceof Integer) {
                        modifiedListPaths.add(it.dropRight(1))
                    }
                }
                return new DocumentUtil.Replace(keepId)
            }
        }
        // Remove duplicates
        modifiedListPaths.each {
            var obj = DocumentUtil.getAtPath(depender.graph, it)
            if (obj instanceof List) {
                obj.unique(true)
            }
        }
        if (modified) {
            depender.scheduleSave(loud: isLoudAllowed)
        }
    }

    obsolete.scheduleDelete()
}

selectByIds([keepId]) { kept ->
    obsoleteThingUris.each { uri ->
        kept.doc.addThingIdentifier(uri)
        kept.scheduleSave()
    }
}

