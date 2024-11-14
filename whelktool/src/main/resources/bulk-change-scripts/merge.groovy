import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.JOB_TYPE
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY

Map deprecateLink = parameters.get(DEPRECATE_KEY)
Map keep = parameters.get(KEEP_KEY)

String deprecateId = deprecateLink[ID_KEY]
String keepId = deprecateLink[ID_KEY]

if (!deprecateId || !keepId) return

List<String> obsoleteThingUris = []

selectByIds([deprecateId]) { obsolete ->
    obsoleteThingUris = obsolete.doc.getThingIdentifiers()

    selectByIds(obsolete.getDependers()) { depender ->
        if (depender.doc.getThingType() == JOB_TYPE) {
            return
        }
        def modified = DocumentUtil.traverse(depender.graph) { value, path ->
            // TODO: What if there are links to a record uri?
            if (path && path.last() == ID_KEY && obsoleteThingUris.contains(value)) {
                return new DocumentUtil.Replace(keepId)
            }
        }
        if (modified) {
            depender.scheduleSave(loud: isLoudAllowed)
        }
        // Remove duplicate links
        DocumentUtil.traverse(depender.graph) { value, path ->
            if (value instanceof List) {
                value.unique(true) { it instanceof Map ? it[ID_KEY] : it }
                return new DocumentUtil.Nop()
            }
        }
    }

    obsolete.scheduleDelete()
}

selectByIds([keep]) { kept ->
    obsoleteThingUris.each { uri ->
        kept.doc.addThingIdentifier(uri)
        kept.scheduleSave()
    }
}

