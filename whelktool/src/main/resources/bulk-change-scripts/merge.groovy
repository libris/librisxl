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
Set<String> dependsOnObsolete = []

selectByIds([deprecateId]) { obsolete ->
    // Assert that the resource to deprecate is not the same as the one to be kept
    if (obsolete.doc.getThingIdentifiers().first() == keepId) {
        return
    }

    obsoleteThingUris = obsolete.doc.getThingIdentifiers()
    dependsOnObsolete = obsolete.getDependers()
}

selectByIds(dependsOnObsolete) { depender ->
    List<List> modifiedListPaths = []
    def modified = DocumentUtil.traverse(depender.graph) { value, path ->
        // TODO: What if there are links to a record uri?
        if (path && path.last() == ID_KEY && obsoleteThingUris.contains(value)) {
            def pathToLink = path.dropRight(1)
            if (pathToLink.last() != DEPRECATE_KEY) {
                if (pathToLink.last() instanceof Integer) {
                    modifiedListPaths.add(pathToLink.dropRight(1))
                }
                return new DocumentUtil.Replace(keepId)
            }
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

selectByIds([deprecateId]) { obsolete ->
    if (obsolete.doc.getThingIdentifiers().first() != keepId) {
        obsolete.scheduleDelete()
    }
}

selectByIds([keepId]) { kept ->
    obsoleteThingUris.each { uri ->
        kept.doc.addThingIdentifier(uri)
        kept.scheduleSave()
    }
}

