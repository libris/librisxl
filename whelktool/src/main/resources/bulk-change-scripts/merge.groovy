import whelk.util.DocumentUtil

import static java.util.Collections.synchronizedSet
import static whelk.JsonLd.ID_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY

List<String> deprecate = parameters.get(DEPRECATE_KEY)
String keep = parameters.get(KEEP_KEY)

Set<String> allObsoleteThingUris = synchronizedSet([] as Set<String>)
selectByIds(deprecate) { obsoleteDoc ->
    def obsoleteThingUris = obsoleteDoc.doc.getThingIdentifiers()
    selectByIds(obsoleteDoc.getDependers()) { dependingDoc ->
        def modified = DocumentUtil.traverse(dependingDoc.graph) { value, path ->
            // What if there are links to a record uri?
            if (path && path.last() == ID_KEY && obsoleteThingUris.contains(value)) {
                return new DocumentUtil.Replace(keep)
            }
        }
        if (modified) {
            dependingDoc.scheduleSave(loud: isLoudAllowed)
        }
    }
    allObsoleteThingUris.addAll(obsoleteThingUris)
}

selectByIds(deprecate) {
    it.scheduleDelete()
}

selectByIds([keep]) {
    allObsoleteThingUris.each {uri ->
        it.doc.addThingIdentifier(uri)
    }
    it.scheduleSave()
}

