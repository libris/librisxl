import whelk.util.DocumentUtil

import static whelk.JsonLd.ID_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY

List<String> deprecate = parameters.get(DEPRECATE_KEY)
String keep = parameters.get(KEEP_KEY)

Set<String> allDeprecatedThingUris = Collections.synchronizedSet()
selectByIds(deprecate) {
    allDeprecatedThingUris.addAll(it.doc.getThingIdentifiers())
}

selectBySqlWhere("id in (select id from lddb__dependencies where dependsonid in ( '${deprecate.join("','")}' )") { docItem ->
    def modified = DocumentUtil.traverse(docItem.graph) { value, path ->
        if (path && path.last() == ID_KEY && allDeprecatedThingUris.contains(value)) {
            return new DocumentUtil.Replace(keep)
        }
    }
    if (modified) {
        docItem.scheduleSave(loud: isLoudAllowed)
    }
}

selectByIds(deprecate) {
    it.scheduleDelete()
}

selectByIds([keep]) { docItem ->
    allDeprecatedThingUris.each {uri ->
        docItem.doc.addThingIdentifier(uri)
    }
    docItem.scheduleSave()
}

