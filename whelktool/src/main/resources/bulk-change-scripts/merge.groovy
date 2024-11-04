import whelk.Whelk
import whelk.datatool.bulkchange.Specification

import static whelk.datatool.bulkchange.BulkJobDocument.DEPRECATE_KEY
import static whelk.datatool.bulkchange.BulkJobDocument.KEEP_KEY

List<String> deprecate = parameters.get(DEPRECATE_KEY)
String keep = parameters.get(KEEP_KEY)

Specification.Merge merge = new Specification.Merge(deprecate, keep)

selectByIds(merge.getDependers(getWhelk())) {
    if (merge.relink(it.graph, it.whelk)) {
        it.scheduleSave(loud: isLoudAllowed)
    }
}

selectByIds(deprecate) {
    it.scheduleDelete()
}

selectByIds([keep]) {
    merge.addSameAsLinks((Map<String, Object>) it.graph[1], (Whelk) it.whelk)
    it.scheduleSave()
}

