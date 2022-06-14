/**
 * Removes local entities without the property 'code'.
 *
 * See LXL-3873 for more info.
 */

import whelk.util.Statistics

Statistics statistics = new Statistics(3).printOnShutdown()
String where = """
    collection = 'bib' AND
    deleted = false AND
    EXISTS (
        SELECT * FROM jsonb_array_elements(data->'@graph'->1->'instanceOf'->'classification')
        WHERE value->'code' IS NULL AND value->'@id' IS NULL
    )
"""

selectBySqlWhere(where) { data ->
    def thing = data.graph[1]
    boolean changed = false

    thing.instanceOf?.classification?.removeAll {
        if (!it."@id" && !it.code) {
            changed = true
            if (it.inScheme) {
                if (it.inScheme.code) {
                    statistics.increment("no @id, no code, has inScheme with code", it, data.doc.getShortId())
                } else {
                    statistics.increment("no @id, no code, has inScheme without code", it, data.doc.getShortId())
                }
            } else {
                statistics.increment("no @id, no code, no inScheme", it, data.doc.getShortId())
            }
            return true
        }
        return false
    }

    if (thing.instanceOf?.classification?.isEmpty()) {
        thing.instanceOf.remove("classification")
    }

    if (changed) {
        data.scheduleSave()
    }
}
