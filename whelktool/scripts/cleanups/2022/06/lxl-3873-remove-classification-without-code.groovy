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
        SELECT * FROM jsonb_array_elements(
            CASE jsonb_typeof(data->'@graph'->1->'instanceOf'->'classification')
                WHEN 'array' THEN data->'@graph'->1->'instanceOf'->'classification'
                WHEN 'object' THEN jsonb_build_array(data->'@graph'->1->'instanceOf'->'classification')
                ELSE '[]'::jsonb
            END
        )
        WHERE value->'code' IS NULL AND value->'@id' IS NULL
    )
"""

selectBySqlWhere(where) { data ->
    def thing = data.graph[1]
    boolean changed = false

    if (thing.instanceOf?.classification instanceof Map) {
        thing.instanceOf.classification = [thing.instanceOf.classification]
    }

    thing.instanceOf?.classification?.removeAll {
        if (it instanceof Map && !it."@id" && !it.code) {
            changed = true
            statistics.increment("shapes", it.keySet().toList().sort(), data.doc.getShortId())
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
