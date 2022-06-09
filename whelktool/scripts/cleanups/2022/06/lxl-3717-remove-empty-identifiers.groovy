/**
 * Removes empty identifiers from auth.
 *
 * See LXL-3717 for more info.
 */

String where = """
  collection = 'auth' AND
  deleted = false AND
  data#>>'{@graph,1,identifiedBy}' IS NOT NULL
"""

selectBySqlWhere(where) { data ->
    def thing = data.graph[1]

    if (!thing.identifiedBy) {
        return
    }

    def shouldSave = thing.identifiedBy.removeAll {
        it["@type"] && it.size() == 1
    }

    if (thing.identifiedBy.isEmpty()) {
        thing.remove("identifiedBy")
    }

    if (shouldSave) {
        data.scheduleSave()
    }
}
