import whelk.util.DocumentUtil

String where = """id in (
   SELECT lh.id FROM lddb lh WHERE
   lh.data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Vvt'
   AND
   (
   lh.data#>>'{@graph,1,shelfMark}' like '%obelrummet%'
   OR lh.data#>>'{@graph,1,hasComponent}' like '%obelrummet%'
   OR lh.data#>>'{@graph,1,physicalLocation}' like '%obelrummet%'
   )
   AND lh.deleted = 'false')
"""

selectBySqlWhere(where, silent: false, { hold ->

    DocumentUtil.traverse(hold.graph[1], {value, path ->
        if (value instanceof String) {
            if (value.contains("Nobelrummet")) {
                newLoc = value.replace("Nobelrummet", "Linnésalen")
                new DocumentUtil.Replace(newLoc)
            }
        }
    })

   hold.scheduleSave(loud: true, onError: { e ->
        failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
    })

})