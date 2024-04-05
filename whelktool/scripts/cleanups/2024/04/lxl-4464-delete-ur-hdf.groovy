/*
 * Delete all holdings for Hdf in bibliography UR
 *
 * See LXL-4464 for more info.
 *
 */
failedHoldIDs = getReportWriter("failed-holdIDs")

String where = """
  collection = 'hold' 
  AND deleted = false 
  AND data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Hdf' 
  AND data#>>'{@graph,1,itemOf,@id}' in (
    SELECT data#>>'{@graph,1,@id}' 
    FROM lddb 
    WHERE collection = 'bib' 
    AND deleted = false 
    AND data#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/UR"}]'
  )
"""

selectBySqlWhere(where) { hold ->
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
}
