/*
 * This deletes holdings for sigel Z where bib is an electronic resource from UR
 *
 * See LXL-2652 for more info.
 *
 */

PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")

where = """
id in 
(
    select hold.id
    from
        lddb as hold,
        lddb as bib,
        lddb__identifiers
    where hold.collection = 'hold'
    and hold.data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Z'
    and hold.deleted = false
    and hold.data #>> '{@graph,1,itemOf,@id}' = lddb__identifiers.iri
    and lddb__identifiers.id = bib.id
    and bib.deleted = false
    and bib.data #>>'{@graph,1,marc:mediaTerm}' = 'Elektronisk resurs'
    and bib.data #>>'{@graph,0,bibliography,0,sigel}' = 'UR'
)
"""

selectBySqlWhere(where, silent: false) { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
}