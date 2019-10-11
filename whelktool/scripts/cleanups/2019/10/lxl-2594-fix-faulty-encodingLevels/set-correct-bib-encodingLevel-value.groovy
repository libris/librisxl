/*
 * This removes auth and hold specific values of encodingLevel
 * and set correct valid bib level
 *
 * See LXL-2594 for more info.
 *
 */

PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
BIB_ENCODING_LEVEL = "marc:AbbreviatedLevel"

selectBySqlWhere("""
        collection = 'bib' AND 
        (data#>>'{@graph,0,encodingLevel}' = 'marc:HoldingsLevel1' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:HoldingsLevel2' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:HoldingsLevel3' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:HoldingsLevel4' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:HoldingsLevel4WithPieceDesignation' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:MixedLevels' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:OtherLevel' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:CompleteAuthorityRecord' OR
        data#>>'{@graph,0,encodingLevel}' = 'marc:IncompleteAuthorityRecord') AND 
        lddb.deleted = FALSE
    """) { data ->

    def (record, instance, work) = data.graph

    record.put('encodingLevel', BIB_ENCODING_LEVEL)
    scheduledForUpdate.println("${record[ID]}")
    data.scheduleSave(onError: { e ->
        failedBibIDs.println("Failed to save ${record[ID]} due to: $e")
    })
}
