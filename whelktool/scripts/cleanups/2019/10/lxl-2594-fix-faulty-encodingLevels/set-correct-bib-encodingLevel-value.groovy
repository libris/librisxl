/*
 * This removes auth and hold specific values of encodingLevel
 * and set correct valid bib level
 *
 * See LXL-2594 for more info.
 *
 */

PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
VALID_BIB_ENCODING_LEVELS = ["marc:AbbreviatedLevel",
                             "marc:CoreLevel",
                             "marc:FullLevel",
                             "marc:FullLevelMaterialNotExamined",
                             "marc:LessThanFullLevelMaterialNotExamined",
                             "marc:MinimalLevel",
                             "marc:PartialPreliminaryLevel",
                             "marc:PrepublicationLevel"]

selectBySqlWhere("""
        collection = 'bib' AND data#>>'{@graph,0,encodingLevel}' IS NOT NULL
    """) { data ->

    def (record, instance, work) = data.graph

    if (!VALID_BIB_ENCODING_LEVELS.contains(record['encodingLevel'])) {
        record.put('encodingLevel', VALID_BIB_ENCODING_LEVELS[0])
        scheduledForUpdate.println("${record[ID]}")
        data.scheduleSave(onError: { e ->
            failedBibIDs.println("Failed to save ${record[ID]} due to: $e")
        })
    }
}
