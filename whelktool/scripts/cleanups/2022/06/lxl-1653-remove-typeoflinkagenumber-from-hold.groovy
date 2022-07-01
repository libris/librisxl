/*
 * This removes marc:typeOfLinkageNumber and solves both incorrect usage of indicators in 014 and faulty modeling leaving it outside the LibrisIIINumber entity.
 *
 * See LXL-1653 for more info.
 *
 */

PrintWriter failedIDs = getReportWriter("failed-to-update")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectBySqlWhere("""
        collection = 'hold' AND data#>>'{@graph,0,marc:typeOfLinkageNumber}' IS NOT NULL
    """) { data ->

    def (record, thing) = data.graph

    record.remove('marc:typeOfLinkageNumber')
    scheduledForUpdate.println("${record[ID]}")

    data.scheduleSave(onError: { e ->
        failedIDs.println("Failed to save ${record[ID]} due to: $e")
    })
}