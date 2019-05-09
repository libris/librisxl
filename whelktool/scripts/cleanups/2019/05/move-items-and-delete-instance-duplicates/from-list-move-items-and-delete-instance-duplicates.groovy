/*
 * This script moves holdings from one instance to another specified by
 * the file id_lxl-2281_eboksberikning.txt The holdings are moved from
 * the instance in the right column to the instance in the left column.
 *
 * See LXL-2380 for more info.
 *
 */

PrintWriter failedIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForChange = getReportWriter("scheduledForChange")
PrintWriter info = getReportWriter("info")

def file = new File(scriptDir, "id_lxl-2281_eboksberikning.txt");

file.eachLine { line ->
    def (goodInstanceId, badInstanceId) = line.split(";").collect { it.trim() }

    def fullGoodInstanceId = "http://libris.kb.se/bib/${goodInstanceId}" as String

    //Create a map of sigels mapping to items held by the good instance
    def sigelToGoodHoldingMap = [:]

    selectByIds([fullGoodInstanceId]) { goodBib ->
        info.println("Good bib ${fullGoodInstanceId} is a ${goodBib.graph[1][TYPE]}")
        selectBySqlWhere("""
                data#>>'{@graph,1,itemOf,@id}' = '${goodBib.graph[1][ID]}' AND
                collection = 'hold'
        """, silent: true) { hold ->
            def heldBy = hold.graph[1].heldBy[ID]
            if (sigelToGoodHoldingMap.containsKey(heldBy)) {
                info.println("Warning, instance (${fullGoodInstanceId}) has multiple holdings " +
                        "for the same sigel (${heldBy}) ignoring duplicate item ${hold.graph[0][ID]}")
            } else {
                sigelToGoodHoldingMap.put(heldBy, hold)
            }
        }
    }

    selectByIds([badInstanceId]) { badInstance ->
        info.println("Bad bib ${badInstanceId} is a ${badInstance.graph[1][TYPE]}")
        def fullBadInstanceId = badInstance.graph[1][ID]

        selectBySqlWhere("""
                data#>>'{@graph,1,itemOf,@id}' = '${fullBadInstanceId}' AND
                collection = 'hold'
        """, silent: true) { hold ->

            //If the sigel holding this item also has a holding on the good instance
            //then delete the holding on the good instance before moving all holdings
            def sigel = hold.graph[1].heldBy[ID]
            def holdId = hold.graph[0][ID]

            try {
                if (sigelToGoodHoldingMap.containsKey(sigel)) {
                    def holdingToDelete = sigelToGoodHoldingMap.get(sigel)
                    def holdingToDeleteId = holdingToDelete.graph[1][ID]
                    holdingToDelete.scheduleDelete(loud: true)
                    scheduledForChange.println "For item held by ${sigel}:\n" +
                            "DELETE HOLD ${holdingToDeleteId} on instance <${fullGoodInstanceId}>)"
                }

                //Move item from bad to good instance
                hold.graph[1].itemOf = [(ID): fullGoodInstanceId]
                hold.scheduleSave(onError: { e ->
                    failedIDs.println("Failed to save ${holdId} due to: $e")
                }, loud: true)
                scheduledForChange.println "For item held by ${sigel}:\n" +
                        "CHANGE HOLD <${holdId}> itemOf TO: <${fullGoodInstanceId}> " +
                        "(FROM: <${badInstanceId}>)"
            } catch (Exception e) {
                failedIDs.println "Failed to delete ${holdingToDeleteId} due to: $e, not moving" +
                        "${holdId} from ${badInstanceId}"
            }
        }

        //Delete bad instance now that the holdings have been moved
        badInstance.scheduleDelete(onError: { e ->
            failedIDs.println("Failed to delete ${badInstanceId} due to: $e")
        }, loud: true)
        scheduledForChange.println "DELETE BIB <${badInstanceId}> (kept <${fullGoodInstanceId}>)"
    }
}


