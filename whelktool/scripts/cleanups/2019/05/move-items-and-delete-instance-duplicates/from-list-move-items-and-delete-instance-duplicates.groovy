/*
 * This script moves holdings from one instance to another specified by
 * the file id_lxl-2281_eboksberikning.txt. The holdings are moved from
 * the instance in the right column to the instance in the left column.
 * If the move is successful, the record in the right column is deleted.
 *
 * See LXL-2380 for more info.
 *
 */

PrintWriter failedIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForChange = getReportWriter("scheduledForChange")
PrintWriter info = getReportWriter("info")

def file = new File(scriptDir, "id_lxl-2281_eboksberikning.txt");

file.eachLine { line ->
    def (goodBibId, badBibId) = line.split(";").collect { it.trim() }

    def fullGoodBibId = "http://libris.kb.se/bib/${goodBibId}" as String

    //Create a map of sigels mapping to items held by the good instance
    def sigelToGoodHoldingMap = [:]

    selectByIds([fullGoodBibId]) { goodBib ->
        def goodInstanceId = {goodBib.graph[1][ID]}
        info.println("Good bib ${goodInstanceId} is a ${goodBib.graph[1][TYPE]}")
        selectBySqlWhere("""
                data#>>'{@graph,1,itemOf,@id}' = '${goodInstanceId}' AND
                collection = 'hold'
        """, silent: true) { hold ->
            def heldBy = hold.graph[1].heldBy[ID]
            def holdId = hold.graph[0][ID]
            if (sigelToGoodHoldingMap.containsKey(heldBy)) {
                info.println("Warning, instance (${goodInstanceId}) has multiple holdings " +
                        "for the same sigel (${heldBy}) ignoring duplicate item ${holdId}")
            } else {
                sigelToGoodHoldingMap.put(heldBy, holdId)
            }
        }

        selectByIds([badBibId]) { badBib ->
            def badInstanceId = badBib.graph[1][ID]
            info.println("Bad bib ${badInstanceId} is a ${badBib.graph[1][TYPE]}")

            selectBySqlWhere("""
                data#>>'{@graph,1,itemOf,@id}' = '${badInstanceId}' AND
                collection = 'hold'
        """, silent: true) { hold ->

                def sigel = hold.graph[1].heldBy[ID]
                def holdId = hold.graph[0][ID]

                try {
                    //If the sigel holding this item also has a holding on the good instance
                    //then delete the holding on the good instance before moving all holdings
                    if (sigelToGoodHoldingMap.containsKey(sigel)) {
                        def holdingToDeleteId = sigelToGoodHoldingMap.get(sigel)
                        selectByIds([holdingToDeleteId]) { holdingToDelete ->
                            holdingToDeleteId.scheduleDelete(loud: true)
                            scheduledForChange.println "For item held by ${sigel}:\n" +
                                    "DELETE HOLD ${holdingToDeleteId} on instance <${goodInstanceId}>"
                        }
                    }

                    //Move item from bad to good instance
                    hold.graph[1].itemOf = [(ID): goodInstanceId]
                    hold.scheduleSave(onError: { e ->
                        failedIDs.println("Failed to save ${holdId} due to: $e")
                    }, loud: true)
                    scheduledForChange.println "For item held by ${sigel}:\n" +
                            "CHANGE HOLD <${holdId}> itemOf TO: <${goodInstanceId}> " +
                            "(FROM: <${badInstanceId}>)"
                } catch (Exception e) {
                    failedIDs.println "Failed to delete ${holdingToDeleteId} due to: $e, not moving" +
                            "${holdId} from ${badInstanceId}"
                }
            }

            //Delete bad record now that the holdings have been moved
            badBib.scheduleDelete(onError: { e ->
                failedIDs.println("Failed to delete ${badInstanceId} due to: $e")
            }, loud: true)
            scheduledForChange.println "DELETE BIB <${badInstanceId}> (kept <${goodInstanceId}>)"
        }
    }
}