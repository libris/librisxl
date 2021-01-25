PrintWriter failedUpdating = getReportWriter("failed-updating")
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

File HoldIDs = new File(scriptDir, "Udix_ID2.txt")

List propertiesToCheck = ["associatedMedia", "marc:hasTextualHoldingsBasicBibliographicUnit"]

selectByIds( HoldIDs.readLines() )  { hold ->
def holds = hold.graph[1]

propertiesToCheck.each { prop ->
    holds[prop].removeIf { it["marc:publicNote"].contains("Externt magasin / Closed stacks") } 
    
    }

    scheduledForUpdating.println("${hold.doc.getURI()}")
    hold.scheduleSave(loud: true, onError: { e ->
        failedUpdating.println("Failed to update ${hold.doc.shortId} due to: $e")
    
    })
  
}