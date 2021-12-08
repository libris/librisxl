PrintWriter failedUpdating = getReportWriter("failed-updating.txt")
File HoldIDs = new File(scriptDir, "KB_ebok_ID.txt")

List propertiesToCheck = ["associatedMedia", "isPrimaryTopicOf", "electronicLocator"]

selectByIds(HoldIDs.readLines() )  { hold ->
    def item = hold.graph[1]
    
    propertiesToCheck.each { prop ->
        if (item[prop] instanceof List && item[prop].size() > 0) {
            item[prop].each { it ->
                boolean shouldChange = false
                asList(it["marc:publicNote"]).each { note ->
                    if (note instanceof String && note.contains("Lokalt tillgänglig på KB")) {
                        shouldChange = true
                    }
                }

                if (shouldChange) {
                    it["marc:publicNote"] = ["E-boken är inte längre tillgänglig på Kungliga biblioteket"]
                    it["cataloguersNote"] = ["E-boksdatorn/servern stängd 2019 p.g.a. tekniska problem"]
                }
            }
        } else if (item[prop] instanceof Map && item[prop]["marc:publicNote"]) {
            item[prop]["marc:publicNote"] = ["E-boken är inte längre tillgänglig på Kungliga biblioteket"]
            item[prop]["cataloguersNote"] = ["E-boksdatorn/servern stängd 2019 p.g.a. tekniska problem"]
        }
    }

    hold.scheduleSave(loud: true, onError: { e ->
        failedUpdating.println("Failed to update ${hold.doc.shortId} due to: $e")
    })
}

private List asList(Object o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}