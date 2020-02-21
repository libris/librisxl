PrintWriter failedHoldIDs = getReportWriter("failed-holdIDs")
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

File bibids = new File(scriptDir, "informalProgram.tsv")
List<String> ProgramLines = bibids.readLines()

for (String operation : ProgramLines) {
    // Examples of operations:
    // (OCoLC)695585517	HdiE	https://ezp.sub.su.se/login?url=https://search.proquest.com/docview/2090304880
    // (OCoLC)695585519	Ldig	http://ludwig.lub.lu.se/login?url=https://search.proquest.com/docview/2090320704
    // (OCoLC)695585519	Gdig	http://ezproxy.ub.gu.se/login?url=https://search.proquest.com/docview/2090320704
    // (OCoLC)695585519	Udig	http://ezproxy.its.uu.se/login?url=https://search.proquest.com/docview/2090320704

    String[] part = operation.split("\\t")
    String oclcId = part[0]
    String sigel = part[1]
    String newUri = part[2]

    String where = "id in (\n" +
            "with bibUris as\n" +
            "(\n" +
            "select data#>>'{@graph,1,@id}' from lddb where \n" +
            "data#>'{@graph,0,identifiedBy}' @> '[{\"@type\":\"SystemNumber\", \"value\":\"${oclcId}\"}]'\n" +
            ")\n" +
            "select id from lddb lh\n" +
            "where lh.data#>>'{@graph,1,itemOf,@id}' in (select * from bibUris)\n" +
            "and\n" +
            "lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/${sigel}'" +
            ")"

    selectBySqlWhere(where, silent: true) { hold ->

        //hold.scheduleRevertTo(loud:false, time:"2020-02-12T07:00:00Z")

        if (hold.graph[1].associatedMedia[0].uri) {
            if (hold.graph[1].associatedMedia[0].uri instanceof List) {
                hold.graph[1].associatedMedia[0].uri.clear()
                hold.graph[1].associatedMedia[0].uri.add(newUri)
            } else {
                hold.graph[1].associatedMedia[0].uri = newUri
            }

            scheduledForUpdating.println("${hold.doc.getURI()}")
            hold.scheduleSave(loud: true, onError: { e ->
                failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
            })
        } else
            failedHoldIDs.println("Failed to update ${hold.doc.shortId}, it did not have the expected @graph,1,associatedMedia,0,uri")

    }
}