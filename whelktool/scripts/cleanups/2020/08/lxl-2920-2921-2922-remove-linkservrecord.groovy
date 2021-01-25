PrintWriter scheduledForDeletion = getReportWriter("scheduled-deletes")
PrintWriter failedUpdating = getReportWriter("failed-deletes")

def linkServers = ["EBZ", "SFX", "WaSeSS"]

for (String linkserver : linkServers) {
    String where = "id in\n" +
            "(\n" +
            "select id from lddb l where\n" +
            "(\n" +
            "\tdata#>'{@graph,0,technicalNote,0,label}' @> '[\"£LINKSERV\"]'::jsonb\n" +
            "\tor\n" +
            "\tdata#>>'{@graph,0,technicalNote,0,label}' = '£LINKSERV'\n" +
            ")\n" +
            "and\n" +
            "data#>>'{@graph,0,encodingLevel}' = 'marc:PartialPreliminaryLevel'\n" +
            "and not exists\n" +
            "(\n" +
            "\tselect d.dependsonid from lddb__dependencies d where l.id = d.dependsonid and d.relation = 'itemOf'\n" +
            ")\n" +
            "and created < '2020-01-01' and deleted = false\n" +
            ")"

    where = where.replace("£LINKSERV", linkserver)

    selectBySqlWhere(where) { data ->
        scheduledForDeletion.println("${data.doc.getURI()}")
        data.scheduleDelete(onError: { e ->
            failedUpdating.println("Failed to delete ${data.doc.shortId} due to: $e")
        })
    }
}
