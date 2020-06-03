/**
 * Delete junk nodes from holdings that were created by writing
 * embellished documents through the CRUD API.
 *
 * See LXL-3198 for more information
 */

PrintWriter updatedReport = getReportWriter("updated.txt")
PrintWriter failedReport = getReportWriter("failed.txt")

where = """
    collection = 'hold' 
    and data #>> '{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/D' 
    and jsonb_array_length(data #> '{@graph}') > 2
    """.stripIndent()

selectBySqlWhere(where) { hold ->

    if (hold.graph.size() > 2) {
        hold.doc.data['@graph'] = hold.graph.take(2)

        updatedReport.println("${hold.doc.getURI()}")
        hold.scheduleSave(onError: { e ->
            failedReport.println("Failed to update ${hold.doc.shortId} due to: $e")
        })
    }
}
