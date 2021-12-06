PrintWriter NoMatch = getReportWriter("T-No-Match")
PrintWriter MulBib = getReportWriter("T-MUL-BIB.txt")
PrintWriter failedHoldIDs = getReportWriter("T-Failed-to-delete-holdIDs.txt")

File bibids = new File(scriptDir, "T_total_id.txt")
List<String> ProgramLines = bibids.readLines()

for (String operation : ProgramLines) {
    String fuzzyID = operation.trim().toUpperCase()

    if (fuzzyID ==~ /^\(\D{6}\)(.*)/) {
    String ctrlnr = fuzzyID.replaceFirst(/^\(\D{6}\)/, "")

    where = """id in
        (select lb.id
        from lddb lb
        where lb.collection = 'bib' and
        lb.data#>>'{@graph,0,controlNumber}' = '${ctrlnr}'
    )"""
    }
    else {

    where = """id in
        (select lb.id
        from lddb lb
        where lb.collection = 'bib' and
        lb.data#>'{@graph,0,identifiedBy}' @> '[{\"@type\": \"LibrisIIINumber\", \"value\":\"$fuzzyID\"}]'
    )"""
    }

    List bibIds = []
                
    selectBySqlWhere(where, { bib ->
        bibIds << bib.doc.getShortId()
    })

    if (bibIds.isEmpty()) {
        // Om inga bib-poster kunde hittas, logga och hoppa vidare till nästa rad i for-loopen
        NoMatch.println("$fuzzyID\tno matching record")
        continue
    }

    if (bibIds.size() > 1) {
    // Om flera bib-poster matchar, logga och hoppa sen vidare till nästa rad i for-loopen
        MulBib.println("$fuzzyID\tMULTIPLE HITS\t$bibIds")
        continue
    }

    // Om exakt en bib-post matchar kan vi gå fortsätta med denna
    selectByIds(bibIds, {bib ->
    def bibMainEntity = bib.graph[1]["@id"]

         selectBySqlWhere("""
            data#>>'{@graph,1,itemOf,@id}' = '${bibMainEntity}' AND
            collection = 'hold' AND
            data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/T'
        """, silent: true) { hold ->

            hold.scheduleDelete(onError: { e ->
                failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
            })
        }
    })
}
