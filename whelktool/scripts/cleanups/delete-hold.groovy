failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
scheduledForDeletion = getReportWriter("scheduled-for-deletion")
String BIB_ID_FILE = 'bib-id-list'
String SIGEL_FILE = 'sigel-list'
int BATCH_SIZE = 100

List<String> bibIds = new File(scriptDir, BIB_ID_FILE).readLines()
sigel = new File(scriptDir, SIGEL_FILE).readLines().collect{ it.trim() }.join("','")


List<String> batch = []
for (String bibId in bibIds) {
    batch << bibId
    if (batch.size() == BATCH_SIZE) {
        delete(batch)
        batch =  []
    }
}
delete(batch)

void delete(List<String> bibIds) {
    String bibIdString = bibIds.join("','")
    String where = """
        id in 
        ( 
            select l.id from 
            lddb__identifiers i 
            left join lddb__dependencies d on d.dependsonid = i.id and d.relation = 'itemOf' 
            left join lddb l on d.id = l.id 
            where 
            i.iri in ('${bibIdString}') and l.data#>>'{@graph,1,heldBy,@id}' = '$sigel' 
        )
        """.stripIndent()

    selectBySqlWhere(where, silent: false, { hold ->
        scheduledForDeletion.println("${hold.doc.getURI()}")
        //hold.scheduleDelete(onError: { e ->
        //    failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
        //})
    })
}

