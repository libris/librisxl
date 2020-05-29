/**
 * See LXL-3185 for more information
 */

scheduledForDelete = getReportWriter("scheduled-for-delete")
failed = getReportWriter("failed-to-delete-holdIDs")

def bibQuery = """id in (select id from lddb l where 
        data#>>'{@graph,0,descriptionCreator,@id}' = 'https://libris.kb.se/library/Mo' 
        and collection = 'bib' 
        and data#>>'{@graph,0,bibliography}' is null 
        and not exists(select d.dependsonid from lddb__dependencies d where l.id = d.dependsonid and d.relation = 'itemOf') 
        and deleted = false)""".stripIndent()

selectBySqlWhere(bibQuery) { bib ->
    scheduledForDelete.println("${bib.doc.getURI()}")
    bib.scheduleDelete(onError: { e ->
        failed.println("Failed to delete ${bib.doc.shortId} due to: $e")
    })
}