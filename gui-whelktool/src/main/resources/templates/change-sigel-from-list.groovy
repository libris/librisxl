PrintWriter failedHoldIDs = getReportWriter("failed-to-update-holdIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
File bibids = new File('INPUT')

String bibidstring = bibids.readLines().join("','")

selectBySqlWhere("""id in
(
 select lh.id
  from
   lddb lb
  left join
   lddb lh
  on lh.data#>>'{@graph,1,itemOf,@id}' = lb.data#>>'{@graph,1,@id}'
 where lb.data#>>'{@graph,0,controlNumber}' in ( '$bibidstring' ) and lb.collection = 'bib'
 and
 lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/FROMSIGEL'
)""", silent: false, { hold ->
    scheduledForUpdate.println("${hold.doc.getURI()}")
    def heldBy = hold.graph[1].heldBy

    heldBy["@id"] = 'https://libris.kb.se/library/TOSIGEL'

    if (hold.graph[1].hasComponent) {
        hold.graph[1].hasComponent.each { component ->
            if (component.heldBy) {
                component.heldBy["@id"] = 'https://libris.kb.se/library/TOSIGEL'
            }
        }
    }

    hold.scheduleSave(loud: true, onError: { e ->
        failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
    })
})
