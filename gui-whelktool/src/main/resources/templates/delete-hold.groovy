PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
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
 where lb.data#>>'{@graph,0,controlNumber}' in ( '$bibidstring' )
 and
 lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/SIGEL'
)""", silent: false, { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
})
