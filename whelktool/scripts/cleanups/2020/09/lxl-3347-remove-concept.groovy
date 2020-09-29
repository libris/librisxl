/*
 HERE BE DRAGONS.

 These records collide on URIs due to a bug (being fixed in: https://github.com/libris/librisxl/pull/739)

 This was intended to be impossible, but now that it has happened, unconventional measures need
 to be taken to fix it.

 Before running this script, you have to do the following directly in the database:
 DELETE FROM lddb__identifiers WHERE id = 'khw07zp325hdggw'

 This will "trick" the system into temporarily not "seeing" the conflict, which in turn
 will allow the record to be removed.

 See: https://jira.kb.se/browse/LXL-3347
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

// Resave all records depending on 'khw07zp325hdggw'. The system, looking for the
// linktarget: https://id.kb.se/term/barn/Nya%20tiden, should now _only_ find xv8bf2tg0dsjgnl
// and link everything to that record.
selectBySqlWhere("id in (select id from lddb__dependencies where dependsonid = 'khw07zp325hdggw')") { data ->
    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}

// There should no longer be any records depending on khw07zp325hdggw, and khw07zp325hdggw should no longer
// have any lines reserving https://id.kb.se/term/barn/Nya%20tiden in the identifiers table.
// Therefore, removing it, should now be ok.
selectBySqlWhere("id = 'khw07zp325hdggw'") { data ->
    data.scheduleDelete(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
