PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter failedDeleting = getReportWriter("failed-deletions")

List<Map> GF_ID = [['aukt': 'dbqswz1x416cq0b', 'gfuri': 'https://id.kb.se/term/barngf/Roliga%20b%C3%B6cker'],
                  ['aukt': 'qn2479d831mc7nz', 'gfuri': 'https://id.kb.se/term/barngf/Sorgliga%20b%C3%B6cker']]

GF_ID.each { gf ->

   String aukt
   String gfuri

   String where = """
         collection = 'bib' AND
         deleted = false AND
         id IN (SELECT id FROM lddb__dependencies WHERE dependsonid = '${gf['aukt']}')
       """

   selectBySqlWhere(where) { data ->
      def thing = data.graph[1]

      if (thing.instanceOf?.genreForm) {
         thing.instanceOf?.genreForm.removeIf {it['@id'] == gf['gfuri']}
      }

      if (thing.instanceOf?.subject) {
         thing.instanceOf?.subject.removeIf {it['@id'] == gf['gfuri']}
      }

      data.scheduleSave(onError: { e ->
      failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
      })

   }  

   // Delete the auth records as no references to it should remain
   selectByIds([gf['aukt']]) { auth ->
      auth.scheduleDelete(onError: { e ->
        failedDeleting.println("Failed to delete ${auth.doc.shortId} due to: $e")
      })
   }
}
