/**
 * Mark all old MARC works with inCollection to separate the from the new BIBFRAME works.
 *
 * See LXL-3357 for more information
 */

PrintWriter report = getReportWriter("scheduled-for-update.txt")

String where = "data#>>'{@graph,1,@type}' = 'Work' and data#>>'{@graph,1,sameAs}' LIKE '%resource/auth%'"
selectBySqlWhere(where, { auth ->

    auth.graph[1]['inCollection'] = [["@id": "https://id.kb.se/term/uniformWorkTitle"]]

    report.println(auth.doc.shortId)

    auth.scheduleSave()
})