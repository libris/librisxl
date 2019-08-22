/*
 * This changes the type of local geographic subjects from Place to Geographic
 *
 * See LXL-1621 for more info.
 *
 */

String INCORRECT_TYPE = 'Place'
String CORRECT_TYPE = 'Geographic'
PrintWriter scheduledToUpdateGeographicSubject = getReportWriter("scheduled-to-update-geographic-subject")


selectBySqlWhere("""
        data#>>'{@graph,2,subject}' LIKE '%${INCORRECT_TYPE}%' AND collection = 'bib'
    """) { data ->

    def (record, instance, work) = data.graph

    if (work.subject.any{ it[TYPE] == INCORRECT_TYPE}) {
        work.subject.each {
            if (it[TYPE] == INCORRECT_TYPE)
                it[TYPE] = CORRECT_TYPE
        }
        scheduledToUpdateGeographicSubject.println("${record[ID]}")
        data.scheduleSave()
    }

}
