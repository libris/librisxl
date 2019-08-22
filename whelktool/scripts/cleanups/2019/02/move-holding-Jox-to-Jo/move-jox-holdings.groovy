/*
 * This moves all holdings for Jox to Jo.
 *
 * See LXL-2248 for more info.
 *
 */
String CURRENT_SIGEL = 'https://libris.kb.se/library/Jox'
String NEW_SIGEL = 'https://libris.kb.se/library/Jo'

PrintWriter scheduledForMove = getReportWriter("scheduled-for-move")
PrintWriter notMoved = getReportWriter("refused-to-move")

selectBySqlWhere("""
        collection = 'hold' AND
        data#>>'{@graph,1,heldBy,@id}' = '${CURRENT_SIGEL}'
    """, silent: false) { hold ->
    def (record, thing) = hold.graph
    boolean wouldCreateDupe = false

    selectBySqlWhere("""
        collection = 'hold' AND
        data#>>'{@graph,1,itemOf,@id}' = '${thing["itemOf"][ID]}' AND
        data#>>'{@graph,1,heldBy,@id}' = '$NEW_SIGEL'
    """, silent: true) { other_hold ->
        wouldCreateDupe = true
    }

    if (wouldCreateDupe) {
        notMoved.println("Not moving ${hold.doc.getURI()}, since it would create a duplicate.")
    } else {
        thing['heldBy']['@id'] = NEW_SIGEL
        scheduledForMove.println("${hold.doc.getURI()}")
        hold.scheduleSave(loud: true)
    }
}
