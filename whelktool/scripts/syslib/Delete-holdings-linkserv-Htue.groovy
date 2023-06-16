String SIGEL_TO_DELETE = 'Htue'
String HOLD_ID_FILE = 'Htue_ids.txt'

File holdsToRemove = new File(scriptDir, HOLD_ID_FILE)

selectByIds( holdsToRemove.readLines() ) { hold ->
    if (hold.doc.getHeldBySigel() == SIGEL_TO_DELETE) {
        hold.scheduleDelete(loud: true)
    }
}
