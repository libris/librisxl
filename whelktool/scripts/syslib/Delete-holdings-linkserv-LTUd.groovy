String SIGEL_TO_DELETE = 'LTUd'
String HOLD_ID_FILE = 'LTUd-diff-ID.txt'

File holdsToRemove = new File(scriptDir, HOLD_ID_FILE)

selectByIds( holdsToRemove.readLines() ) { hold ->
    if (hold.doc.getHeldBySigel() == SIGEL_TO_DELETE) {
        hold.scheduleDelete()
    }
}
