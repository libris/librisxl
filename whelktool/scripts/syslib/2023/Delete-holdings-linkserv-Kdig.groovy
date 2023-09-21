String SIGEL_TO_DELETE = 'Kdig'
String HOLD_ID_FILE = 'Kdig-diff-ID-no-eebo.txt'

File holdsToRemove = new File(scriptDir, HOLD_ID_FILE)

selectByIds( holdsToRemove.readLines() ) { hold ->
    if (hold.doc.getHeldBySigel() == SIGEL_TO_DELETE) {
        hold.scheduleDelete(loud: true)
    }
}
