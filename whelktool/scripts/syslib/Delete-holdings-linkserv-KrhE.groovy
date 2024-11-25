String SIGEL_TO_DELETE = 'KrhE'
String HOLD_ID_FILE = 'KrhE_ids.txt'

File holdsToRemove = new File('.', HOLD_ID_FILE)

selectByIds( holdsToRemove.readLines() ) { hold ->
    if (hold.doc.getHeldBySigel() == SIGEL_TO_DELETE) {
        hold.scheduleDelete(loud: true)
    }
}
