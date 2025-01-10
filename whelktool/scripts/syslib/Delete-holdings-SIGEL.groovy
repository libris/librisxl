// KP 250110, add -Dsigel=SIGEL to command line

def sigel = System.getProperty("sigel")

if ( sigel == null ) {
	println('set sigel with -Dsigel=SIGEL')
	System.exit(0)
}

String SIGEL_TO_DELETE = sigel
String HOLD_ID_FILE = sigel + '-holds.ids'

File holdsToRemove = new File('.', HOLD_ID_FILE)

selectByIds( holdsToRemove.readLines() ) { hold ->
    if (hold.doc.getHeldBySigel() == SIGEL_TO_DELETE) {
        hold.scheduleDelete(loud: true)
    }
}
