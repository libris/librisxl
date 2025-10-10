// KP 251006, add -Dsigel=SIGEL to command line

def sigel = System.getProperty("sigel")

if ( sigel == null ) {
	println('set sigel with -Dsigel=SIGEL')
	System.exit(0)
}

String SIGEL_TO_RESAVE = sigel
String HOLD_ID_FILE = sigel + '-holds.ids'

File holdsToResave = new File('.', HOLD_ID_FILE)

if ( holdsToResave == null) {
	println("Couldn't find " + HOLD_ID_FILE)
	System.exit(0)
}

println("Resaving holdings for " + SIGEL_TO_RESAVE + " from " + HOLD_ID_FILE)

selectByIds( holdsToResave.readLines() ) { hold ->
    if (hold.doc.getHeldBySigel() == SIGEL_TO_RESAVE) {
        hold.scheduleSave(loud: true)
    }
}
