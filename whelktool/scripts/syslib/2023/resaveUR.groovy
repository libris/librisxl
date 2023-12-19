// MUST be run with --allow-loud

File holdsToResave = new File(scriptDir, "Elan.txt")

selectByIds( holdsToResave.readLines() ) { hold ->
    hold.scheduleSave(loud: true)
}
