// MUST be run with --allow-loud

File holdsToResave = new File(scriptDir, "Resave_5sigel_ID.txt")

selectByIds( holdsToResave.readLines() ) { hold ->
    hold.scheduleSave(loud: true)
}
