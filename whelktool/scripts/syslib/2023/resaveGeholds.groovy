// MUST be run with --allow-loud

File holdsToResave = new File(scriptDir, "ge_holds_resave.txt")

selectByIds( holdsToResave.readLines() ) { hold ->
    hold.scheduleSave(loud: true)
}