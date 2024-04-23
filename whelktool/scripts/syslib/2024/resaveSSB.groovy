// MUST be run with --allow-loud

File holdsToResave = new File(scriptDir, "SSB_id.txt")

selectByIds( holdsToResave.readLines() ) { hold ->
    hold.scheduleSave(loud: true)
}