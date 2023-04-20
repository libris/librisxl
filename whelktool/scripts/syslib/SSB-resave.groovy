// MUST be run with --allow-loud

File bibsToResave = new File(scriptDir, "SSB-resave-ids.txt")

selectByIds( bibsToResave.readLines() ) { bib ->
    bib.scheduleSave(loud: true)
}
