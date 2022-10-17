// MUST be run with --allow-loud

File bibsToResave = new File(scriptDir, "KAO-MODIFIED-Prod.txt")

selectByIds( bibsToResave.readLines() ) { bib ->
    bib.scheduleSave(loud: true)
}
