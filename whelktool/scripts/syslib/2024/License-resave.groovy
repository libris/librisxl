// MUST be run with --allow-loud

File bibsToResave = new File(scriptDir, "License-MODIFIED.txt")

selectByIds( bibsToResave.readLines() ) { bib ->
    bib.scheduleSave(loud: true)
}
