// MUST be run with --allow-loud

File holdsToResave = new File(scriptDir, "umea_export_test.txt")

selectByIds( holdsToResave.readLines() ) { hold ->
    hold.scheduleSave(loud: true)
}