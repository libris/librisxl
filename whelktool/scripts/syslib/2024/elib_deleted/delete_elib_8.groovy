// MUST be run with --allow-loud

File holdsToDelete = new File(scriptDir, "elib_8.txt")

selectByIds( holdsToDelete.readLines() ) { hold ->
    hold.scheduleDelete(loud: false)
}