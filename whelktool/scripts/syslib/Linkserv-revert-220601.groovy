// MUST be run with --allow-loud

File bibsToRevert = new File(scriptDir, "Linkserv-bib-220601.txt")

selectByIds( bibsToRevert.readLines() ) { it ->
    it.scheduleRevertTo(loud:true, time:"2022-06-01T14:30:00Z")
}
