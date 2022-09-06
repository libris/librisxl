// MUST be run with --allow-loud

File bibsToRevert = new File(scriptDir, "Linkserv-bib-220509-1.txt")

selectByIds( bibsToRevert.readLines() ) { it ->
    it.scheduleRevertTo(loud:true, time:"2022-05-08T14:30:00Z")
}
