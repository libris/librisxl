/* For analysis */

File bibIDsFile = new File(scriptDir, 'has_broader_ids.txt')
report = getReportWriter("report")

selectByIds(bibIDsFile.readLines()) { bib ->
    def recordId = bib.graph[0][ID]
    report.println("Record ID: $recordId InCollection: ${bib.graph[1]['inCollection']}")
}
