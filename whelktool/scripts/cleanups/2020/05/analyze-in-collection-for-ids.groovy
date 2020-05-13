/* For analysis */

report = getReportWriter("report")

selectByCollection('auth') { auth ->
    def recordId = auth.graph[1][ID]
    report.println("Record ID: $recordId InCollection: ${auth.graph[1]['inCollection']}")
}
