import whelk.util.DocumentUtil

PrintWriter report = getReportWriter("issn.txt")

selectByCollection('bib') { doc ->
    DocumentUtil.traverse(doc.graph[1]) { value, path -> 
        if (value instanceof Map && value['@type'] == 'ISSN') {
            report.println("${doc.doc.shortId}\t${value['value']}")
        }
    }
}