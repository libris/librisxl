/*
 * Move URI of digitized version from MARC field 852 (hold) to field 856 (bib)
 *
 * See LXL-2625 for more info.
 *
 */

URI_REGEXP = /.*(http:\/\/urn.kb.se\/resolve\?urn=urn:nbn:se:alvin:portal:record-\d+).*/
failedBibIDs = getReportWriter("failed-to-update-bibIDs")
report = getReportWriter("report--bib-hold-uri")

File bibIDsFile = new File(scriptDir, 'eod_utan_856_001.txt')

selectByIds(bibIDsFile.readLines().collect { 'http://libris.kb.se/bib/' + it }) { bib ->
    try {
        Tuple2 holdUri = findAndRemoveAlvinUri(holds(bib.doc.getURI()))
        hold = holdUri.first
        String uri = holdUri.second

        insertUri(bib.doc.data, uri)

        report.println("${bib.doc.getURI()} ${hold.doc.getURI()} ${uri}")
        hold.scheduleSave()
        bib.scheduleSave()
    }
    catch(Exception e) {
        e.printStackTrace()
        failedBibIDs.println("Failed to update ${bib.doc.shortId} due to: $e")
    }
}

private Tuple2 findAndRemoveAlvinUri(List holds) {
    List holdUris = holds.collect{ new Tuple2(it, extractUri(it.doc.data)) }
    holdUris.retainAll{ it.second != null }

    if (holdUris.isEmpty()) {
        throw new RuntimeException("No URI found")
    }
    if (holdUris.size() > 1) {
        throw new RuntimeException("Multiple holds with URI")
    }

    return holdUris[0]
}

private String extractUri(docData) {
    String uri
    def item = docData['@graph'][1]
    if (item['uri']) {
        uri = parseUri(item.remove('uri'))
    } else if (item['hasComponent']) {
        for (i in item['hasComponent']) {
            if (i['uri']) {
                if (uri != null) {
                    throw new RuntimeException("Multiple URIs in hold")
                }
                uri = parseUri(i.remove('uri'))
            }
        }
    }

    return uri
}

private String parseUri(List uris) {
    if (uris.size() > 1) {
        throw new RuntimeException('Multiple URIs in "uri"')
    }
    String uri = uris[0]

    def matcher = uri =~ URI_REGEXP
    if (matcher.matches()) {
        return matcher.group(1)
    }
    else {
        throw new RuntimeException('Unexpected URI format: ' + uri)
    }
}

private void insertUri(docData, String uri) {
    def instance = docData['@graph'][1]

    if (!instance['associatedMedia']) {
        instance['associatedMedia'] = []
    }

    instance['associatedMedia'] <<
        [
            "@type": "MediaObject",
            "uri": [uri],
            "marc:publicNote": ["Fritt tillgänglig via Alvin (Universitetsbiblioteket, Lunds universitet)"]
        ]
}

private List holds(bibId) {
    where = "data #>> '{@graph,1,itemOf,@id}' = '${bibId}#it' " +
            """
         AND collection = 'hold' 
         AND deleted = false
         AND (
               data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/U' 
            OR data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Uka'
            )
         """

    List holds = []
    selectBySqlWhere(where, silent: false) { hold ->
        holds << hold
    }
    return holds
}