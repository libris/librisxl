/*
 * This copies uri of digitized version from MARC field 852 (hold) to field 856 (bib)
 *
 * See LXL-2625 for more info.
 *
 */

URI_REGEXP = /.*(http:\/\/urn.kb.se\/resolve\?urn=urn:nbn:se:alvin:portal:record-\d+).*/

PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
File bibIDsFile = new File(scriptDir, 'eod_utan_856_001.txt')

selectByIds(bibIDsFile.readLines().collect { 'http://libris.kb.se/bib/' + it }) { bib ->
    try {
        insertUri(bib.doc.data, getAlvinUri(bib.doc.getURI()))
        scheduledForUpdate.println("${bib.doc.getURI()}")
        bib.scheduleSave()
    }
    catch(Exception e) {
        failedBibIDs.println("Failed to update ${bib.doc.shortId} due to: $e")
    }
}

String getAlvinUri(bibId) {
    where = "data #>> '{@graph,1,itemOf,@id}' = '${bibId}#it' " +
         """
         AND collection = 'hold' 
         AND deleted = false
         AND (
               data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/U' 
            OR data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Uka'
            )
         """

    String uri
    selectBySqlWhere(where, silent: false) { hold ->
        def matcher = hold.doc.data['@graph'][1].toString() =~ URI_REGEXP
        if (matcher.matches()) {
            uri = matcher.group(1)
        }
    }

    if (uri == null) {
        throw new RuntimeException("No URI found")
    }

    return uri
}

void insertUri(docData, String uri) {
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