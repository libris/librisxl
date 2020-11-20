PrintWriter manualReview = getReportWriter("manualReview")

String where = "collection = 'bib' and jsonb_typeof( data#>'{@graph,0,descriptionCreator}' ) = 'array'"

selectBySqlWhere(where) { data ->

    // The overwhelming majority:
    if ( data.graph[0].descriptionCreator.any { it["@id"] == "https://libris.kb.se/library/HdiE" } ) {
        data.graph[0].descriptionCreator = ["@id": "https://libris.kb.se/library/HdiE"]
    } else {
        manualReview.println(data.doc.getURI())
    }

    data.scheduleSave()
}
