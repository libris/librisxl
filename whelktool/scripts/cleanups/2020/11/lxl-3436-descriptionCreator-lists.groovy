PrintWriter manualReview = getReportWriter("manualReview")

String where = "collection = 'bib' and jsonb_typeof( data#>'{@graph,0,descriptionCreator}' ) = 'array'"

// Human-decided special cases:
String specialCasesString = """
9tmrfjlm56qfjq1 https://libris.kb.se/library/A
q82bv9s206154f9 https://libris.kb.se/library/R
tc565xc50g4hppm https://libris.kb.se/library/Mo
9tmpphmm2j5fpgf https://libris.kb.se/library/Skov
zh9jnnw953smwrg https://libris.kb.se/library/Afr
xg8g44q81ksn91b https://libris.kb.se/library/SEK
l3wnrs2x2xqrgjh https://libris.kb.se/library/T
1jb727gc055gj17 https://libris.kb.se/library/T
6qjng75j3pb1c78 https://libris.kb.se/library/Hig
3mfpswwf4cw8hrs https://libris.kb.se/library/Hig
6qjmc7cj4pz535q https://libris.kb.se/library/LnuV
s9333tp417dmk53 https://libris.kb.se/library/Udig
m5zd80nz4t5cxm8 https://libris.kb.se/library/Skov
zg8w95t932wmj1c https://libris.kb.se/library/Og
l3wwzm6x40pghfb https://libris.kb.se/library/Lbio
9tmm2ndm5b0cvrz https://libris.kb.se/library/Udig
fzr53rsr5hgx2v2 https://libris.kb.se/library/Za
sb454sk438mjz9v https://libris.kb.se/library/Mo
dxq6d9kq4ts15h4 https://libris.kb.se/library/Skov
vd66xtp64dbsp0w https://libris.kb.se/library/Mo
9slkgfcm0tp5fls https://libris.kb.se/library/Lfa
2ldfnscd0w4zzkw https://libris.kb.se/library/Mo
q710bmt25t7nfsx https://libris.kb.se/library/Lfa
"""

Map specialCases = [:]
specialCasesString.eachLine { line ->
    String[] parts = line.split(" ")
    if (parts.length == 2) {
        String docId = parts[0]
        String libUri = parts[1]
        specialCases.put(docId, libUri)
    }
}

selectBySqlWhere(where) { data ->

    // The overwhelming majority:
    if ( data.graph[0].descriptionCreator.any { it["@id"] == "https://libris.kb.se/library/HdiE" } ) {
        data.graph[0].descriptionCreator = ["@id": "https://libris.kb.se/library/HdiE"]
    } else if ( specialCases.containsKey(data.doc.getShortId()) ) {
        System.err.println("MATCH!, now putting:" + ["@id": specialCases.get(data.doc.getShortId())])
        data.graph[0].descriptionCreator = ["@id": specialCases.get(data.doc.getShortId())]
    } else {
        manualReview.println(data.doc.getURI())
    }

    data.scheduleSave()
}
