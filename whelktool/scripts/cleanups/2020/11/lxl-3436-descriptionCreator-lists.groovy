PrintWriter manualReview = getReportWriter("manualReview")

String where = "collection = 'bib' and jsonb_typeof( data#>'{@graph,0,descriptionCreator}' ) = 'array'"

// Human-decided special cases:
String specialCasesString = """
https://libris.kb.se/9tmrfjlm56qfjq1 https://libris.kb.se/library/A
https://libris.kb.se/q82bv9s206154f9 https://libris.kb.se/library/R
https://libris.kb.se/tc565xc50g4hppm https://libris.kb.se/library/Mo
https://libris.kb.se/9tmpphmm2j5fpgf https://libris.kb.se/library/Skov
https://libris.kb.se/zh9jnnw953smwrg https://libris.kb.se/library/Afr
https://libris.kb.se/xg8g44q81ksn91b https://libris.kb.se/library/SEK
https://libris.kb.se/l3wnrs2x2xqrgjh https://libris.kb.se/library/T
https://libris.kb.se/1jb727gc055gj17 https://libris.kb.se/library/T
https://libris.kb.se/6qjng75j3pb1c78 https://libris.kb.se/library/Hig
https://libris.kb.se/3mfpswwf4cw8hrs https://libris.kb.se/library/Hig
https://libris.kb.se/6qjmc7cj4pz535q https://libris.kb.se/library/LnuV
https://libris.kb.se/s9333tp417dmk53 https://libris.kb.se/library/Udig
https://libris.kb.se/m5zd80nz4t5cxm8 https://libris.kb.se/library/Skov
https://libris.kb.se/zg8w95t932wmj1c https://libris.kb.se/library/Og
https://libris.kb.se/l3wwzm6x40pghfb https://libris.kb.se/library/Lbio
https://libris.kb.se/9tmm2ndm5b0cvrz https://libris.kb.se/library/Udig
https://libris.kb.se/fzr53rsr5hgx2v2 https://libris.kb.se/library/Za
https://libris.kb.se/sb454sk438mjz9v https://libris.kb.se/library/Mo
https://libris.kb.se/dxq6d9kq4ts15h4 https://libris.kb.se/library/Skov
https://libris.kb.se/vd66xtp64dbsp0w https://libris.kb.se/library/Mo
https://libris.kb.se/9slkgfcm0tp5fls https://libris.kb.se/library/Lfa
https://libris.kb.se/2ldfnscd0w4zzkw https://libris.kb.se/library/Mo
https://libris.kb.se/q710bmt25t7nfsx https://libris.kb.se/library/Lfa
"""

Map specialCases = [:]
specialCasesString.eachLine { line ->
    String[] parts = line.split(" ")
    if (parts.length == 2) {
        String docUri = parts[0]
        String libUri = parts[1]
        specialCases.put(docUri, libUri)
    }
}

selectBySqlWhere(where) { data ->

    // The overwhelming majority:
    if ( data.graph[0].descriptionCreator.any { it["@id"] == "https://libris.kb.se/library/HdiE" } ) {
        data.graph[0].descriptionCreator = ["@id": "https://libris.kb.se/library/HdiE"]
    } else if ( specialCases.containsKey(data.doc.getURI()) ) {
            data.graph[0].descriptionCreator = ["@id": specialCases.get(data.doc.getURI())]
    } else {
        manualReview.println(data.doc.getURI())
    }

    data.scheduleSave()
}
