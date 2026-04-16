var input = new File(scriptDir, 'X_IDs_with_narrower_broader_SAOGF.csv').readLines()

List ids = []
Map idToLabelMap = [:]

// get header indexes
def header = input[0].split("\\|").toList()
def iIndex = header.indexOf("id")
def labelIndex = header.indexOf("gfBroaderLabel")
def broaderIndex = header.indexOf("gfBroader")

input.drop(1).each { line ->
    def cols = line.split("\\|")

    def iValue = cols[iIndex]
    def broaderLabel = cols[labelIndex]

    ids << iValue
    idToLabelMap.get(iValue, []) << broaderLabel
}

selectByIds(ids) { instanceDoc ->
    def instance = instanceDoc.graph[1]
    def instanceRecordShortId = instanceDoc.doc.shortId

    for (broader in idToLabelMap[instanceRecordShortId]) {

        Map complementaryGf = [
                "@type": "GenreForm",
                "inCollection": [
                        ["@id": "https://id.kb.se/term/div/complement"]
                ],
                "inScheme": [
                        "@id": "https://id.kb.se/term/saogf"
                ],
                "prefLabel": broader
        ]

        if ("@id" !in instance.instanceOf) {
            Map work = instance["instanceOf"]
            incrementStats('type', work["@type"])

            if (!(complementaryGf in work.category)) {
                work.category << complementaryGf
                incrementStats('complement', broader)
            }

            instanceDoc.scheduleSave()

        }
        else {
            String workId = instance["instanceOf"]["@id"]
            selectByIds([workId]) { workDoc ->
                Map work = workDoc.graph[1]
                incrementStats('type', work["@type"])

                if (!(complementaryGf in work.category)) {
                    work.category << complementaryGf
                }

                workDoc.scheduleSave()


            }
        }
    }
}

