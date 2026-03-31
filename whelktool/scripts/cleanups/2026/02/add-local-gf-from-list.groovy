var filePath = System.properties['filepath']

var input = new File(filePath).readLines()

List ids = []
Map idToLabelMap = [:]

// get header indexes
def header = input[0].split("\\|").toList()
def iIndex = header.indexOf("i")
def labelIndex = header.indexOf("gfBroaderLabel")
def broaderIndex = header.indexOf("gfBroader")

input.drop(1).each { line ->
    def cols = line.split("\\|")

    def iValue = cols[iIndex]
    def broaderLabel = cols[labelIndex]

    ids << iValue
    idToLabelMap.get(iValue, []) << [broaderLabel]
}

selectByIds(ids) { instanceDoc ->
    def instance = instanceDoc.graph[1]
    def instanceId = instance["@id"]

    for (broader in idToLabelMap[instanceId]) {

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
        }
        else {
            String workId = instance["instanceOf"]["@id"]
            println workId
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

    instanceDoc.scheduleSave()

}

