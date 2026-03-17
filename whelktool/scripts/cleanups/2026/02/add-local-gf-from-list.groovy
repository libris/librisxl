var filePath = System.properties['filepath']

var input = new File(filePath).readLines()

List ids = []
Map idToLabelMap = [:]

// get header indexes
def header = input[0].split(",").toList()
def iIndex = header.indexOf("i")
def labelIndex = header.indexOf("gfBroaderLabel")
def broaderIndex = header.indexOf("gfBroader")

input.drop(1).each { line ->
    def cols = line.split(",")

    def iValue = cols[iIndex]
    def broaderLabel = cols[labelIndex]
    def broaderUri = cols[broaderIndex]

    ids << iValue
    idToLabelMap[iValue] = [uri: broaderUri, label: broaderLabel]
}

selectByIds(ids) { instanceDoc ->
    def instance = instanceDoc.graph[1]
    def instanceId = instance["@id"]

    Map complementaryGf = [
            "@type": "GenreForm",
            "inCollection": [
                    ["@id": "https://id.kb.se/term/div/complement"]
            ],
            "prefLabel": idToLabelMap[instanceId]["label"]
    ]

    println complementaryGf
    if ("@id" !in instance.instanceOf) {
        Map work = instance["instanceOf"]
        incrementStats('type', work["@type"])

        work.get('category') << complementaryGf

        instanceDoc.scheduleSave()

    }
    else {
        String workId = instance["instanceOf"]["@id"]

        selectByIds([workId]) { workDoc ->
            Map work = workDoc.graph[1]
            incrementStats('type', work["@type"])

            work.get('category') << complementaryGf

            workDoc.scheduleSave()


        }
    }
}

