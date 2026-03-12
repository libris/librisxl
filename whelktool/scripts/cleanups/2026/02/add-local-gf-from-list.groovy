import static whelk.JsonLd.ID_KEY

incrementStats('category', 'name', 'example')

var filePath = System.properties['nowhelk.filepath']

var input = new File(filePath).readLines()

List ids = []
Map idToLabelMap = [:]

// get header indexes
def header = input[0].split(",").toList()
def iIndex = header.indexOf("i")
def labelIndex = header.indexOf("gfBroaderLabel")

input.drop(1).each { line ->
    def cols = line.split(",")

    def iValue = cols[iIndex]
    def label = cols[labelIndex]

    ids << iValue
    idToLabelMap[iValue] = label
}

selectByIds(ids) { i ->
    def instance = i.graph[1]
    def instanceId = instance["@id"]

    if ("@id" !in instance.instanceOf) {
        //println "Embedded work"
        incrementStats('type', instance["instanceOf"]["@type"])
        //println idToLabelMap[instanceId]
    }
    else {
        println "Linked work"
        println instance["instanceOf"]["@id"]
        def workId = instance["instanceOf"]["@id"]
        work = selectById(workId).graph[1]
        incrementStats('type', work["@type"])
        println idToLabelMap[instanceId]
    }

    //process
}
