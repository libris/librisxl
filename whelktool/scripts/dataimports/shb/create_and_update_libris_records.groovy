import groovy.json.JsonSlurper

/// Functions ///

void addObjectsToList(Map librisRecord, String property, List objects) {
    
    def thisProperty = librisRecord.get(property, [])

    for (object in objects) {
        if (!thisProperty.contains(object)) {
        thisProperty << object
        }

    librisRecord[property] = thisProperty
    }

}

Map updateWork(Map work, List classifications, List subjects) {

    if (subjects) {
        // SAO headings -- skip and report if heading already in record
        addObjectsToList(work, 'subjects', subjects)
    }

    if (classifications) {
        // SAB headings -- skip and report if heading already in record
        addObjectsToList(work, 'classification', classifications)
        }

    return work
}

/// Main action ///

// Get data folder from system properties
def dataFolder = System.properties['dataFolder']

// Open update file
def updateFile = new File(dataFolder, 'update.jsonl')
// Open create file
def createFile = new File(dataFolder, 'create.jsonl')

println "Update file: ${updateFile.absolutePath}"
println "Create file: ${createFile.absolutePath}"

// Set up reporting
def report = getReportWriter('INFO.tsv')

// Get ready to read some jsonl
def slurper = new JsonSlurper()

//// Update matched Libris records ////
println 'Getting started updating records'

// Read update file into a map
Map librisToShbMap = [:]

updateFile.eachLine { line ->
    librisToShbMap.putAll(slurper.parseText(line))
}

// Get all the Libris IDs
Set librisIds = librisToShbMap.keySet()

println librisIds

// Fetch Libris record from Libris
selectByIds(librisIds) { instanceDoc ->
    def (librisRecord, librisInstance) = instanceDoc.graph

    String librisId = instanceDoc.doc.shortId

    def (shbRecord, shbInstance) = librisToShbMap[librisId]['@graph']

    println "Libris ID ${librisId} -> ${shbRecord['@id']}"

    // Add fields and values from SHB to Libris record
    println "Updating the admin record..."

    // bibliography:SHB -- skip and report if bibliography:SHB already in record
    Map shbBibliography = [
        '@id': 'https://libris.kb.se/library/SHB'
    ]
    addObjectsToList(librisRecord, 'bibliography', [shbBibliography])


    // Anything else? Metadata source?

    // Add fields and values from SHB to Libris work
    println "Updating the work..."

    def classifications = shbInstance.get('instanceOf').get('classification',[])
    def subjects = shbInstance.get('instanceOf').get('subjects',[])

    // Find the Libris work
    Map instanceOf = librisInstance.get('instanceOf', {})

    // Local
    if ('@id' !in instanceOf) {
        println "Using local work..."
        Map work = instanceOf
        librisInstance.instanceOf = updateWork(work, classifications, subjects)
    }

    // Linked
    else {
        println "Fetching linked work..."
        workId = instanceOf["@id"] 
        selectByIds([workId]) { workDoc ->
        Map work = workDoc.graph[1]

        workDoc.graph[1] = updateWork(work, classifications, subjects)

        // Save updated work
        workDoc.scheduleSave()
        }
    }

    // Save updated instance
    instanceDoc.scheduleSave()
}

//// Now let's create new Libris records ////
println 'Getting started creating records'
// Read create file into a map of create-objects
List<Map> createList = []

createFile.eachLine { line ->
    createList << create(slurper.parseText(line) as Map)
}

// For each line
// Save new record to Libris
selectFromIterable(createList) { newRecord ->

    // Save new work
    newRecord.scheduleSave()
}

// The end
