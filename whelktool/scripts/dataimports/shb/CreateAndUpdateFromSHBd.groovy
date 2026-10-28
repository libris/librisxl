import static whelk.util.Jackson.mapper

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
        addObjectsToList(work, 'subject', subjects)
    }

    if (classifications) {
        // SAB headings -- skip and report if heading already in record
        addObjectsToList(work, 'classification', classifications)
        }

    return work
}

/// Main action ///

// Get data folder from system properties
def dataPath = System.properties['dataFolder']
def dataFolder = new File(dataPath)

// Find update file
def updateFile = dataFolder.listFiles()?.find {
    it.isFile() && it.name ==~ /.*_update\.json(l)?/
}

// Find create file
def createFile = dataFolder.listFiles()?.find {
    it.isFile() && it.name ==~ /.*_create\.json(l)?/
}

println "Update file: ${updateFile.absolutePath}"
println "Create file: ${createFile.absolutePath}"

// Set up reporting
def report = getReportWriter('INFO.tsv')

//// Update matched Libris records ////
println 'Getting started updating records'

// Read update file into a map
Map librisToShbMap = [:]

updateFile.eachLine { line ->
    librisToShbMap.putAll(mapper.readValue(line, Map))
}

// Get all the Libris IDs
Set librisIds = librisToShbMap.keySet()

// For each ID, fetch Libris record from Libris
selectByIds(librisIds) { instanceDoc ->
    def (librisRecord, librisInstance) = instanceDoc.graph

    String librisId = instanceDoc.doc.shortId

    def (shbRecord, shbInstance) = librisToShbMap[librisId]['@graph']

    // Add fields and values from SHB to Libris record and work //
    // Skip and report if object already present in Lirbis record //

    // bibliography:SHB 
    Map shbBibliography = [
        '@id': 'https://libris.kb.se/library/SHB'
    ]
    addObjectsToList(librisRecord, 'bibliography', [shbBibliography])

    // sourceConsulted
    Map shbSourceConsulted = shbRecord["sourceConsulted"][0]
    addObjectsToList(librisRecord, 'sourceConsulted', [shbSourceConsulted])

    // Work: classification and subject
    // Get the Libris work
    Map instanceOf = librisInstance.get('instanceOf', {})

    def classifications = shbInstance.get('instanceOf').get('classification',[])
    def subjects = shbInstance.get('instanceOf').get('subject',[])

    // If the work is local
    if ('@id' !in instanceOf) {
        Map work = instanceOf
        librisInstance.instanceOf = updateWork(work, classifications, subjects)
    }

    // If the work is linked
    else {
        println "Fetching linked work..."
        String workId = instanceOf["@id"] 
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
    createList << create(mapper.readValue(line, Map))
}

// For each line
// Save new record to Libris
selectFromIterable(createList) { newRecord ->

    // Save new work
    newRecord.scheduleSave()
}

// The end
