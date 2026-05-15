//Identifierar instanser som 
//- pekar ömsesidigt på varandra via (otherPhysicalFormat>describedBy>controlNumber --> sameAs)
//- har marc:displayText physicalversion Digitaliserad version (se specificering ovan)
//

// Set up logging
def report = getReportWriter("report.tsv")

// Read IDs
var input = new File(scriptDir, 'ids.tsv').readLines()

List ids = []
Map idMap = [:]

// get header indexes
def header = input[0].split("\t").toList()
def physicalIndex = header.indexOf("instance_physical")
def digitalIndex = header.indexOf("instance_digital")

input.drop(1).each { line ->
    def cols = line.split("\t")

    def physicalIds = cols[physicalIndex]
    def digitalIds = cols[digitalIndex]

    ids << physicalIds

    idMap.get(physicalIds, []) << digitalIds
}

// Fetch the phyiscal instance from Libris
selectByIds(ids) { physicalDoc ->

    def physicalInstance = physicalDoc.graph[1]
    def physicalShortId = physicalDoc.doc.shortId
    def physicalId = physicalInstance["@id"]
    def physicalSameAs = physicalInstance.sameAs[0]['@id'].tokenize('/').last()

    println "\nPhysical before"
    println physicalInstance

    // Get id:s of digital reproductions of this resource from the ID map
        selectByIds(idMap[physicalShortId]) { digitalDoc -> 

            // Double check that local/old IDs match? maybe not necessary.

            def digitalInstance = digitalDoc.graph[1]
            def digitalSameAs = digitalInstance.sameAs[0]['@id'].tokenize('/').last()

                println "\nDigital before"
                println digitalInstance

            if (digitalInstance.reproductionOf) {
                def digitalId = digitalInstance["@id"]
                report.println("$digitalId\talready has reproductionOf. Skipping.")
            }

            else {

                // Add "reproductionOf" with a link to the physical instance
                digitalInstance.reproductionOf =  [
                    "@id": physicalId
                ]

                // In the digital instance
                // TODO Remove the property otherPhysicalFormat or a certain blank node?
                digitalInstance.otherPhysicalFormat.removeAll { localEntity -> 
                    localEntity.describedBy?.any { recordDescribed ->
                    recordDescribed.controlNumber == physicalSameAs
                    }
                } 
                
                // In the physical instance
                // TODO Remove the property otherPhysicalFormat or a certain blank node?
                physicalInstance.otherPhysicalFormat.removeAll { localEntity -> 
                    localEntity.describedBy?.any { recordDescribed ->
                    recordDescribed.controlNumber == digitalSameAs
                    }
                }
                println "\nDigital after"
                println digitalInstance
            }

            println "\nPhysical after"
            println physicalInstance 

    }

}
