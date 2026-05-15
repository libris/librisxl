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

    // Get id:s of digital reproductions of this resource from the ID map
        selectByIds(idMap[physicalShortId]) { digitalDoc -> 

            def digitalInstance = digitalDoc.graph[1]
            def digitalId = digitalInstance["@id"]
            def digitalSameAs = digitalInstance.sameAs[0]['@id'].tokenize('/').last()

            println "\nLinking ${digitalId} to $physicalId"
            // Double check that local/old IDs match? maybe not necessary.

            if (digitalInstance.reproductionOf) {
                report.println("$digitalId\tSKIPPED\talready has reproductionOf")
            }

            else {

                // Add "reproductionOf" with a link to the physical instance
                digitalInstance.reproductionOf =  [
                    "@id": physicalId
                ]

                incrementStats("Links added to reproductionOf", "-")

                // Clean up the digital instance
                removeOtherPhysicalFormat(digitalInstance, physicalSameAs)
                
                // Clean up the physical instance
                removeOtherPhysicalFormat(physicalInstance, digitalSameAs)
            }
    }

}

/**
 * Removes local entities from otherPhysicalFormat whose describedBy.controlNumber
 * match the supplied pairedSameAs value.
 * Removes otherPhysicalFormat if it is an empty list.
 */
void removeOtherPhysicalFormat(Map instance, String pareidSameAsId) {
    def matches = instance.otherPhysicalFormat.findAll {
        localEntity -> localEntity.describedBy?.any {
            recordDescribed -> recordDescribed.controlNumber == pareidSameAsId
        }
    }

    if (matches.size() > 1) {
        report.println("${instance["@id"]}\tINFO\nFound ${matches.size()} matches for controlNumber=${pareidSameAsId}")
    }

    instance.otherPhysicalFormat.removeAll(matches)

    incrementStats("Local entities removed", "-")

    if (!instance.otherPhysicalFormat) {
        instance.remove("otherPhysicalFormat")

        incrementStats("Empty otherPhysicalFormat removed", "-")

    }

}