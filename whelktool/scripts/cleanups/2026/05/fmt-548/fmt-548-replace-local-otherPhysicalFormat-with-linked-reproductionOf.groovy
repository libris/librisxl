//Identifierar instanser som 
//- pekar ömsesidigt på varandra via (otherPhysicalFormat>describedBy>controlNumber --> sameAs)
//- har marc:displayText physicalversion Digitaliserad version (se specificering ovan)
//

// Set up logging
def report = getReportWriter("INFO.tsv")

// Read IDs
var input = new File(scriptDir, 'mutual_matches_ids.tsv').readLines()

List ids = []
Map idMap = [:]

// Get header indexes
def header = input[0].split("\t").toList()
def physicalIndex = header.indexOf("instance_physical")
def digitalIndex = header.indexOf("instance_digital")

// Read IDs
input.drop(1).each { line ->
    def cols = line.split("\t")

    def physicalIds = cols[physicalIndex]
    def digitalIds = cols[digitalIndex]

    ids << physicalIds

    idMap.get(physicalIds, []) << digitalIds
}

// Fetch the phyiscal instance from Libris
selectByIds(ids) { physicalDoc ->

    Map physicalInstance = physicalDoc.graph[1]
    String physicalId = physicalInstance["@id"]

    String physicalShortId = physicalDoc.doc.shortId
    String physicalControlNumber = physicalDoc.graph[0].controlNumber

    // Get id:s of digital reproductions of this resource from the ID map
        selectByIds(idMap[physicalShortId]) { digitalDoc -> 

            Map digitalInstance = digitalDoc.graph[1]
            String digitalControlNumber = digitalDoc.graph[0].controlNumber
            String digitalId = digitalInstance["@id"]

            if (!digitalInstance.containsKey('otherPhysicalFormat') || !physicalInstance.containsKey('otherPhysicalFormat')) {
                report.println ("$physicalId <-> $digitalId\tSKIPPED\totherPhysicalFormat already removed during previous srcipt run")
                return
            }

            // If reproduction of is a list, leave unchanged and report
            if (digitalInstance.reproductionOf instanceof List) {
                report.println("$digitalId\tSKIPPED\treproductionOf is a list\t$digitalInstance.reproductionOf")
                return
            }

            String reproductionId = digitalInstance.reproductionOf?.get("@id")

            if (reproductionId && reproductionId != physicalId) {
                // Already linked to another physical instance - leave this digital instance unchanged and report
                report.println("$digitalId\tSKIPPED\tHas reproductionOf with other ID\t$digitalInstance.reproductionOf")
                return
            }

            else if (reproductionId == physicalId) {
                // Already linked to expected physical instance - report
                report.println("$digitalId\tINFO\tAlready has reproductionOf with this ID\t$digitalInstance.reproductionOf")
                incrementStats("reproductionOf - already linked", "-")
            }
            
            else {
                // Add "reproductionOf" with a link to the physical instance
                digitalInstance.reproductionOf =  [
                    "@id": physicalId
                ]
                incrementStats("reproductionOf - new link added", "-")
            }

            // Clean up the digital instance
            removeOtherPhysicalFormat(digitalInstance, physicalControlNumber, report)
                
            // Clean up the physical instance
            removeOtherPhysicalFormat(physicalInstance, digitalControlNumber, report)

            digitalDoc.scheduleSave()
            physicalDoc.scheduleSave()
    }
}


void removeOtherPhysicalFormat(Map instance, String pairedControlNumber, report) {
    // Find local entities in otherPhysicalFormat whose controlNumber match the controlNumber of the paired instance
    def matches = instance.otherPhysicalFormat.findAll {
        localEntity -> localEntity.describedBy?.any {
            recordDescribed -> recordDescribed.controlNumber == pairedControlNumber
        }
    }

    // If there is more or less than one match, report this
    if (matches.size() != 1) {
        report.println("${instance["@id"]}\tINFO\tFound ${matches.size()} local entities in otherPhysicalFormat with this controlNumber\t${pairedControlNumber}")
    }

    // Remove the matching local entities
    instance.otherPhysicalFormat.removeAll(matches)

    incrementStats("otherPhysicalFormat - local entity removed", "-")

    // If otherPhysicalFormat is an empty list, remove it
    if (!instance.otherPhysicalFormat) {
        instance.remove("otherPhysicalFormat")

        incrementStats("otherPhysicalFormat - empty property removed", "-")

    }

}