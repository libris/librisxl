package whelk.importer

import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.IdGenerator
import whelk.Whelk
import whelk.exception.CancelUpdateException

class DatasetImporter {
    static final ObjectMapper mapper = new ObjectMapper()

    static importDataset(Whelk whelk, String filePath, String dataset, String marcCollection) {

        if ( ! ["auth", "bib", "hold", "definitions"].any{ it.equals(marcCollection) } )
            throw new RuntimeException("Unknown marc collection.")

        Set<String> idsInInput = []

        File inDataFile = new File(filePath)
        inDataFile.eachLine { line ->
            Map data = mapper.readValue(line.getBytes("UTF-8"), Map)
            Document incomingDoc = new Document(data)
            ensureAbsoluteSystemId(incomingDoc)
            idsInInput.add(incomingDoc.getShortId())
            incomingDoc.setInDataSet(dataset)

            // This race condition should be benign. If there is a document with
            // the same ID created in between the check and the creation, we'll
            // get an exception and fail early (unfortunate but acceptable).
            Document storedDoc = whelk.getDocument(incomingDoc.getShortId())
            if (storedDoc != null) {

                // Update (potentially) of existing document
                whelk.storeAtomicUpdate(incomingDoc.getShortId(), true, "xl", null, { doc ->
                    if (doc.getChecksum() != incomingDoc.getChecksum()) {
                        //System.err.println("Did not match checksums:\n\n" + doc.getDataAsString() + "\n\n" + incomingDoc.getDataAsString())
                        doc.data = incomingDoc.data
                    }
                    else {
                        throw new CancelUpdateException() // Not an error, merely cancels the update
                    }
                })
            } else {

                // New document
                whelk.createDocument(incomingDoc, "xl", null, marcCollection, false)
            }

            // Clear out anything that was previously stored in this dataset, but was not in the in-data now.
            whelk.storage.doForIdInDataset(dataset, { String storedIdInDataset ->
                System.err.println(storedIdInDataset)
            })
        }
    }

    private static void ensureAbsoluteSystemId(Document doc) {
        def sysBaseIri = Document.BASE_URI
        // A system id is in place; do nothing and return.
        if (doc.recordIdentifiers.any { it.startsWith(sysBaseIri.toString()) }) {
            return
        }
        // A relative system id (slug) exists; make it absolute and return.
        def slug = doc.recordIdentifiers.find { it =~ /^\w+$/ }
        if (slug) {
            doc.setId(slug)
            return
        }
        // Mint a new system Id and set it.
        def newId = sysBaseIri.resolve(IdGenerator.generate()).toString()
        doc.addRecordIdentifier(newId)
    }
}