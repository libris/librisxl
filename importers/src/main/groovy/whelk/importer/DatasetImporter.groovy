package whelk.importer

import jline.internal.Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.IdGenerator
import whelk.Whelk
import whelk.exception.CancelUpdateException

class DatasetImporter {
    static final ObjectMapper mapper = new ObjectMapper()

    static void importDataset(Whelk whelk, String filePath, String dataset, String marcCollection) {

        if ( ! ["auth", "bib", "hold", "definitions"].any{ it.equals(marcCollection) } )
            throw new RuntimeException("Unknown marc collection.")

        if (Runtime.getRuntime().maxMemory() < 2l * 1024l * 1024l * 1024l) {
            Log.warn("This application may require substantial amounts of memory, " +
                    "if the dataset in question is large. Please start with -Xmx3G (at least).")
            return
        }

        Set<String> idsInInput = []
        long updatedCount = 0
        long createdCount = 0
        long deletedCount = 0

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
                        doc.data = incomingDoc.data
                    }
                    else {
                        throw new CancelUpdateException() // Not an error, merely cancels the update
                    }
                    ++updatedCount
                })
            } else {

                // New document
                whelk.createDocument(incomingDoc, "xl", null, marcCollection, false)
                ++createdCount
            }
        }

        // Clear out anything that was previously stored in this dataset, but was not in the in-data now.
        // If faced with "can't delete depended on stuff", retry again later, after more other deletes have
        // succeeded (there may be intra-set dependencies).
        List<String> needsRetry = []
        whelk.storage.doForIdInDataset(dataset, { String storedIdInDataset ->
            if (!idsInInput.contains(storedIdInDataset)) {
                if (!remove(whelk, storedIdInDataset)) {
                    needsRetry.add(storedIdInDataset)
                } else {
                    deletedCount++
                }
            }
        })

        boolean anythingRemovedLastPass = true
        while (anythingRemovedLastPass) {
            anythingRemovedLastPass = false
            needsRetry.removeAll { String storedIdInDataset ->
                if (remove(whelk, storedIdInDataset)) {
                    anythingRemovedLastPass = true
                    deletedCount++
                    return true
                }
                return false
            }
        }

        if (!needsRetry.isEmpty()) {
            Log.warn("The following IDs SHOULD have been deleted, but doing so was not " +
                    "possible, so they were skipped (most likely they are still depended upon):\n" + needsRetry)
        }

        System.err.println("Created: " + createdCount +" new,\n" +
                "updated: " + updatedCount + " existing and\n" +
                "deleted: " + deletedCount + " old records (should have been: " + (deletedCount + needsRetry.size()) + "),\n" +
                "out of the: " + idsInInput.size() + " records in dataset: \"" + dataset + "\".\n" +
                "Dataset now in sync.")
    }

    private static boolean remove(Whelk whelk, String id) {
        try {
            whelk.remove(id, "xl", null)
            return true
        } catch ( RuntimeException re ) {
            // The expected exception here is: java.lang.RuntimeException: Deleting depended upon records is not allowed.
            return false
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