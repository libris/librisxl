package whelk.importer

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.Whelk
import whelk.exception.CancelUpdateException

import static whelk.util.Jackson.mapper

@Log
class DatasetImporter {
    static void importDataset(Whelk whelk, String filePath, String dataset) {

        if (Runtime.getRuntime().maxMemory() < 2l * 1024l * 1024l * 1024l) {
            log.warn("This application may require substantial amounts of memory, " +
                    "if the dataset in question is large. Please start with -Xmx3G (at least).")
            return
        }

        Set<String> idsInInput = []

        long updatedCount = 0
        long createdCount = 0
        long lineCount = 0

        File inDataFile = new File(filePath)
        inDataFile.eachLine { line ->
            Map data = mapper.readValue(line.getBytes("UTF-8"), Map)
            Document incomingDoc = new Document(data)
            ensureAbsoluteSystemId(incomingDoc)
            idsInInput.add(incomingDoc.getShortId())
            incomingDoc.addInDataset(dataset)

            // This race condition should be benign. If there is a document with
            // the same ID created in between the check and the creation, we'll
            // get an exception and fail early (unfortunate but acceptable).
            Document storedDoc = whelk.getDocument(incomingDoc.getShortId())
            if (storedDoc != null) {

                // Update (potentially) of existing document
                whelk.storeAtomicUpdate(incomingDoc.getShortId(), true, "xl", null, { doc ->
                    if (doc.getChecksum(whelk.jsonld) != incomingDoc.getChecksum(whelk.jsonld)) {
                        doc.data = incomingDoc.data
                    }
                    else {
                        throw new CancelUpdateException() // Not an error, merely cancels the update
                    }
                    ++updatedCount
                })
            } else {

                // New document
                whelk.createDocument(incomingDoc, "xl", null, "definitions", false)
                ++createdCount
            }

            if ( lineCount % 100 == 0 ) {
                System.err.println("Processed " + lineCount + " input records. " + createdCount + " created, " +
                        updatedCount + " updated, " + (lineCount-createdCount-updatedCount) + " already up to date.")
            }
            ++lineCount
        }

        List<String> needsRetry = []
        long deletedCount = removeDeleted(whelk, dataset, idsInInput, needsRetry)

        System.err.println("Created: " + createdCount +" new,\n" +
                "updated: " + updatedCount + " existing and\n" +
                "deleted: " + deletedCount + " old records (should have been: " + (deletedCount + needsRetry.size()) + "),\n" +
                "out of the: " + idsInInput.size() + " records in dataset: \"" + dataset + "\".\n" +
                "Dataset now in sync.")
    }

    protected static long removeDeleted(Whelk whelk, String dataset, Set<String> idsInInput, List<String> needsRetry) {
        // Clear out anything that was previously stored in this dataset, but was not in the in-data now.
        // If faced with "can't delete depended on stuff", retry again later, after more other deletes have
        // succeeded (there may be intra-set dependencies). If the dataset contains circular dependencies,
        // deletions will never be possible until the circle is unlinked/broken somewhere along the chain.
        long deletedCount = 0
        whelk.storage.doForIdInDataset(dataset, { String storedIdInDataset ->
            if (!idsInInput.contains(storedIdInDataset)) {
                if (!remove(whelk, storedIdInDataset)) {
                    needsRetry.add(storedIdInDataset)
                } else {
                    deletedCount++
                    if (deletedCount % 50 == 0) {
                        System.err.println("Cleaning up: " + deletedCount + " records deleted (they are no longer in the dataset).")
                    }
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
            log.warn("The following IDs SHOULD have been deleted, but doing so was not " +
                    "possible, so they were skipped (most likely they are still depended upon):\n" + needsRetry)
        }

        return deletedCount
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
        // IDs need to be both reproducible and absolute!
        throw new RuntimeException("Could not obtain a proper record ID for: " + doc.getURI().toString())
    }
}
