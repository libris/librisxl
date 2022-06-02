package whelk.importer

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import groovy.transform.Immutable
import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.IdGenerator
import whelk.util.DocumentUtil
import whelk.exception.CancelUpdateException

import static whelk.util.Jackson.mapper

@Log
class DatasetImporter {

    static GRAPH = JsonLd.GRAPH_KEY
    static ID = JsonLd.ID_KEY
    static TYPE = JsonLd.TYPE_KEY
    static REVERSE = JsonLd.REVERSE_KEY

    static String HASH_IT = '#it'

    @Immutable
    static class DatasetInfo {
        String uri
        int createdMs
        String uriSpace
    }

    static void importDataset(Whelk whelk, String filePath, String datasetUri) {

        if (Runtime.getRuntime().maxMemory() < 2l * 1024l * 1024l * 1024l) {
            log.warn("This application may require substantial amounts of memory, " +
                    "if the dataset in question is large. Please start with -Xmx3G (at least).")
            return
        }

        Set<String> idsInInput = []

        long updatedCount = 0
        long createdCount = 0
        long lineCount = 0

        DatasetInfo dsInfo = getDatasetInfo(whelk, datasetUri)

        File inDataFile = new File(filePath)
        inDataFile.eachLine { line ->
            Map data = mapper.readValue(line.getBytes("UTF-8"), Map)
            Document incomingDoc = completeRecord(whelk, data, dsInfo)
            idsInInput.add(incomingDoc.getShortId())

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
        long deletedCount = removeDeleted(whelk, datasetUri, idsInInput, needsRetry)

        System.err.println("Created: " + createdCount +" new,\n" +
                "updated: " + updatedCount + " existing and\n" +
                "deleted: " + deletedCount + " old records (should have been: " + (deletedCount + needsRetry.size()) + "),\n" +
                "out of the: " + idsInInput.size() + " records in dataset: \"" + datasetUri + "\".\n" +
                "Dataset now in sync.")
    }

    protected static long removeDeleted(Whelk whelk, String datasetUri, Set<String> idsInInput, List<String> needsRetry) {
        // Clear out anything that was previously stored in this dataset, but was not in the in-data now.
        // If faced with "can't delete depended on stuff", retry again later, after more other deletes have
        // succeeded (there may be intra-set dependencies). If the dataset contains circular dependencies,
        // deletions will never be possible until the circle is unlinked/broken somewhere along the chain.
        long deletedCount = 0
        whelk.storage.doForIdInDataset(datasetUri, { String storedIdInDataset ->
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

    static DatasetInfo getDatasetInfo(Whelk whelk, String datasetUri) {
        Map owningDataset = whelk.storage.getDocumentByIri(datasetUri)?.data[GRAPH][1]
        assert owningDataset
        def uriSpace = owningDataset['uriSpace']
        if (uriSpace == null) {
            uriSpace = owningDataset[ID]
            if (!uriSpace.endsWith('/')) {
                uriSpace += '/'
            }
        }
        int createdMs = parseW3CDateTime(owningDataset['created'])

        return new DatasetInfo(datasetUri, createdMs, uriSpace)
    }

    static Document completeRecord(Whelk whelk, Map data, DatasetInfo dsInfo) {
        if (GRAPH !in data) {
            def givenId = data[ID]
            def newId = mintNewId(dsInfo, givenId)
            if (newId != null) {
                addToSameAs(data, givenId)
                data[ID] = newId
            }

            if (REVERSE in data) {
                Map revs = data.remove(REVERSE)
            }

            Map record = data.remove('meta') ?: [:]
            record[ID] = data[ID].replace(HASH_IT, '')
            record[TYPE] = 'Record'

            record['mainEntity'] = [(ID): data[ID]]
            data = [(GRAPH): [record, data]]
        }

        DocumentUtil.findKey(data[GRAPH][1], ID) { id, path ->
            if ('sameAs' in path) {
                return
            }
            def canonical = whelk.storage.getThingId(id)
            if (canonical && id != canonical) {
                return new DocumentUtil.Replace(canonical)
            }
        }

        Document doc = new Document(data)
        ensureAbsoluteSystemId(doc)
        doc.addInDataset(dsInfo.uri)

        return doc
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
        throw new RuntimeException("Could not obtain a proper record ID for: " + doc.recordIdentifiers.toString())
    }

    private static long parseW3CDateTime(String dt) {
        return ZonedDateTime.parse(dt, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli()
    }

    private static String mintNewId(DatasetInfo dsInfo, String givenId) {
        if (!givenId.startsWith(dsInfo.uriSpace)) {
            return null
        }
        String slug = givenId.substring(dsInfo.uriSpace.length())
        int timestamp = dsInfo.createdMs + fauxOffset(slug)
        String xlId = IdGenerator.generate(timestamp, slug)
        return Document.BASE_URI.resolve(xlId).toString() + HASH_IT
    }

    private static int fauxOffset(String s) {
        int n = 0
        for (int i = 0 ; i < s.size(); i++) {
            n += s[i].codePointAt(0) * ((i+1) ** 2)
        }
        return n
    }

    private static void addToSameAs(Map entity, String id) {
        List<Map<String, String>> same = entity.get('sameAs', [])
        if (!same.find { it[ID] == id }) {
            same << [(ID): id]
        }
    }

}
