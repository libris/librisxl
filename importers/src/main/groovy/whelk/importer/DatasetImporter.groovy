package whelk.importer

import groovy.util.logging.Log4j2 as Log
import groovy.transform.CompileStatic
import static groovy.transform.TypeCheckingMode.SKIP

import whelk.Document
import whelk.JsonLd
import whelk.TargetVocabMapper
import whelk.Whelk
import whelk.converter.TrigToJsonLdParser
import whelk.exception.CancelUpdateException
import whelk.util.DocumentUtil

import static whelk.util.Jackson.mapper

@Log
@CompileStatic
class DatasetImporter {

    static String GRAPH = JsonLd.GRAPH_KEY
    static String ID = JsonLd.ID_KEY
    static String TYPE = JsonLd.TYPE_KEY
    static String REVERSE = JsonLd.REVERSE_KEY

    static String HASH_IT = '#it'

    // Flags:
    static SELF_DESCRIBED = 'self-described'
    // verify that id:s are served by the system; if not, unless these are cache-records; use:
    static INTERNAL_MAIN_IDS = 'internal-main-ids' // replace id with XL-id (move current to sameAs)
    static CACHE_RECORDS = 'cache-records' // with preserved id:s (only part of XL as an implementation detail!)

    Whelk whelk
    DatasetInfo dsInfo
    String recordType = 'Record'
    boolean selfDescribed = false
    boolean makeMainIds = false

    TargetVocabMapper tvm = null
    Map contextDocData = null

    Map<String, String> aliasMap = [:]

    DatasetImporter(Whelk whelk, String datasetUri, Map flags=[:]) {
        this.whelk = whelk
        selfDescribed = SELF_DESCRIBED in flags
        makeMainIds = INTERNAL_MAIN_IDS in flags
        if (CACHE_RECORDS in flags) {
            recordType = 'CacheRecord'
        }
        dsInfo = getDatasetInfo(datasetUri)

        if (Runtime.getRuntime().maxMemory() < 2l * 1024l * 1024l * 1024l) {
            log.warn("This application may require substantial amounts of memory, " +
                    "if the dataset in question is large. Please start with -Xmx3G (at least).")
            return
        }
    }

    void importDataset(String sourceUrl) {
        Set<String> idsInInput = []

        long updatedCount = 0
        long createdCount = 0
        long lineCount = 0

        if (tvm == null) {
            contextDocData = getDocByMainEntityId(whelk.kbvContextUri).data
            tvm = new TargetVocabMapper(whelk.jsonld, contextDocData)
        }

        eachItem(sourceUrl) { Map data ->
            Document incomingDoc = completeRecord(data)
            idsInInput.add(incomingDoc.getShortId())

            // This race condition should be benign. If there is a document with
            // the same ID created in between the check and the creation, we'll
            // get an exception and fail early (unfortunate but acceptable).
            Document storedDoc = whelk.getDocument(incomingDoc.getShortId())
            if (storedDoc != null) {
                // Update (potentially) of existing document
                update(incomingDoc, {
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
        long deletedCount = removeDeleted(idsInInput, needsRetry)

        System.err.println("Created: " + createdCount +" new,\n" +
                "updated: " + updatedCount + " existing and\n" +
                "deleted: " + deletedCount + " old records (should have been: " + (deletedCount + needsRetry.size()) + "),\n" +
                "out of the: " + idsInInput.size() + " records in dataset: \"" + dsInfo.uri + "\".\n" +
                "Dataset now in sync.")
    }

    void dropDataset() {
        long deletedCount = removeDeleted([] as Set, [])
        System.err.println("Deleted dataset ${dsInfo.uri} with ${deletedCount} existing records")
    }

    private void eachItem(String sourceUrl, Closure processItem) {
        if (sourceUrl ==~ /.+\.(jsonl|json(ld)?\.lines)$/) {
            File inDataFile = new File(sourceUrl)
            inDataFile.eachLine { line ->
                Map data = mapper.readValue(line.getBytes("UTF-8"), Map)
                processItem(data)
            }
        } else {
            Map data = new URL(sourceUrl).withInputStream { TrigToJsonLdParser.parse(it) }
            data = (Map) tvm.applyTargetVocabularyMap(whelk.defaultTvmProfile, contextDocData, data)
            List<Map> graph = (List<Map>) data[GRAPH]
            for (Map item : graph) {
                processItem(item)
            }
        }
    }

    protected Document update(Document incomingDoc, Closure callback=null) {
        whelk.storeAtomicUpdate(incomingDoc.getShortId(), true, "xl", null, { doc ->
            if (doc.getChecksum(whelk.jsonld) == incomingDoc.getChecksum(whelk.jsonld)) {
                throw new CancelUpdateException() // Not an error, merely cancels the update
            }
            doc.data = incomingDoc.data
            if (callback) {
                callback()
            }
        })
    }

    protected long removeDeleted(Set<String> idsInInput, List<String> needsRetry) {
        // Clear out anything that was previously stored in this dataset, but was not in the in-data now.
        // If faced with "can't delete depended on stuff", retry again later, after more other deletes have
        // succeeded (there may be intra-set dependencies). If the dataset contains circular dependencies,
        // deletions will never be possible until the circle is unlinked/broken somewhere along the chain.
        long deletedCount = 0
        whelk.storage.doForIdInDataset(dsInfo.uri, { String storedIdInDataset ->
            if (!idsInInput.contains(storedIdInDataset)) {
                if (!remove(storedIdInDataset)) {
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
                if (remove(storedIdInDataset)) {
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

    private boolean remove(String id) {
        try {
            whelk.remove(id, "xl", null)
            return true
        } catch ( RuntimeException re ) {
            // The expected exception here is: java.lang.RuntimeException: Deleting depended upon records is not allowed.
            return false
        }
    }

    DatasetInfo getDatasetInfo(String datasetUri) {
        if (selfDescribed) {
            return new DatasetInfo([(ID): datasetUri])
        }
        Document datasetRecord = whelk.storage.getDocumentByIri(datasetUri)
        if (datasetRecord == null) {
            throw new RuntimeException("Could not get dataset data for: $datasetUri")
        }
        Map datasetData = ((List) datasetRecord.data[GRAPH])[1]
        assert datasetData[ID] == datasetUri
        return new DatasetInfo(datasetData)
    }

    @CompileStatic(SKIP)
    Document completeRecord(Map data) {
        if (GRAPH !in data) {
            def givenId = data[ID]
            def slug = dsInfo.mintPredictableRecordSlug(givenId)
            assert slug
            def newRecordId = Document.BASE_URI.resolve(slug).toString()

            if (makeMainIds) {
                def newId = newRecordId + HASH_IT
                addToSameAs(data, givenId)
                data[ID] = newId
            }

            if (REVERSE in data) {
                Map revs = data.remove(REVERSE)
            }

            Map record = data.remove('meta') ?: [:]
            record[ID] = newRecordId
            record[TYPE] = recordType

            record['mainEntity'] = [(ID): data[ID]]
            data = [(GRAPH): [record, data]]
        }

        DocumentUtil.findKey(data[GRAPH][1], ID) { id, path ->
            if ('sameAs' in path) {
                return
            }
            def canonical = aliasMap.get(id) ?: whelk.storage.getThingId(id)
            if (canonical && id != canonical) {
                aliasMap[id] = canonical
                return new DocumentUtil.Replace(canonical)
            }
        }

        Document doc = new Document(data)
        ensureAbsoluteSystemId(doc)
        doc.addInDataset(dsInfo.uri)

        if (dsInfo.uriRegexPattern && !dsInfo.uriRegexPattern.matcher(incomingDoc.getMainId()).matches()) {
            throw new RuntimeException("${incomingDoc.getMainId()} does not match ${dsInfo.uriRegexPattern}")
        }

        return doc
    }

    private void ensureAbsoluteSystemId(Document doc) {
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

    private void addToSameAs(Map entity, String id) {
        List<Map<String, String>> same = entity.get('sameAs', [])
        if (!same.find { it[ID] == id }) {
            same << [(ID): id]
        }
    }

    private Document getDocByMainEntityId(String id) {
        return whelk.storage.loadDocumentByMainId(id, null)
    }

}
