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

    static String HASH_IT = '#it'

    // Flags:
    // verify that id:s are served by the system; else use:
    static REPLACE_MAIN_IDS = 'replace-main-ids' // replace id with XL-id (move current to sameAs)

    Whelk whelk
    String datasetUri
    DatasetInfo dsInfo
    private Document dsRecord

    boolean replaceMainIds = false
    String collection = null

    TargetVocabMapper tvm = null
    Map contextDocData = null

    Map<String, String> aliasMap = [:]

    DatasetImporter(Whelk whelk, String datasetUri, Map flags=[:], String datasetDescPath=null) {
        this.whelk = whelk
        this.datasetUri = datasetUri
        if (datasetDescPath != null) {
            Map givenDsData = (Map) loadData(datasetDescPath)[GRAPH].find { it[ID] == datasetUri  }
            dsInfo = getDatasetInfo(datasetUri, givenDsData)
            dsRecord = completeRecord(givenDsData, 'Record')
            createOrUpdateDocument(dsRecord)
        }

        replaceMainIds = flags.get(REPLACE_MAIN_IDS) == true
        collection = "definitions" // TODO: legacy construct; prefer to keep this as null?

        if (Runtime.getRuntime().maxMemory() < 2l * 1024l * 1024l * 1024l) {
            log.warn("This application may require substantial amounts of memory, " +
                    "if the dataset in question is large. Please start with -Xmx3G (at least).")
            return
        }
    }

    TargetVocabMapper getTvm() {
        if (tvm == null) {
            contextDocData = getDocByMainEntityId(whelk.kbvContextUri).data
            tvm = new TargetVocabMapper(whelk.jsonld, contextDocData)
        }
        return tvm
    }

    void importDataset(String sourceUrl) {
        Set<String> idsInInput = []

        if (dsRecord != null) {
            idsInInput.add(dsRecord.getShortId())
        }

        String recordType = sourceUrl ==~ /^(https?):.+/ ? 'CacheRecord' : 'Record'

        long updatedCount = 0
        long createdCount = 0
        long lineCount = 0

        boolean first = true

        processDataset(sourceUrl) { Map data ->
            if (dsInfo == null) {
                if (!first) {
                    throw new RuntimeException("Self-described dataset must be the first item.")
                }
                dsInfo = getDatasetInfo(datasetUri, data)
            }
            first = false

            Document incomingDoc = completeRecord(data, recordType)
            idsInInput.add(incomingDoc.getShortId())

            // This race condition should be benign. If there is a document with
            // the same ID created in between the check and the creation, we'll
            // get an exception and fail early (unfortunate but acceptable).
            createOrUpdateDocument(incomingDoc, { wasCreated ->
                if (wasCreated) {
                    ++createdCount
                } else {
                    ++updatedCount
                }
            })

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
        if (dsInfo == null) {
            dsInfo =  new DatasetInfo([(ID): datasetUri])
        }
        long deletedCount = removeDeleted([] as Set, [])
        System.err.println("Deleted dataset ${dsInfo.uri} with ${deletedCount} existing records")
    }

    protected DatasetInfo getDatasetInfo(String datasetUri, Map givenData=null) {
        if (givenData) {
            Map dsData = givenData
            if (GRAPH in givenData) {
                dsData = (Map) givenData[GRAPH].find { it[ID] == datasetUri }
            }
            if (dsData[ID] != datasetUri) {
                throw new RuntimeException("Provided dataset ${givenData[ID]} does not match: ${datasetUri}")
            }
            return new DatasetInfo(dsData)
        }
        Document datasetRecord = whelk.storage.getDocumentByIri(datasetUri)
        if (datasetRecord == null) {
            throw new RuntimeException("Could not get dataset data for: $datasetUri")
        }
        Map datasetData = ((List) datasetRecord.data[GRAPH])[1]
        assert datasetData[ID] == datasetUri
        return new DatasetInfo(datasetData)
    }

    protected Document completeRecord(Map data, String recordType) {
        if (GRAPH !in data) {
            data = makeSystemRecordData(data, recordType)
        }

        normalizeLinks(data)

        Document doc = new Document(data)
        ensureAbsoluteSystemId(doc)
        doc.addInDataset(dsInfo.uri)

        List<String> thingIds = doc.getThingIdentifiers()
        if (dsInfo.uriRegexPattern && !thingIds.any { dsInfo.uriRegexPattern.matcher(it).matches() }) {
            throw new RuntimeException("None of ${thingIds} does not match ${dsInfo.uriRegexPattern}")
        }

        return doc
    }

    protected Map makeSystemRecordData(Map data, String recordType) {
        String givenId = data[ID]
        def slug = dsInfo.mintPredictableRecordSlug(givenId)
        assert slug
        def newRecordId = Document.BASE_URI.resolve(slug).toString()

        if (replaceMainIds) {
            def newId = newRecordId + HASH_IT
            addToSameAs(data, givenId)
            data[ID] = newId
        }

        Map record = (Map) data.remove('meta') ?: [:]
        record[ID] = newRecordId
        record[TYPE] = recordType
        record['mainEntity'] = [(ID): data[ID]]

        return [(GRAPH): [record, data]]
    }

    @CompileStatic(SKIP)
    protected void normalizeLinks(Map data) {
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
    }

    private void processDataset(String sourceUrl, Closure processItem) {
        if (sourceUrl ==~ /.+\.(jsonl|json(ld)?\.lines)$/) {
            File inDataFile = new File(sourceUrl)
            inDataFile.eachLine { line ->
                Map data = mapper.readValue(line.getBytes("UTF-8"), Map)
                processItem(data)
            }
        } else {
            Map data = new URL(sourceUrl).withInputStream { loadTurtleAsSystemShaped(it) }
            List<Map> graph = (List<Map>) data[GRAPH]
            for (Map item : graph) {
                processItem(item)
            }
        }
    }

    private Map loadData(String path) {
            File file = new File(path)
            if (path ==~ /.+\.ttl$/) {
                return (Map) file.withInputStream { loadTurtleAsSystemShaped(it) }
            } else if (path ==~ /.+\.(jsonld|json)/) {
                return mapper.readValue(file, Map)
            } else {
                throw new RuntimeException("Could not load: $path - unrecognized suffix")
            }
    }

    private Map loadTurtleAsSystemShaped(InputStream ins) {
        Map data = TrigToJsonLdParser.parse(ins)
        data = (Map) getTvm().applyTargetVocabularyMap(whelk.defaultTvmProfile, contextDocData, data)
    }

    private Document getDocByMainEntityId(String id) {
        return whelk.storage.loadDocumentByMainId(id, null)
    }

    private void createOrUpdateDocument(Document incomingDoc, Closure callback=null) {
        Document storedDoc = whelk.getDocument(incomingDoc.getShortId())
        if (storedDoc != null) {
            updateIfModified(incomingDoc, { if (callback) callback(false) })
        } else {
            whelk.createDocument(incomingDoc, "xl", null, collection, false)
            if (callback) callback(true)
        }
    }

    private Document updateIfModified(Document incomingDoc, Closure callback=null) {
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

    private long removeDeleted(Set<String> idsInInput, List<String> needsRetry) {
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

    private void ensureAbsoluteSystemId(Document doc) {
        def sysBaseIri = Document.BASE_URI
        // If system id is in place:
        if (doc.recordIdentifiers.any { it.startsWith(sysBaseIri.toString()) }) {
            return
        }
        // If a relative system id (slug) exists:
        def slug = doc.recordIdentifiers.find { it =~ /^\w+$/ }
        if (slug) {
            // Makes it absolute
            doc.setId(slug)
            return
        }
        // IDs need to be both reproducible and absolute:
        throw new RuntimeException("Could not obtain a proper record ID for: " + doc.recordIdentifiers.toString())
    }

    private void addToSameAs(Map entity, String id) {
        List<Map<String, String>> same = entity.get('sameAs', [])
        if (!same.find { it[ID] == id }) {
            same << [(ID): id]
        }
    }

}
