package whelk.importer

import groovy.util.logging.Slf4j as Log
import groovy.transform.CompileStatic
import whelk.exception.LinkValidationException

import whelk.Document
import whelk.JsonLd
import whelk.TargetVocabMapper
import whelk.Whelk
import whelk.converter.TrigToJsonLdParser
import whelk.converter.JsonLdShapes
import whelk.util.DocumentUtil

import java.time.Duration

import static whelk.JsonLd.asList
import static whelk.JsonLd.findInData
import static whelk.util.Jackson.mapper
import static whelk.util.LegacyIntegrationTools.NO_MARC_COLLECTION

@Log
@CompileStatic
class DatasetImporter {

    static String CONTEXT = JsonLd.CONTEXT_KEY
    static String VOCAB = JsonLd.VOCAB_KEY
    static String BASE = '@base'
    static String GRAPH = JsonLd.GRAPH_KEY
    static String ID = JsonLd.ID_KEY
    static String TYPE = JsonLd.TYPE_KEY
    static String VALUE = JsonLd.VALUE_KEY
    static String XSD_NS = 'http://www.w3.org/2001/XMLSchema#'

    static String HASH_IT = '#it'

    // Flags:
    // verify that id:s are served by the system; else use:
    static REPLACE_MAIN_IDS = 'replace-main-ids' // replace id with XL-id (move current to sameAs)

    static FORCE_DELETE = 'force-delete'
    
    enum WRITE_RESULT {
        ALREADY_UP_TO_DATE,
        UPDATED,
        CREATED
    }

    Whelk whelk
    String datasetUri

    DatasetInfo dsInfo
    private Document dsRecord
    Map givenDsData = null
    boolean useExistingDatasetDescription = false

    boolean replaceMainIds = false
    boolean forceDelete = false
    String collection = NO_MARC_COLLECTION

    TargetVocabMapper tvm = null
    Map contextDocData = null

    static final int THING_ID_CACHE_SIZE = 100_000
    static final int DEPENDENCY_CHECK_BATCH_SIZE = 100
    static final int INDEX_FLUSH_INTERVAL = 10_000

    private long writesSinceIndexFlush = 0

    // LRU cache for whelk.storage.getThingId() results
    private Map<String, String> thingIdCache = new LinkedHashMap<String, String>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > THING_ID_CACHE_SIZE
        }
    }

    DatasetImporter(Whelk whelk, String datasetUri, Map flags=[:], Object descriptions=null) {
        this.whelk = whelk
        this.datasetUri = datasetUri
        if (datasetUri != null) {
          log.info("Initialized DatasetImporter for ${datasetUri}")
        }
        log.info("Using system context: ${whelk.systemContextUri}")
        if (whelk.systemContextUri) {
            contextDocData = getDocByMainEntityId(whelk.systemContextUri)?.data
          if (contextDocData.containsKey(CONTEXT)) {
            def ctx = contextDocData.get(CONTEXT)
            if (ctx instanceof Map) log.info("Context size: ${ctx.size()}")
          } else {
            log.warn("Context missing ${CONTEXT}")
          }
        }

        if (descriptions != null) {
            Map datasetDesc = descriptions instanceof Map ? (Map) descriptions : loadData((String) descriptions)
            givenDsData = (Map) findInData(datasetDesc, datasetUri)
        }

        replaceMainIds = flags.get(REPLACE_MAIN_IDS) == true
        forceDelete = flags.get(FORCE_DELETE) == true

        if (Runtime.getRuntime().maxMemory() < 2l * 1024l * 1024l * 1024l) {
            log.warn("This application may require substantial amounts of memory, " +
                    "if the dataset in question is large. Please start with -Xmx3G (at least).")
            return
        }
    }

    static void loadDescribedDatasets(Whelk whelk, String datasetDescPath, String sourceBaseDir, Set<String> onlyDatasets=null, Map flags=[:]) {
        log.info("Loading datasets described in: ${datasetDescPath}")
        var dsImp = new DatasetImporter(whelk, null)
        var datasets = (Map) new File(datasetDescPath).withInputStream {
            dsImp.contextDocData ? dsImp.loadTurtleAsSystemShaped(it) : loadSelfCompactedTurtle(it)
        }
        for (Map item : (List<Map>) datasets[GRAPH] ?: asList(datasets)) {
            if (onlyDatasets && item[ID] !in onlyDatasets) {
                log.info("Skipping dataset: ${item[ID]}")
                continue
            }
            if (item[TYPE] == 'Dataset' && 'sourceData' in item) {
                Map sourceRef = item['sourceData']

                String sourceUrl = null
                if (ID in sourceRef) {
                    sourceUrl = sourceRef[ID]
                } else {
                    String sourcePath = asList(sourceRef['uri'])[0]
                    sourceUrl = new File(new File(sourceBaseDir), sourcePath).toString()
                }
                assert sourceUrl

                new DatasetImporter(whelk, (String) item[ID], flags, item).importDataset(sourceUrl)
            }
        }
    }

    TargetVocabMapper getTvm() {
        if (tvm == null) {
            tvm = new TargetVocabMapper(whelk.jsonld, contextDocData)
        }
        return tvm
    }

    void importDataset(String sourceUrl) {
        whelk.startDeferredIndexing()
        try {
            doImportDataset(sourceUrl)
        } finally {
            whelk.endDeferredIndexing()
        }
    }

    private void doImportDataset(String sourceUrl) {
        long startTime = System.nanoTime()
        log.info("Importing from: ${sourceUrl}")

        Set<String> idsInInput = []

        if (dsRecord != null) {
            idsInInput.add(dsRecord.getShortId())
        }

        String recordType = sourceUrl ==~ /^(https?):.+/ ? JsonLd.CACHE_RECORD_TYPE : JsonLd.RECORD_TYPE

        long updatedCount = 0
        long createdCount = 0
        long lineCount = 1 // The datasets' self describing first record also counts.

        boolean first = true

        processDataset(sourceUrl) { Map data ->
            if (first) {
                first = false
                String dsId = determineDatasetDescription(data)
                if (dsId) {
                    idsInInput.add(dsId)
                }
            } else if (dsInfo == null) {
                if (!first) {
                    throw new RuntimeException("Self-described dataset must be the first item.")
                }
            }

            Document incomingDoc = completeRecord(data, recordType, true)
            idsInInput.add(incomingDoc.getShortId())

            // This race condition should be benign. If there is a document with
            // the same ID created in between the check and the creation, we'll
            // get an exception and fail early (unfortunate but acceptable).
            switch (createOrUpdateDocument(incomingDoc)) {
                case WRITE_RESULT.CREATED:
                    createdCount++;
                    break;
                case WRITE_RESULT.UPDATED:
                    updatedCount++;
            }
            countWriteAndMaybeFlushIndexing()

            if ( lineCount % 100 == 0 ) {
                log.info("Processed " + lineCount + " input records. " + createdCount + " created, " +
                        updatedCount + " updated, " + (lineCount-createdCount-updatedCount) + " already up to date.")
            }
            ++lineCount
        }

        List<String> needsRetry = []
        long deletedCount = removeDeleted(idsInInput, needsRetry)

        // FIXME: this is a workaround for lddb__dependers not being populated correctly when a doc in a
        // dataset links to another doc in the dataset that has not yet been imported.
        // A symptom is for example @reverse/broader not being calculated correctly.
        // Should be fixed by merging PlaceholderRecord handling?
        recalculateStaleDependencies(idsInInput)

        System.err.println("Indexing remaining changes...")
        whelk.flushDeferredIndexing()

        Duration elapsedTime = Duration.ofNanos(System.nanoTime() - startTime)
        String elapsed = String.format("%02dh%02dm%02ds", elapsedTime.toHours(), elapsedTime.toMinutesPart(), elapsedTime.toSecondsPart())
        log.info("Created: " + createdCount +" new,\n" +
                "\tupdated: " + updatedCount + " existing and\n" +
                "\tdeleted: " + deletedCount + " old records (should have been: " + (deletedCount + needsRetry.size()) + "),\n" +
                "\tout of the: " + idsInInput.size() + " records in dataset: \"" + dsInfo.uri + "\".\n" +
                "\tDataset now in sync in ${elapsed}.")
    }

    void dropDataset() {
        if (dsInfo == null) {
            dsInfo =  new DatasetInfo([(ID): datasetUri])
        }
        long deletedCount
        whelk.startDeferredIndexing()
        try {
            deletedCount = removeDeleted([] as Set, [])
        } finally {
            whelk.endDeferredIndexing()
        }
        log.info("Deleted dataset ${dsInfo.uri} with ${deletedCount} existing records")
    }

    private void processDataset(String sourceUrl, Closure processItem) {
        if (sourceUrl ==~ /.+\.(ndjson(ld)?|jsonl|json(ld)?\.lines)$/) {
            File inDataFile = new File(sourceUrl)
            inDataFile.eachLine { line ->
                Map data = mapper.readValue(line.getBytes("UTF-8"), Map)
                processItem(data)
            }
        } else {
            Map data
            if (sourceUrl ==~ /^\w+:\/\/.+/) {
                data = new URL(sourceUrl).withInputStream { loadTurtleAsSystemShaped(it) }
            } else {
                data = (Map) new File(sourceUrl).withInputStream { loadTurtleAsSystemShaped(it) }
            }
            List<Map> graph = (List<Map>) data[GRAPH] ?: asList(data)
            for (Map item : graph) {
                processItem(item)
            }
        }
    }

    protected String determineDatasetDescription(Map data) {
        Map selfDescribedDsData = findInData(data, datasetUri)
        String dsId = null
        if (selfDescribedDsData != null) {
            log.info("Using self-described dataset description")
            setDatasetInfo(datasetUri, data)
        } else if (givenDsData != null) {
            log.info("Using given dataset description")
            setDatasetInfo(datasetUri, givenDsData)
            dsRecord = completeRecord(givenDsData, JsonLd.SYSTEM_RECORD_TYPE)
            createOrUpdateDocument(dsRecord)
            dsId = dsRecord.getShortId()
        } else if (useExistingDatasetDescription) {
            log.info("Using existing dataset description")
            lookupDatasetInfo(datasetUri)
        }
        return dsId
    }

    protected void setDatasetInfo(String datasetUri, Map givenData) {
        Map dsData = findInData(givenData, datasetUri)
        if (dsData == null) {
            throw new RuntimeException("Provided dataset ${givenData[ID]} does not match: ${datasetUri}")
        }
        dsInfo = new DatasetInfo(dsData)
        log.info("Using new dataset: ${dsInfo.uri}")
    }

    protected void lookupDatasetInfo(String datasetUri) {
        Document datasetRecord = whelk.storage.getDocumentByIri(datasetUri)
        if (datasetRecord == null) {
            throw new RuntimeException("Could not get dataset data for: $datasetUri")
        }
        Map datasetData = ((List) datasetRecord.data[GRAPH])[1]
        assert datasetData[ID] == datasetUri
        dsInfo = new DatasetInfo(datasetData)
        log.info("Using already defined dataset: ${dsInfo.uri}")
    }

    protected Document completeRecord(Map data, String recordType, boolean remap = false) {
        if (GRAPH !in data) {
            data = makeSystemRecordData(data, recordType)
        }

        if (remap) {
            applyMappings((Map) ((List) data[GRAPH])[1])
        }

        normalizeLinks(data)

        Document doc = new Document(data)
        ensureAbsoluteSystemId(doc)
        doc.addInDataset(dsInfo.uri)

        List<String> thingIds = doc.getThingIdentifiers()
        if (dsInfo.uriRegexPattern && !thingIds.any {
            it == dsInfo.uri || dsInfo.uriRegexPattern.matcher(it).matches()
        }) {
            throw new RuntimeException("None of ${thingIds} matches ${dsInfo.uriRegexPattern}")
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

    protected void applyMappings(Map mainEntity) {
        String type = mainEntity[TYPE]
        String mappedType = dsInfo.classMap[type]
        if (mappedType) {
            mainEntity[TYPE] = mappedType
        }
    }

    protected void normalizeLinks(Map data) {
        DocumentUtil.findKey(((List) data[GRAPH])[1], ID) { Object id, List<Object> path ->
            if ('sameAs' in path) {
                return DocumentUtil.NOP
            }
            String canonical = getThingId((String) id)
            if (canonical && id != canonical) {
                return new DocumentUtil.Replace(canonical)
            }
            return DocumentUtil.NOP
        }
    }

    private String getThingId(String id) {
        if (thingIdCache.containsKey(id)) {
            return thingIdCache[id]
        }
        String thingId = whelk.storage.getThingId(id)
        thingIdCache[id] = thingId
        return thingId
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

    /**
     * This method is <em>only</em> intended for initial whelk bootstrapping,
     * to initially load systemContext (contextDocData).
     */
    private static Map loadSelfCompactedTurtle(InputStream ins) {
        // Assuming that the Turtle *shape* follows a hard-coded system context!
        log.warn("Using loadSelfCompactedTurtle should only happen during setup of fresh whelk installation")
        Map data = (Map) TrigToJsonLdParser.parseRaw(ins)
        if (CONTEXT in data) {
            Map ctx = [:]
            ctx.putAll((Map) data[CONTEXT])
            // Make URI:s absolute
            ctx.remove(BASE)
            // Force partial system context shape:
            ctx['xsd'] = XSD_NS
            // Assumes VOCAB + created in source actually means this!
            ctx['created'] = [(TYPE): 'xsd:dateTime']
            data = (Map) JsonLdShapes.reCompact(data, [(CONTEXT): ctx])
        }
        return data
    }

    private Map loadTurtleAsSystemShaped(InputStream ins) {
        assert contextDocData
        Map data = TrigToJsonLdParser.parseRaw(ins)
        if (checkedSystemShaped(data, whelk.jsonld.vocabId)) {
            return (Map) JsonLdShapes.reCompact(data, contextDocData)
        } else {
            log.info("Applying target vocabulary map")
            return (Map) getTvm().applyTargetVocabularyMap(whelk.systemContextUri, contextDocData, data)
        }
    }

    private static boolean checkedSystemShaped(Map data, String vocabId) {
        if (data[CONTEXT] instanceof Map) {
            Map ctx = (Map) data[CONTEXT]
            int expectedSize = 0
            if (ctx[VOCAB] == vocabId) {
                expectedSize++
                if (ctx.containsKey(BASE)) {
                    expectedSize++
                }
                if (ctx['xsd'] == XSD_NS) {
                    expectedSize++
                }
            }
            if (ctx.size() == expectedSize) {
                // Force plain string uri value to be expanded as datatyped:
                if ('uri' !in ctx) {
                    ctx['uri'] = [(TYPE): 'xsd:anyURI']
                }
                return true
            }
        }
        return false
    }

    private Document getDocByMainEntityId(String id) {
        return whelk.storage.loadDocumentByMainId(id, null)
    }

    private WRITE_RESULT createOrUpdateDocument(Document incomingDoc) {
        Set<String> storedIds = new HashSet<>()
        WRITE_RESULT result
        if (whelk.storage.exists(incomingDoc.getShortId())) {
            boolean updated = whelk.storeAtomicUpdate(incomingDoc.getShortId(), true, false, false, "xl", null, { Document doc ->
                storedIds = identifiers(doc)
                doc.data = incomingDoc.data
            })
            if (updated) {
                result = WRITE_RESULT.UPDATED
            } else {
                result = WRITE_RESULT.ALREADY_UP_TO_DATE
            }
        } else {
            whelk.createDocument(incomingDoc, "xl", null, collection, false)
            result = WRITE_RESULT.CREATED
        }
        if (result != WRITE_RESULT.ALREADY_UP_TO_DATE) {
            thingIdCache.keySet().removeAll(storedIds)
            thingIdCache.keySet().removeAll(identifiers(incomingDoc))
        }
        return result
    }

    private static Set<String> identifiers(Document doc) {
        Set<String> ids = new HashSet<>(doc.getRecordIdentifiers())
        ids.addAll(doc.getThingIdentifiers())
        return ids
    }

    private void recalculateStaleDependencies(Set<String> idsInInput) {
        long recalculated = 0
        for (List<String> batch : new ArrayList<String>(idsInInput).collate(DEPENDENCY_CHECK_BATCH_SIZE)) {
            Map<String, Document> docs = whelk.storage.bulkLoad(batch)
            Map<String, Set<String>> stale = whelk.storage.findStaleDependencies(docs.values())
            stale.each { String id, Set<String> changedDependencies ->
                whelk.storage.recalculateDependencies(docs[id])
                recalculated++
            }
        }
        if (recalculated > 0) {
            System.err.println("Recalculated stale dependencies of ${recalculated} records.")
        }
    }

    private long removeDeleted(Set<String> idsInInput, List<String> needsRetry) {
        // Clear out anything that was previously stored in this dataset, but was not in the in-data now.
        // If faced with "can't delete depended on stuff", retry again later, after more other deletes have
        // succeeded (there may be intra-set dependencies). If the dataset contains circular dependencies,
        // deletions will never be possible until the circle is unlinked/broken somewhere along the chain.
        long deletedCount = 0
        whelk.storage.doForIdInDataset(dsInfo.uri, { String storedIdInDataset ->
            if (!idsInInput.contains(storedIdInDataset)) {
                if (!remove(storedIdInDataset, forceDelete)) {
                    needsRetry.add(storedIdInDataset)
                } else {
                    deletedCount++
                    if (deletedCount % 50 == 0) {
                        log.info("Cleaning up: " + deletedCount + " records deleted (they are no longer in the dataset).")
                    }
                }
            }
        })

        boolean anythingRemovedLastPass = true
        while (anythingRemovedLastPass) {
            anythingRemovedLastPass = false
            needsRetry.removeAll { String storedIdInDataset ->
                if (remove(storedIdInDataset, forceDelete)) {
                    anythingRemovedLastPass = true
                    deletedCount++
                    return true
                }
                return false
            }
        }

        if (!needsRetry.isEmpty()) {
            Set dependers = needsRetry.collect { whelk.storage.getDependers(it) }.flatten().toSet()
            if (needsRetry.containsAll(dependers)) {
                var removed = needsRetry.findResults { remove(it, true) ? it : null }
                deletedCount += removed.size()
                needsRetry.removeAll(removed)
                if (!needsRetry.isEmpty()) {
                    log.error("Could not force delete the following IDs:\n" + needsRetry)
                }
            }
        }
        
        if (!needsRetry.isEmpty()) {
            log.warn("The following IDs SHOULD have been deleted, but doing so was not " +
                    "possible, so they were skipped (most likely they are still depended upon):\n" + needsRetry)
        }
        
        return deletedCount
    }

    private boolean remove(String id, boolean force) {
        try {
            log.info("Removing " + id + " from dataset")
            whelk.remove(id, "xl", null, force)
            countWriteAndMaybeFlushIndexing()
            return true
        } catch (LinkValidationException ignored) {
            return false
        }
    }

    private void countWriteAndMaybeFlushIndexing() {
        if (++writesSinceIndexFlush >= INDEX_FLUSH_INTERVAL) {
            whelk.flushDeferredIndexing()
            writesSinceIndexFlush = 0
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
