package whelk.importer

import groovy.util.logging.Log4j2 as Log
import groovy.transform.CompileStatic
import static groovy.transform.TypeCheckingMode.SKIP

import whelk.Document
import whelk.JsonLd
import whelk.TargetVocabMapper
import whelk.Whelk
import whelk.converter.TrigToJsonLdParser
import whelk.util.DocumentUtil

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

    Map<String, String> aliasMap = [:]

    DatasetImporter(Whelk whelk, String datasetUri, Map flags=[:], Object descriptions=null) {
        this.whelk = whelk
        this.datasetUri = datasetUri
        if (whelk.systemContextUri) {
            contextDocData = getDocByMainEntityId(whelk.systemContextUri)?.data
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
        var dsImp = new DatasetImporter(whelk, null)
        var datasets = (Map) new File(datasetDescPath).withInputStream {
            dsImp.contextDocData ? dsImp.loadTurtleAsSystemShaped(it) : loadSelfCompactedTurtle(it)
        }
        for (Map item : (List<Map>) datasets[GRAPH] ?: asList(datasets)) {
            if (onlyDatasets && item[ID] !in onlyDatasets) {
                System.err.println("Skipping dataset: ${item[ID]}")
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
        System.err.println("Importing from: ${sourceUrl}")

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

            if ( lineCount % 100 == 0 ) {
                System.err.println("Processed " + lineCount + " input records. " + createdCount + " created, " +
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
        idsInInput.each { whelk.storage.recalculateDependencies(whelk.getDocument(it)) }

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
            System.err.println("Using self-described dataset description")
            setDatasetInfo(datasetUri, data)
        } else if (givenDsData != null) {
            System.err.println("Using given dataset description")
            setDatasetInfo(datasetUri, givenDsData)
            dsRecord = completeRecord(givenDsData, JsonLd.SYSTEM_RECORD_TYPE)
            createOrUpdateDocument(dsRecord)
            dsId = dsRecord.getShortId()
        } else if (useExistingDatasetDescription) {
            System.err.println("Using existing dataset description")
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
        System.err.println("Using new dataset: ${dsInfo.uri}")
    }

    protected void lookupDatasetInfo(String datasetUri) {
        Document datasetRecord = whelk.storage.getDocumentByIri(datasetUri)
        if (datasetRecord == null) {
            throw new RuntimeException("Could not get dataset data for: $datasetUri")
        }
        Map datasetData = ((List) datasetRecord.data[GRAPH])[1]
        assert datasetData[ID] == datasetUri
        dsInfo = new DatasetInfo(datasetData)
        System.err.println("Using already defined dataset: ${dsInfo.uri}")
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

    private static Map loadSelfCompactedTurtle(InputStream ins) {
        // Assuming that the Turtle *shape* follows a hard-coded system context!
        Map data = (Map) TrigToJsonLdParser.parse(ins)
        if (CONTEXT in data) {
            Map ctx = [:]
            ctx.putAll((Map) data[CONTEXT])
            // Make URI:s absolute
            ctx.remove(BASE)
            // Force partial system context shape:
            ctx['xsd'] = XSD_NS
            // Assumes VOCAB + created in source actually means this!
            ctx['created'] = [(TYPE): 'xsd:dateTime']
            data = (Map) TrigToJsonLdParser.compact(data, [(CONTEXT): ctx])
        }
        return data
    }

    private Map loadTurtleAsSystemShaped(InputStream ins) {
        assert contextDocData
        Map data = TrigToJsonLdParser.parse(ins)
        if (data[CONTEXT] instanceof Map) {
            Map ctx = (Map) data[CONTEXT]
            int expectedSize = 0
            if (ctx[VOCAB] == whelk.jsonld.vocabId) {
                expectedSize++
                if (ctx.containsKey(BASE)) {
                    expectedSize++
                }
                if (ctx['xsd'] == XSD_NS) {
                    expectedSize++
                }
            }
            if (ctx.size() == expectedSize) {
                // Forces plain string uri values to be taken as datatyped
                if ('uri' !in ctx) {
                    ctx['uri'] = [(TYPE): 'xsd:anyURI']
                }
                return (Map) TrigToJsonLdParser.compact(data, contextDocData)
            }
        }
        return (Map) getTvm().applyTargetVocabularyMap(whelk.systemContextUri, contextDocData, data)
    }

    private Document getDocByMainEntityId(String id) {
        return whelk.storage.loadDocumentByMainId(id, null)
    }

    private WRITE_RESULT createOrUpdateDocument(Document incomingDoc) {
        Document storedDoc = whelk.getDocument(incomingDoc.getShortId())
        WRITE_RESULT result
        if (storedDoc != null) {
            boolean updated = whelk.storeAtomicUpdate(incomingDoc.getShortId(), true, false, "xl", null, { doc ->
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
        return result
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
                        System.err.println("Cleaning up: " + deletedCount + " records deleted (they are no longer in the dataset).")
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
            whelk.remove(id, "xl", null, force)
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
