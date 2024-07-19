package whelk.importer

import whelk.Document
import whelk.Whelk
import whelk.history.DocumentVersion
import whelk.util.BlockingThreadPool
import whelk.util.LegacyIntegrationTools

import static whelk.util.Jackson.mapper

class WhelkCopier {

    static final int DEFAULT_FETCH_SIZE = 100
    static final int SAVE_BATCH_SIZE = 200

    Whelk source
    Whelk dest
    List recordIds
    String additionalTypes
    boolean shouldExcludeItems
    boolean shouldIncludeHistory
    BlockingThreadPool.SimplePool threadPool = BlockingThreadPool.simplePool(Runtime.getRuntime().availableProcessors())
    List<Document> saveQueue = []

    private int copied = 0
    private int copiedVersions = 0

    WhelkCopier(Whelk source, Whelk dest, List<String> recordIds, String additionalTypes, boolean shouldExcludeItems, boolean shouldIncludeHistory) {
        this.source = source
        this.dest = dest
        this.recordIds = recordIds
        this.additionalTypes = additionalTypes
        this.shouldExcludeItems = shouldExcludeItems
        this.shouldIncludeHistory = shouldIncludeHistory

        dest.storage.doVerifyDocumentIdRetention = false
    }

    void run() {
        TreeSet<String> alreadyImportedIDs = new TreeSet<>()

        if (additionalTypes) {
            String whereClause
            if (additionalTypes == "--all-types") {
                whereClause = "deleted = false"
                if (shouldExcludeItems) {
                    whereClause += " and data#>>'{@graph,1,@type}' != 'Item'"
                }
            } else {
                String[] types = additionalTypes.split(",")
                whereClause = "deleted = false and data#>>'{@graph,1,@type}' in (\n" +
                        "'" + types.join("','") + "'" + ")"
            }

            source.storage.withDbConnection {
                for (doc in selectBySqlWhere(whereClause)) {
                    if (doc.deleted) continue
                    doc.baseUri = source.baseUri
                    if (!alreadyImportedIDs.contains(doc.shortId)) {
                        alreadyImportedIDs.add(doc.shortId)
                        queueSave(doc)
                    }
                }
            }
        }

        for (String id in recordIds) {
            Document doc
            if (id.contains("/")) {
                doc = source.storage.getDocumentByIri(id)
            }
            else
                doc = source.getDocument(id)
            if (doc == null) {
                System.err.println("Could not load document with ID: $id")
                continue
            }
            doc.baseUri = source.baseUri

            // this links to:
            source.storage.withDbConnection {
                for (relDoc in selectBySqlWhere("""id in (select dependsonid from lddb__dependencies where id = '${id}')""")) {
                    if (relDoc.deleted) continue
                    relDoc.baseUri = source.baseUri
                    if (!alreadyImportedIDs.contains(relDoc.shortId)) {
                        alreadyImportedIDs.add(relDoc.shortId)
                        queueSave(relDoc)
                    }
                }
                if (!alreadyImportedIDs.contains(doc.shortId)) {
                    alreadyImportedIDs.add(doc.shortId)
                    queueSave(doc)
                }
            }
            // links to this:
            def linksToThisWhere
            if (shouldExcludeItems) {
                linksToThisWhere = "id in (select id from lddb__dependencies where dependsonid = '${id}' and relation != 'itemOf')"
            } else {
                linksToThisWhere = "id in (select id from lddb__dependencies where dependsonid = '${id}')"
            }
            source.storage.withDbConnection {
                for (revDoc in selectBySqlWhere(linksToThisWhere)) {
                    if (revDoc.deleted) continue
                    revDoc.baseUri = source.baseUri
                    if (!alreadyImportedIDs.contains(revDoc.shortId)) {
                        alreadyImportedIDs.add(revDoc.shortId)
                        queueSave(revDoc)
                    }
                }
            }
        }

        if (shouldIncludeHistory) {
            source.storage.withDbConnection {
                for (String shortId in alreadyImportedIDs) {
                    source.storage.loadDocumentHistory(shortId).eachWithIndex { DocumentVersion docVersion, i ->
                        // Skip the first (latest) version, it'll be added by quickCreateDocument
                        if (i == 0) {
                            return
                        }
                        docVersion.doc.baseUri = source.baseUri
                        // Add some out-of-record data so we know what to do/use in save()
                        docVersion.doc.data["_isVersion"] = true
                        docVersion.doc.data["_changedBy"] = docVersion.changedBy
                        docVersion.doc.data["_changedIn"] = docVersion.changedIn
                        queueSave(docVersion.doc)
                    }
                }
            }
        }

        flushSaveQueue()
        threadPool.awaitAllAndShutdown()

        dest.storage.reDenormalize()
        if (shouldIncludeHistory) {
            System.err.println("Copied ${copied} documents (from ${recordIds.size()} selected), including ${copiedVersions} historical versions.")
        } else {
            System.err.println("Copied ${copied} documents (from ${recordIds.size()} selected).")
        }
    }

    Iterable<Document> selectBySqlWhere(whereClause) {
        def query = """
            SELECT id, data, created, modified, deleted
            FROM lddb
            WHERE $whereClause
            """
        def conn = source.storage.getMyConnection()
        conn.setAutoCommit(false)
        def stmt = conn.prepareStatement(query)
        stmt.setFetchSize(DEFAULT_FETCH_SIZE)
        def rs = stmt.executeQuery()
        source.storage.iterateDocuments(rs)
    }

    void queueSave(Document doc) {
        saveQueue.add(doc)
        copied++
        if (shouldIncludeHistory && doc.data["_isVersion"]) {
            copiedVersions++
        }

        if (copied % 200 == 0)
            System.err.println "Records queued for copying: $copied"
        if (saveQueue.size() >= SAVE_BATCH_SIZE) {
            flushSaveQueue()
        }
    }

    void flushSaveQueue() {
        List<Document> batch = saveQueue
        saveQueue = []
        threadPool.submit(() -> {
            for (Document d : batch)
                save(d)
        })
    }

    void save(Document doc) {
        def libUriPlaceholder = "___TEMP_HARDCODED_LIB_BASEURI"
        def newDataRepr = doc.dataAsString.replaceAll( // Move all lib uris, to a temporary placeholder.
                '"\\Q' + source.baseUri.resolve("library/").toString() + '\\E',
                '"' + libUriPlaceholder)
        newDataRepr = newDataRepr.replaceAll( // Replace all other baseURIs
                '"\\Q' + source.baseUri.toString() + '\\E',
                '"' + dest.baseUri.toString())
        newDataRepr = newDataRepr.replaceAll( // Move the hardcoded lib uris back
                '"\\Q' + libUriPlaceholder + '\\E',
                '"' + source.baseUri.resolve("library/").toString())

        Document newDoc = new Document(mapper.readValue(newDataRepr, Map))

        def newId = dest.baseUri.resolve(doc.shortId).toString()
        newDoc.id = newId

        def collection = LegacyIntegrationTools.determineLegacyCollection(newDoc, dest.jsonld)
        if (collection != "definitions") {
            try {
                if (doc.data["_isVersion"]) {
                    Date created = Date.from(doc.getCreatedTimestamp())
                    Date modified = Date.from(doc.getModifiedTimestamp())
                    String changedIn = doc.data["_changedIn"]
                    String changedBy = doc.data["_changedBy"]
                    dest.quickCreateDocumentVersion(newDoc, created, modified, changedIn, changedBy, collection)
                } else {
                    dest.quickCreateDocument(newDoc, "xl", "WhelkCopier", collection)
                }
            } catch (Exception e) {
                System.err.println("Could not save $doc.shortId due to: $e")
            }
        } else {
            System.err.println "Collection could not be determined for id ${newDoc.getShortId()}, document will not be exported."
        }
    }

}
