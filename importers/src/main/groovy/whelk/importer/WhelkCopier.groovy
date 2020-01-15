package whelk.importer

import whelk.Whelk
import whelk.Document
import whelk.util.LegacyIntegrationTools

class WhelkCopier {

    static final int DEFAULT_FETCH_SIZE = 100

    Whelk source
    Whelk dest
    List recordIds

    private int copied = 0

    WhelkCopier(source, dest, recordIds) {
        this.source = source
        this.dest = dest
        this.recordIds = recordIds

        dest.storage.versioning = false
        dest.storage.doVerifyDocumentIdRetention = false
    }

    void run() {
        for (id in recordIds) {
            def doc
            if (id.contains("/")) {
                doc = source.storage.getDocumentByIri(id)
            }
            else
                doc = source.getDocument(id)
            if (doc == null) {
                System.err.println("Could not load document with ID: $id");
                continue
            }
            doc.baseUri = source.baseUri

            // this links to:
            for (relDoc in selectBySqlWhere("""id in (select dependsonid from lddb__dependencies where id = '${id}')""")) {
                if (relDoc.deleted) continue
                relDoc.baseUri = source.baseUri
                save(relDoc)
            }
            save(doc)
            // links to this:
            for (revDoc in selectBySqlWhere("""id in (select id from lddb__dependencies where dependsonid = '${id}')""")) {
                if (revDoc.deleted) continue
                revDoc.baseUri = source.baseUri
                save(revDoc)
            }
        }
        System.err.println "Copied $copied documents (from ${recordIds.size()} selected)."
    }

    Iterable<Document> selectBySqlWhere(whereClause) {
        def query = """
            SELECT id, data, created, modified, deleted
            FROM $source.storage.mainTableName
            WHERE $whereClause
            """
        def conn = source.storage.getConnection()
        conn.setAutoCommit(false)
        def stmt = conn.prepareStatement(query)
        stmt.setFetchSize(DEFAULT_FETCH_SIZE)
        def rs = stmt.executeQuery()
        source.storage.iterateDocuments(rs)
    }

    void save(doc) {
        System.err.println "[$copied] Copying $doc.shortId from $source.baseUri to $dest.baseUri"

        def newDataRepr = doc.dataAsString.replaceAll(/"${source.baseUri}/,
                '"'+dest.baseUri.toString())
        def newDoc = new Document(doc.mapper.readValue(newDataRepr, Map))

        def newId = dest.baseUri.resolve(doc.shortId).toString()
        newDoc.id = newId

        if (dest.storage.getMainId(newId)) {
            dest.storeAtomicUpdate(newDoc.getShortId(), true, "xl", "WhelkCopier") {
                it.data = newDoc.data
            }
        } else {
            def collection = LegacyIntegrationTools.determineLegacyCollection(newDoc, dest.jsonld)
            if (collection) {
                dest.createDocument(newDoc, "xl", "WhelkCopier", collection, false)
            } else {
                System.err.println "Collection could not be determined for id ${newDoc.getShortId()}, document will not be exported."
            }
        }
        copied++
    }

}
