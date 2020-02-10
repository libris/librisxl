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
        TreeSet<String> alreadyImportedIDs = new TreeSet<>()

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
                if (!alreadyImportedIDs.contains(relDoc.shortId)) {
                    alreadyImportedIDs.add(relDoc.shortId)
                    save(relDoc)
                }
            }
            if (!alreadyImportedIDs.contains(doc.shortId)) {
                alreadyImportedIDs.add(doc.shortId)
                save(doc)
            }
            // links to this:
            for (revDoc in selectBySqlWhere("""id in (select id from lddb__dependencies where dependsonid = '${id}')""")) {
                if (revDoc.deleted) continue
                revDoc.baseUri = source.baseUri
                if (!alreadyImportedIDs.contains(revDoc.shortId)) {
                    alreadyImportedIDs.add(revDoc.shortId)
                    save(revDoc)
                }
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

        def libUriPlaceholder = "___TEMP_HARDCODED_LIB_BASEURI"
        def newDataRepr = doc.dataAsString.replaceAll( // Move all lib uris, to a temporary placeholder.
                source.baseUri.resolve("library/").toString().replace(".", "\\."),
                libUriPlaceholder)
        newDataRepr = newDataRepr.replaceAll( // Replace all other baseURIs
                source.baseUri.toString().replace(".", "\\."),
                dest.baseUri.toString().replace(".", "\\."))
        newDataRepr = newDataRepr.replaceAll( // Move the hardcoded lib uris back
                libUriPlaceholder,
                source.baseUri.resolve("library/").toString().replace(".", "\\."))

        def newDoc = new Document(doc.mapper.readValue(newDataRepr, Map))

        def newId = dest.baseUri.resolve(doc.shortId).toString()
        newDoc.id = newId

        def collection = LegacyIntegrationTools.determineLegacyCollection(newDoc, dest.jsonld)
        if (collection && collection != "definitions") {
            try {
                dest.quickCreateDocument(newDoc, "xl", "WhelkCopier", collection)
            } catch (Exception e) {
                System.err.println("Could not save $doc.shortId due to: $e")
            }
        } else {
            System.err.println "Collection could not be determined for id ${newDoc.getShortId()}, document will not be exported."
        }

        copied++
    }

}
