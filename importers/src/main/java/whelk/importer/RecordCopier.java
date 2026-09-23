package whelk.importer;

import whelk.Document;
import whelk.Whelk;
import whelk.component.PostgreSQLComponent;
import whelk.history.DocumentVersion;
import whelk.util.LegacyIntegrationTools;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static whelk.util.Jackson.mapper;

/**
 * Copies a small, explicitly named set of records (and their immediate link
 * dependencies/dependers) from one whelk to another, and indexes them.
 *
 * Unlike {@link WhelkCopier}, which is built for bulk reloads driven by a file
 * of ids, this is meant for pulling in one or a few records from the command
 * line, so it indexes them directly instead of relying on a separate reindex.
 */
public class RecordCopier {

    private static final int FETCH_SIZE = 100;
    private static final int INDEX_BATCH_SIZE = 100;

    private final Whelk source;
    private final Whelk dest;
    private final List<String> recordIds;
    private final boolean copyVersions;
    private final boolean includeItems;
    private final boolean overwriteExisting;

    private int copied = 0;
    private int copiedVersions = 0;
    private int overwritten = 0;
    private int failed = 0;
    private final TreeSet<String> seenIds = new TreeSet<>();
    private final TreeSet<String> copiedIds = new TreeSet<>();
    private final List<String> skippedRecordIds = new ArrayList<>();

    public RecordCopier(Whelk source, Whelk dest, List<String> recordIds,
                        boolean copyVersions, boolean includeItems, boolean overwriteExisting) {
        this.source = source;
        this.dest = dest;
        this.recordIds = recordIds;
        this.copyVersions = copyVersions;
        this.includeItems = includeItems;
        this.overwriteExisting = overwriteExisting;

        dest.getStorage().setDoVerifyDocumentIdRetention(false);
    }

    public void run() {
        for (String id : recordIds) {
            Document doc = loadDocument(id);
            if (doc == null) {
                System.err.println("Could not load document with ID: " + id);
                skippedRecordIds.add(id);
                continue;
            }
            doc.setBaseUri(source.getBaseUri());

            String shortId = doc.getShortId();

            // If this record is already in the destination, either overwrite its contents
            // with the source version and carry on copying deps, or (default)
            // skip it and its deps entirely.
            if (dest.getDocument(shortId) != null) {
                if (!overwriteExisting) {
                    System.err.println("Skipping " + id + ", already present in the destination.");
                    skippedRecordIds.add(id);
                    continue;
                }
                if (!overwrite(doc)) {
                    skippedRecordIds.add(id);
                    continue;
                }
                overwritten++;
                // The record itself is now handled; copy() must not touch it again. Its link
                // deps are still copied below (new deps get added as usual).
                seenIds.add(shortId);
            }

            String dependenciesWhere =
                    "id in (select dependsonid from lddb__dependencies where id = '" + shortId + "')";

            String dependersWhere = includeItems
                    ? "id in (select id from lddb__dependencies where dependsonid = '" + shortId + "')"
                    : "id in (select id from lddb__dependencies where dependsonid = '" + shortId
                            + "' and relation != 'itemOf')";

            List<Document> dependencies = selectBySqlWhere(dependenciesWhere);
            List<Document> dependers = selectBySqlWhere(dependersWhere);

            System.err.println(shortId + ": " + dependencies.size() + " dependencies; "
                + dependers.size() + " dependers");

            for (Document relDoc : dependencies) {
                copy(relDoc, false);
            }

            // The record itself. Copied after its dependencies so that links
            // resolve, and always with versions if --copy-versions is set.
            copy(doc, copyVersions);

            for (Document revDoc : dependers) {
                copy(revDoc, false);
            }
        }

        reDenormalizeCopied();
        index();

        if (copyVersions) {
            System.err.println("Copied " + copied + " documents (from " + recordIds.size() + " selected), "
                    + "including " + copiedVersions + " historical versions.");
        } else {
            System.err.println("Copied " + copied + " documents (from " + recordIds.size() + " selected).");
        }
        if (overwritten > 0) {
            System.err.println("Overwrote " + overwritten + " already-present records.");
        }
        if (!skippedRecordIds.isEmpty()) {
            System.err.println("Skipped " + skippedRecordIds.size() + " of the selected records "
                    + "(already present or not loadable): " + String.join(", ", skippedRecordIds));
        }
        if (failed > 0) {
            System.err.println("Skipped " + failed + " linked documents that could not be written "
                    + "(they most likely already exist in the destination).");
        }
    }

    private Document loadDocument(String id) {
        return id.contains("/")
                ? source.getStorage().getDocumentByIri(id)
                : source.getDocument(id);
    }

    /**
     * Copy a document, unless it is deleted or has already been copied.
     * Versions are only copied for the explicitly named records.
     */
    private void copy(Document doc, boolean withVersions) {
        if (doc.getDeleted()) {
            return;
        }
        doc.setBaseUri(source.getBaseUri());
        if (!seenIds.add(doc.getShortId())) {
            return;
        }

        if (!save(doc)) {
            failed++;
            return;
        }
        copiedIds.add(doc.getShortId());
        copied++;

        if (withVersions) {
            copyVersionsOf(doc.getShortId());
        }
    }

    /**
     * Replace an already-existing destination record's contents with the source version,
     * via an atomic update. Unlike copy(), this does not reconstruct the record's version
     * history (the update writes a single new version entry), so --copy-versions has no
     * effect on records that are overwritten.
     *
     * @return true if the record was updated.
     */
    private boolean overwrite(Document doc) {
        Document newDoc = rewriteToDest(doc);
        if (newDoc == null) {
            return false;
        }
        String shortId = newDoc.getShortId();
        try {
            // writeIdenticalVersions=true so that overwriting an identical record still
            // applies (and reindexes) rather than being cancelled as a no-op.
            boolean updated = dest.storeAtomicUpdate(shortId, false, true, true, "xl", "RecordCopier",
                    (Document existing) -> existing.data = newDoc.data);
            if (!updated) {
                System.err.println("Could not overwrite " + shortId + " (no update was applied).");
                return false;
            }
        } catch (Exception e) {
            System.err.println("Could not overwrite " + shortId + " due to: " + e);
            return false;
        }
        // storeAtomicUpdate already reindexed this record; no need to add it to copiedIds.
        return true;
    }

    private void copyVersionsOf(String shortId) {
        List<DocumentVersion> history = source.getStorage().loadDocumentHistory(shortId);
        for (int i = 0; i < history.size(); i++) {
            // Skip the first (latest) version, it was added by quickCreateDocument above.
            if (i == 0) {
                continue;
            }
            DocumentVersion docVersion = history.get(i);
            docVersion.doc.setBaseUri(source.getBaseUri());
            @SuppressWarnings("unchecked")
            Map<String, Object> data = docVersion.doc.data;
            data.put("_isVersion", true);
            data.put("_changedBy", docVersion.changedBy);
            data.put("_changedIn", docVersion.changedIn);
            save(docVersion.doc);
            copiedVersions++;
        }
    }

    private List<Document> selectBySqlWhere(String whereClause) {
        String query = """
                SELECT id, data, created, modified, deleted
                FROM lddb
                WHERE %s
                """.formatted(whereClause);

        List<Document> docs = new ArrayList<>();
        try (Connection conn = source.getStorage().getOuterConnection()) {
            conn.setAutoCommit(false);
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setFetchSize(FETCH_SIZE);
            ResultSet rs = stmt.executeQuery();
            for (Document doc : PostgreSQLComponent.iterateDocuments(rs)) {
                docs.add(doc);
            }
        } catch (java.sql.SQLException e) {
            throw new RuntimeException(e);
        }
        return docs;
    }

    private Document rewriteToDest(Document doc) {
        String libUriPlaceholder = "___TEMP_HARDCODED_LIB_BASEURI";
        URI sourceBase = source.getBaseUri();
        String libBase = sourceBase.resolve("library/").toString();

        String newDataRepr = doc.getDataAsString().replaceAll( // Move all lib uris, to a temporary placeholder.
                "\"\\Q" + libBase + "\\E",
                "\"" + libUriPlaceholder);
        newDataRepr = newDataRepr.replaceAll( // Replace all other baseURIs
                "\"\\Q" + sourceBase + "\\E",
                "\"" + dest.getBaseUri());
        newDataRepr = newDataRepr.replaceAll( // Move the hardcoded lib uris back
                "\"\\Q" + libUriPlaceholder + "\\E",
                "\"" + libBase);

        Document newDoc;
        try {
            newDoc = new Document(mapper.readValue(newDataRepr, Map.class));
        } catch (Exception e) {
            System.err.println("Could not parse " + doc.getShortId() + " due to: " + e);
            return null;
        }
        newDoc.setId(dest.getBaseUri().resolve(doc.getShortId()).toString());
        return newDoc;
    }

    private boolean save(Document doc) {
        Document newDoc = rewriteToDest(doc);
        if (newDoc == null) {
            return false;
        }

        String collection = LegacyIntegrationTools.determineLegacyCollection(newDoc, dest.getJsonld());
        if ("definitions".equals(collection)) {
            System.err.println("Collection could not be determined for id " + newDoc.getShortId()
                    + ", document will not be copied.");
            return false;
        }

        try {
            if (Boolean.TRUE.equals(doc.data.get("_isVersion"))) {
                Date created = Date.from((Instant) doc.getCreatedTimestamp());
                Date modified = Date.from((Instant) doc.getModifiedTimestamp());
                String changedIn = (String) doc.data.get("_changedIn");
                String changedBy = (String) doc.data.get("_changedBy");
                return dest.quickCreateDocumentVersion(newDoc, created, modified, changedIn, changedBy, collection);
            }
            return dest.quickCreateDocument(newDoc, "xl", "RecordCopier", collection);
        } catch (Exception e) {
            System.err.println("Could not save " + doc.getShortId() + " due to: " + e);
            return false;
        }
    }

    /**
     * Refresh the derivative tables for the copied. WhelkCopier() does storage.reDenormalize()
     * but we don't want to do that for the *entire* database here.
     */
    private void reDenormalizeCopied() {
        System.err.println("Re-denormalizing " + copiedIds.size() + " copied documents.");
        for (Document doc : dest.bulkLoad(copiedIds).values()) {
            dest.getStorage().refreshDerivativeTables(doc, true);
        }
    }

    private void index() {
        System.err.println("Indexing " + copiedIds.size() + " documents.");
        List<String> batch = new ArrayList<>(INDEX_BATCH_SIZE);
        for (String id : copiedIds) {
            batch.add(id);
            if (batch.size() == INDEX_BATCH_SIZE) {
                dest.elastic.bulkIndexWithRetry(batch, dest);
                batch = new ArrayList<>(INDEX_BATCH_SIZE);
            }
        }
        if (!batch.isEmpty()) {
            dest.elastic.bulkIndexWithRetry(batch, dest);
        }
    }
}
