package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import java.util.UUID

import com.netflix.astyanax.*
import com.netflix.astyanax.impl.*
import com.netflix.astyanax.model.*
import com.netflix.astyanax.query.*
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException
import com.netflix.astyanax.connectionpool.*
import com.netflix.astyanax.connectionpool.impl.*
import com.netflix.astyanax.serializers.*
import com.netflix.astyanax.thrift.*
import com.netflix.astyanax.util.*

import com.google.common.collect.ImmutableMap

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.exception.*

@Log
class CassandraStorage extends BasicPlugin implements Storage {

    Keyspace keyspace
    Keyspace versionsKeyspace
    List contentTypes

    final String CF_DOCUMENT_NAME = "document"
    final String COL_NAME_IDENTIFIER = "identifier"
    final String COL_NAME_DATA = "data"
    final String COL_NAME_ENTRY = "entry"
    final String COL_NAME_DATASET = "dataset"
    final String CREATE_TABLE_STATEMENT =
    String.format("CREATE TABLE %s (%s varchar, %s blob, %s varchar, %s varchar, PRIMARY KEY (%s)) WITH COMPACT STORAGE",
    CF_DOCUMENT_NAME, COL_NAME_IDENTIFIER, COL_NAME_DATA, COL_NAME_ENTRY, COL_NAME_DATASET,
    COL_NAME_IDENTIFIER)

    final String CREATE_INDEX_STATEMENT =
    String.format("CREATE INDEX %s ON %s (%s)",
    COL_NAME_DATASET, CF_DOCUMENT_NAME, COL_NAME_DATASET)

    ColumnFamily<String,String> CF_DOCUMENT = ColumnFamily.newColumnFamily(
        CF_DOCUMENT_NAME,
        StringSerializer.get(),
        StringSerializer.get())

    void init(String whelkName) {
        keyspace = setupKeyspace(whelkName+"_"+this.id)
        versionsKeyspace = setupKeyspace(whelkName+"_"+this.id+"_versions")
    }

    Keyspace setupKeyspace(String keyspaceName) {
        String cassandra_host = System.getProperty("cassandra.host")
        String cassandra_cluster = System.getProperty("cassandra.cluster")
        log.debug("Configuring Cassandra at ${cassandra_host}")
        log.debug("Setting up context.")
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
        .forCluster(cassandra_cluster)
        .forKeyspace(keyspaceName)
        .withAstyanaxConfiguration(
            new AstyanaxConfigurationImpl()
            .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
            .setCqlVersion("3.0.0")
            .setTargetCassandraVersion("1.2")
        )
        .withConnectionPoolConfiguration(
            new ConnectionPoolConfigurationImpl("WhelkConnectionPool")
            .setPort(9160)
            .setMaxConnsPerHost(1)
            .setSeeds(cassandra_host+":9160")
        )
        .withConnectionPoolMonitor(
            new CountingConnectionPoolMonitor()
        )
        .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace ksp = context.getClient();

        try {
            def r = ksp.describeKeyspace()
            log.debug("Keyspace in place: $r")
        } catch (Exception e) {
            log.debug("Creating keyspace.")
            ksp.createKeyspace(
                ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                .put("replication_factor", "1")
                .build())
                .put("strategy_class",     "SimpleStrategy")
                .build()
            )

            log.debug("Creating tables and indexes.")

            log.debug("CQL: "+CREATE_TABLE_STATEMENT)
            def result = ksp
            .prepareQuery(CF_DOCUMENT)
            .withCql(CREATE_TABLE_STATEMENT)
            .execute();
            log.debug("CQL: "+CREATE_INDEX_STATEMENT)
            result = ksp
            .prepareQuery(CF_DOCUMENT)
            .withCql(CREATE_INDEX_STATEMENT)
            .execute();
        }
        return ksp
    }

    CassandraStorage(Map settings) {
        super()
        this.contentTypes = settings.get("contentTypes", null)
    }

    @Override
    boolean store(Document doc, checkDigest = true) {
        return store(doc.identifier, doc, keyspace, checkDigest)
    }

    boolean store(String key, Document doc, Keyspace ksp, boolean checkDigest, boolean checkExisting = true, boolean checkContentType = true) {
        log.trace("Received document ${doc.identifier} with contenttype ${doc.contentType}")
        if (doc && (!checkContentType || handlesContent(doc.contentType) )) {
            def existingDocument = (checkExisting ? get(new URI(doc.identifier)) : null)
            if (checkDigest && existingDocument?.entry?.checksum && doc.entry?.checksum == existingDocument?.entry?.checksum) {
                throw new DocumentException(DocumentException.IDENTICAL_DOCUMENT, "Identical document already stored.")
            }
            if (existingDocument) {
                log.trace("Found changes in ${existingDocument.entry.checksum} (orig) and ${doc.entry.checksum} (new)")
                def entry = existingDocument.entry
                int version = (entry.version ?: 1) as int
                doc.entry.version = "" + (version+1)
                String versionedKey = doc.identifier+"?version="+version
                // Create versions
                def versions = entry.versions ?: [:]
                versions[""+version] = ["timestamp" : entry.timestamp, "checksum":entry.checksum]
                doc.entry.versions = versions

                log.debug("existingDocument entry: $entry")
                log.debug("new document entry: ${doc.entry}")
                doc = doc.mergeEntry(entry)
                log.debug("new document entry after merge: ${doc.entry}")
                log.debug("Saving versioned document with versionedKey $versionedKey")
                store(versionedKey, existingDocument, versionsKeyspace, false, false, false)
            }

            // Commence saving
            MutationBatch m = ksp.prepareMutationBatch()
            String dataset = (doc.entry?.dataset ? doc.entry.dataset : "default")
            log.trace("Saving document ${key} with dataset $dataset")
            m.withRow(CF_DOCUMENT, key)
                .putColumn(COL_NAME_IDENTIFIER , doc.identifier)
                .putColumn(COL_NAME_DATA, new String(doc.data, "UTF-8"), null)
                .putColumn(COL_NAME_ENTRY, doc.metadataAsJson, null)
                .putColumn(COL_NAME_DATASET, dataset, null)
            try {
                def result = m.execute()
            } catch (ConnectionException ce) {
                log.error("Connection failed", ce)
                return false
            }
            return true
        } else {
            if (!doc) {
                log.warn("Received null document. No attempt to store.")
            } else if (log.isDebugEnabled()) {
                log.debug("This storage (${this.id}) does not handle document with type ${doc.contentType}. Not saving ${key}")
            }
        }
        return false
    }

    @Override
    Document get(URI uri, String version=null) {
        return get(uri.toString(), version)
    }

    Document get(String uri, String version=null) {
        Document document = null
        log.trace("Version is $version")

        OperationResult<ColumnList<String>> operation
        if (version) {
            operation = versionsKeyspace.prepareQuery(CF_DOCUMENT).getKey(uri+"?version=$version").execute()
        } else {
            operation = keyspace.prepareQuery(CF_DOCUMENT).getKey(uri).execute()
        }
        ColumnList<String> res = operation.getResult()

        log.trace("Search operation result size: ${res.size()}")

        if (res.size() > 0) {
            log.debug("Digging up a document with identifier $uri from ${this.id}.")
            document = new Document()
                .withIdentifier(res.getColumnByName(COL_NAME_IDENTIFIER).getStringValue())
                .withData(res.getColumnByName(COL_NAME_DATA).getByteArrayValue())
                .withMetaEntry(res.getColumnByName(COL_NAME_ENTRY).getStringValue())
        }
        if (!document && version) {
            log.trace("Did document loading fail because we explicitliy requested the latest version ($version)?")
            document = get(uri, null)
            if (document?.entry?.version != version) {
                log.trace(" -- No. Latest document version is ${document?.entry?.version}")
                document = null
            }
        }
        return document
    }


    @Override
    void delete(URI uri) {
        log.debug("Deleting document $uri")
        store(uri.toString(), createTombstone(uri), keyspace, true, true, false)
    }

    Document createTombstone(uri) {
        def tombstone = new Document().withIdentifier(uri).withContentType("application/json").withData('{"identifier":"${uri.toString()}","status":"deleted"}')
        tombstone.entry['deleted'] = true
        return tombstone
    }


    void olddelete(URI uri, version=null) {
        log.debug("Deleting document $uri")
        MutationBatch m
        if (version == null) {
            // Preserve a version in versionSpace
            def existingDocument = get(uri)
            if (existingDocument) {
                store(uri.toString()+"?version=deleted", existingDocument, versionsKeyspace, false, false)
            }
            m = keyspace.prepareMutationBatch()
            m.withRow(CF_DOCUMENT, uri.toString()).delete()
        } else {
            m = versionsKeyspace.prepareMutationBatch()
            m.withRow(CF_DOCUMENT, uri.toString()+"?version="+version).delete()
        }
        try {
            def result = m.execute()
        } catch (ConnectionException ce) {
            throw new WhelkRuntimeException("Failed to delete document with identifier $uri", ce)
        }
    }

    @Override
    Iterable<Document> getAll(String dataset = null) {
        return new Iterable<Document>() {
            Iterator<Document> iterator() {
                def query
                try {
                    if (dataset) {
                        log.debug("Using query: dataset=$dataset")
                        query = keyspace.prepareQuery(CF_DOCUMENT).searchWithIndex()
                        .autoPaginateRows(true)
                        .addExpression().whereColumn("dataset")
                        .equals().value(dataset)
                    } else {
                        log.debug("Using allrows()")
                        query = keyspace.prepareQuery(CF_DOCUMENT).getAllRows()
                    }
                    query = query
                        .setRowLimit(100)
                        .withColumnRange(new RangeBuilder().setLimit(10).build())
                } catch (ConnectionException e) {
                    log.error("Cassandra Query failed.", e)
                    throw e
                }
                return new CassandraIterator(query)
            }
        }
    }

    @Override
    boolean handlesContent(String ctype) {
        return (ctype == "*/*" || !this.contentTypes || this.contentTypes.contains(ctype))
    }

    class CassandraIterator implements Iterator<Document> {

        private Iterator iter
        def query

        CassandraIterator(q) {
            this.query = q
            iter = query.execute().getResult().iterator()
        }

        public boolean hasNext() {
            boolean hn,success = false
            while (!success) {
                try {
                    hn = iter.hasNext()
                    if (!hn && (query instanceof IndexQuery)) {
                        log.trace("Refilling rows (for indexquery)")
                        iter = query.execute().getResult().iterator()
                        hn = iter.hasNext()
                    }
                    success = true
                } catch (Exception ce) {
                    log.warn("Cassandra threw exception ${ce.class.name}: ${ce.message}. Holding for a second ...")
                    Thread.sleep(1000)
                }
            }
            return hn
        }

        public Document next() {
            boolean success = false
            def doc
            while (!success) {
                try {
                    Row<String,String> row = iter.next()
                    doc = new Document()
                        .withIdentifier(row.columns.getColumnByName(COL_NAME_IDENTIFIER).getStringValue())
                        .withData(row.columns.getColumnByName(COL_NAME_DATA).getByteArrayValue())
                        .withMetaEntry(row.columns.getColumnByName(COL_NAME_ENTRY).getStringValue())
                    success = true
                    log.trace("Next yielded ${doc.identifier}")
                } catch (Exception ce) {
                    log.warn("Cassandra threw exception ${ce.class.name}: ${ce.message}. Holding for a second ...")
                    Thread.sleep(1000)
                }
            }
            return doc
        }

        void remove() {}
    }
}
