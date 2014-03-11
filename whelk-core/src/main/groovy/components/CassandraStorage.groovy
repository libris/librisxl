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
import com.netflix.astyanax.annotations.Component

import com.google.common.collect.ImmutableMap

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.exception.*

class DocumentEntry {
    @Component(ordinal=0)
    Long timestamp
    @Component(ordinal=1)
    String field

    DocumentEntry() {}
    DocumentEntry(long ts, String f) {
        this.timestamp = ts
        this.field = f
    }
}

@Log
class CassandraStorage extends BasicPlugin implements Storage {

    Keyspace keyspace
    Keyspace versionsKeyspace
    List contentTypes
    boolean versioningStorage = true

    String cassandraVersion = "1.2"
    String CQLVersion = "3.0.0"

    final String CF_DOCUMENT_NAME = "document"
    final String CF_DOCUMENT_META_NAME = "document_meta"
    final String COL_NAME_IDENTIFIER = "identifier"
    final String COL_NAME_DATA = "data"
    final String COL_NAME_ENTRY = "entry"
    final String COL_NAME_DATASET = "dataset"
    final String COL_NAME_TIMESTAMP = "ts"

    AnnotatedCompositeSerializer<DocumentEntry> documentSerializer = new AnnotatedCompositeSerializer<DocumentEntry>(DocumentEntry.class)
    ColumnFamily<String, DocumentEntry> CF_DOCUMENT = new ColumnFamily<String, DocumentEntry>(CF_DOCUMENT_NAME, StringSerializer.get(), documentSerializer)
    ColumnFamily<String,String> CF_DOCUMENT_META = ColumnFamily.newColumnFamily(
        CF_DOCUMENT_META_NAME,
        StringSerializer.get(),
        StringSerializer.get())

    /*
    final String CREATE_TABLE_STATEMENT =
    String.format("CREATE TABLE %s (%s varchar, %s blob, %s varchar, %s varchar, %s timestamp, PRIMARY KEY (%s, %s))", // WITH COMPACT STORAGE",
    CF_DOCUMENT_NAME, COL_NAME_IDENTIFIER, COL_NAME_DATA, COL_NAME_ENTRY, COL_NAME_DATASET, COL_NAME_TIMESTAMP,
    COL_NAME_IDENTIFIER, COL_NAME_TIMESTAMP)

    final String INSERT_STATEMENT = "INSERT INTO $CF_DOCUMENT_NAME ($COL_NAME_IDENTIFIER, $COL_NAME_DATA, $COL_NAME_ENTRY, $COL_NAME_DATASET, $COL_NAME_TIMESTAMP) VALUES (?, ?, ?, ?, ?);";

    final String CREATE_INDEX_STATEMENT =
    String.format("CREATE INDEX %s_idx ON %s (%s)",
    COL_NAME_DATASET, CF_DOCUMENT_NAME, COL_NAME_DATASET)

    ColumnFamily<String,String> CF_DOCUMENT = ColumnFamily.newColumnFamily(
        CF_DOCUMENT_NAME,
        StringSerializer.get(),
        StringSerializer.get())
    */

    CassandraStorage(Map settings) {
        super()
        this.contentTypes = settings.get("contentTypes", null)
        this.versioningStorage = settings.get("versioning", true)
        this.cassandraVersion = settings.get("cassandraVersion", this.cassandraVersion)
        this.CQLVersion = settings.get("cqlVersion", this.CQLVersion)
    }


    void init(String whelkName) {
        keyspace = setupKeyspace(whelkName+"_"+this.id)
        if (versioningStorage) {
            versionsKeyspace = setupKeyspace(whelkName+"_"+this.id+"_versions")
        }
    }

    Keyspace setupKeyspace(String keyspaceName) {
        String cassandra_host = System.getProperty("cassandra.host")
        String cassandra_cluster = System.getProperty("cassandra.cluster")
        log.info("Configuring Cassandra version $cassandraVersion with CQL version $CQLVersion at ${cassandra_host}")
        log.debug("Setting up context.")
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
        .forCluster(cassandra_cluster)
        .forKeyspace(keyspaceName)
        .withAstyanaxConfiguration(
            new AstyanaxConfigurationImpl()
            .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
            .setCqlVersion(CQLVersion)
            .setTargetCassandraVersion(cassandraVersion)
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
            log.debug("Creating keyspace ${keyspaceName}.")
            ksp.createKeyspace(
                ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                .put("replication_factor", "1")
                .build())
                .put("strategy_class",     "SimpleStrategy")
                .build()
            )

            log.debug("Creating tables and indexes.")
        }
        log.debug("Check for columnfamily")
        try {
            if (!ksp.describeKeyspace().getColumnFamily(CF_DOCUMENT_NAME)) {
                log.info("Creating columnfamily $CF_DOCUMENT_NAME")

                //ksp.createColumnFamily(CF_DOCUMENT, null)
                /*
                ksp.createColumnFamily(CF_DOCUMENT, ImmutableMap.builder()
                    .put("default_validation_class", "UTF8Type")
                    .put("key_validation_class", "UTF8Type")
                    .put("comparator_type", "CompositeType(LongType, UTF8Type)")
                    .build()
                )
                */

                ksp.createColumnFamily(CF_DOCUMENT, ImmutableMap.builder()
                .put("default_validation_class", "UTF8Type")
                .put("key_validation_class", "UTF8Type")
                .put("comparator_type", "CompositeType(LongType, UTF8Type)")
                .build());




                /*
                    .put("column_metadata", ImmutableMap.<String, Object>builder()
                        .put("Index1", ImmutableMap.<String, Object>builder()
                            .put("validation_class", "UTF8Type")
                            .put("index_name",       "Index1")
                            .put("index_type",       "KEYS")
                            .build())
                        .build())
                    .put("default_validation_class", "UTF8Type")
                    .put("key_validation_class", "UTF8Type")
                    .build())
                    */
                    //.put("comparator_type", "CompositeType(LongType, UTF8Type)")
            }


        } catch (Exception ex) {
            log.error("Failed to create columnfamily: ${ex.message}", ex)
        }
        try {
            if (!ksp.describeKeyspace().getColumnFamily(CF_DOCUMENT_META_NAME)) {
                log.info("Creating columnfamily $CF_DOCUMENT_META_NAME")
                ksp.createColumnFamily(CF_DOCUMENT_META, ImmutableMap.builder()
                .put("column_metadata", ImmutableMap.builder()
                    .put(COL_NAME_DATASET, ImmutableMap.builder()
                        .put("validation_class", "UTF8Type")
                        .put("index_name",       COL_NAME_DATASET)
                        .put("index_type",       "KEYS")
                        .build())
                    .put(COL_NAME_DATA, ImmutableMap.builder()
                        .put("validation_class", "BytesType")
                        .build())
                    .put(COL_NAME_ENTRY, ImmutableMap.builder()
                        .put("validation_class", "UTF8Type")
                        .build())
                    .put(COL_NAME_TIMESTAMP, ImmutableMap.builder()
                        .put("validation_class", "LongType")
                        .build())
                    .build())
                .build())

            }
        } catch (Exception ex) {
            log.error("Failed to create columnfamily: ${ex.message}", ex)
        }

        return ksp
    }

    @Override
    boolean store(Document doc, checkDigest = true) {
        return store(doc.identifier, doc, keyspace, checkDigest)
    }

    boolean store(String key, Document doc, Keyspace ksp, boolean checkDigest, boolean checkExisting = true, boolean checkContentType = true) {
        log.trace("Received document ${doc.identifier} with contenttype ${doc.contentType}");
        if (doc && (!checkContentType || handlesContent(doc.contentType) )) {
            if (versioningStorage) {
                def existingDocument = (checkExisting ? get(new URI(doc.identifier)) : null)
                log.trace("existingDocument: $existingDocument")
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
                    versions[""+version] = ["timestamp" : entry.timestamp]
                    if (existingDocument?.entry?.deleted) {
                        versions.get(""+version).put("deleted",true)
                    } else {
                        versions.get(""+version).put("checksum",entry.checksum)
                    }
                    doc.entry.versions = versions

                    log.debug("existingDocument entry: $entry")
                    log.debug("new document entry: ${doc.entry}")
                    doc = doc.mergeEntry(entry)
                    log.debug("new document entry after merge: ${doc.entry}")
                    log.debug("Saving versioned document with versionedKey $versionedKey")
                    store(versionedKey, existingDocument, versionsKeyspace, false, false, false)
                }
            }

            // Commence saving
            String dataset = (doc.entry?.dataset ? doc.entry.dataset : "default")
            log.trace("Saving document ${key} with dataset $dataset")

            try {
                writeDocument(ksp, key, dataset, doc)
            } catch (ConnectionException ce) {
                log.error("Connection failed", ce)
                return false
            } catch (Exception e) {
                log.error("Error", e)
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

    void writeDocument(Keyspace ksp, String key, String dataset, Document doc) {
        MutationBatch mutation = ksp.prepareMutationBatch()

        mutation.withRow(CF_DOCUMENT, key).putColumn(new DocumentEntry(doc.timestamp, COL_NAME_DATA), doc.data, null)
        mutation.withRow(CF_DOCUMENT, key).putColumn(new DocumentEntry(doc.timestamp, COL_NAME_ENTRY), doc.metadataAsJson.getBytes("UTF-8"), null)
        mutation.withRow(CF_DOCUMENT, key).putColumn(new DocumentEntry(doc.timestamp, COL_NAME_DATASET), dataset.getBytes("UTF-8"), null)

        mutation.withRow(CF_DOCUMENT_META, key)
            .putColumn(COL_NAME_DATA, doc.data, null)
            .putColumn(COL_NAME_ENTRY, doc.metadataAsJson, null)
            .putColumn(COL_NAME_DATASET, dataset, null)
            .putColumn(COL_NAME_TIMESTAMP, doc.timestamp, null)

        def results = mutation.execute()
    }

    @Override
    Document get(URI uri, String version=null) {
        return get(uri.toString(), version)
    }

    Document get(String uri, String version=null) {
        Document document = null
        if (version && !versioningStorage) {
            throw new WhelkStorageException("Requested version from non-versioning storage")
        }
        log.trace("Version is $version")

        OperationResult<ColumnList<DocumentEntry>> operation
        if (version) {
            operation = versionsKeyspace.prepareQuery(CF_DOCUMENT).getKey(uri+"?version=$version").execute()
        } else {
            log.debug("Trying to load document with key:$uri")
            operation = keyspace.prepareQuery(CF_DOCUMENT).getKey(uri.toString()).execute()
        }
        ColumnList<DocumentEntry> res = operation.getResult()

        log.trace("Search operation result size: ${res.size()}")

        if (res.size() > 0) {
            log.trace("Digging up a document with identifier $uri from ${this.id}.")
            document = new Document()
            for (r in res) {
                DocumentEntry e = r.name
                document.withTimestamp(e.timestamp)
                if (e.field == COL_NAME_ENTRY) {
                    document.withMetaEntry(r.getStringValue())
                }
                if (e.field == COL_NAME_DATA) {
                    document.withData(r.getByteArrayValue())
                }
            }
        }
        if (!document && version) {
            log.trace("Did document loading fail because we explicitliy requested the latest version ($version)?")
            document = get(uri, null)
            if (document?.entry?.version != version) {
                log.trace(" -- No. Latest document version is ${document?.entry?.version}")
                document = null
            }
        }
        log.trace("Returning document $document")
        return document
    }


    @Override
    void delete(URI uri) {
        log.debug("Deleting document $uri")
        if (versioningStorage) {
            store(uri.toString(), createTombstone(uri), keyspace, true, true, false)
        } else {
            try {
                MutationBatch m = keyspace.prepareMutationBatch()
                m.withRow(CF_DOCUMENT, uri.toString()).delete()
                def result = m.execute()
            } catch (ConnectionException ce) {
                throw new WhelkRuntimeException("Failed to delete document with identifier $uri", ce)
            }
        }
    }

    Document createTombstone(uri) {
        def tombstone = new Document().withIdentifier(uri).withData("DELETED ENTRY")
        tombstone.entry['deleted'] = true
        return tombstone
    }

    @Override
    Iterable<Document> getAll(String dataset = null, Date since = null) {
        if (!dataset && since) {
            throw new QueryException("Parameter 'since' requires a 'dataset'")
        }
        return new Iterable<Document>() {
            Iterator<Document> iterator() {
                def query
                try {
                    if (dataset) {


                        /*
                        OperationResult<ColumnList<PostInfo>> result = getKeyspace()
                        .prepareQuery(CF_DOCUMENT)
                        .getKey("all")
                        .withColumnRange(new RangeBuilder()
                        .setLimit(5)
                        .setReversed(true)
                        .build())
                        .execute();
                        ColumnList<PostInfo> columns = result.getResult();
                        for (Column<PostInfo> column : columns) {
                        // do what you need here
                        }
                        */

                        log.debug("Using query: dataset=$dataset")
                        query = keyspace.prepareQuery(CF_DOCUMENT_META)
                            .searchWithIndex()
                            .addExpression()
                            .whereColumn(COL_NAME_DATASET).equals().value(dataset)
                            .autoPaginateRows(true)
                    } else {
                        log.debug("Using allrows()")
                        query = keyspace.prepareQuery(CF_DOCUMENT).getAllRows()
                    }
                    query = query
                        .setRowLimit(100)
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
                        log.debug("Refilling rows (for indexquery)")
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
                    def res = iter.next()
                    doc = new Document().withIdentifier(res.key)
                    for (c in res.columns) {
                        def field
                        if (c.name instanceof DocumentEntry) {
                            DocumentEntry e = c.name
                            doc.withTimestamp(e.timestamp)
                            field = e.field
                        } else {
                            field = c.name
                        }
                        log.trace("field is $field")
                        if (field == COL_NAME_ENTRY) {
                            doc.withMetaEntry(c.getStringValue())
                        }
                        if (field == COL_NAME_DATA) {
                            doc.withData(c.getByteArrayValue())
                        }
                        if (field == COL_NAME_TIMESTAMP) {
                            doc.withTimestamp(c.getLongValue())
                        }
                    }
                    success = true
                    log.trace("Next yielded ${doc.identifier} (${doc.dataAsString})")
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
