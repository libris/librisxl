package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import java.util.UUID

import com.netflix.astyanax.*
import com.netflix.astyanax.impl.*
import com.netflix.astyanax.model.*
import com.netflix.astyanax.query.*
import com.netflix.astyanax.connectionpool.exceptions.*
import com.netflix.astyanax.connectionpool.*
import com.netflix.astyanax.connectionpool.impl.*
import com.netflix.astyanax.serializers.*
import com.netflix.astyanax.thrift.*
import com.netflix.astyanax.thrift.model.*
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
    Integer version
    @Component(ordinal=2)
    String field

    DocumentEntry() {}
    DocumentEntry(int v, long ts, String f) {
        this.version = v
        this.timestamp = ts
        this.field = f
    }
}

class TimestampEntry {
    @Component(ordinal=0)
    Long timestamp
    @Component(ordinal=1)
    String identifier
}

@Log
class CassandraStorage extends BasicPlugin implements Storage {

    Keyspace keyspace
    List contentTypes
    boolean versioningStorage = true

    String cassandraVersion = "1.2"
    String CQLVersion = "3.0.0"
    String keyspaceSuffix = ""

    final String CF_DOCUMENT_NAME = "document"
    final String CF_DOCUMENT_META_NAME = "document_meta"
    final String CF_TIMESTAMP_NAME = "timestamp"
    final String COL_NAME_IDENTIFIER = "identifier"
    final String COL_NAME_DATA = "data"
    final String COL_NAME_ENTRY = "entry"
    final String COL_NAME_DATASET = "dataset"
    final String COL_NAME_TIMESTAMP = "ts"
    final String COL_NAME_YEAR = "year"
    final String ROW_KEY_TIMESTAMP = "timestamp"

    AnnotatedCompositeSerializer<DocumentEntry> documentSerializer = new AnnotatedCompositeSerializer<DocumentEntry>(DocumentEntry.class)
    ColumnFamily<String, DocumentEntry> CF_DOCUMENT = new ColumnFamily<String, DocumentEntry>(CF_DOCUMENT_NAME, StringSerializer.get(), documentSerializer)
    AnnotatedCompositeSerializer<TimestampEntry> timestampSerializer = new AnnotatedCompositeSerializer<TimestampEntry>(TimestampEntry.class)
    ColumnFamily<String, TimestampEntry> CF_TIMESTAMP = new ColumnFamily<String, TimestampEntry>(CF_TIMESTAMP_NAME, StringSerializer.get(), timestampSerializer)

    ColumnFamily<String,String> CF_DOCUMENT_META = ColumnFamily.newColumnFamily(
        CF_DOCUMENT_META_NAME,
        StringSerializer.get(),
        StringSerializer.get())

    CassandraStorage(Map settings) {
        super()
        this.contentTypes = settings.get("contentTypes", null)
        this.versioningStorage = settings.get("versioning", true)
        this.cassandraVersion = settings.get("cassandraVersion", this.cassandraVersion)
        this.CQLVersion = settings.get("cqlVersion", this.CQLVersion)
        this.keyspaceSuffix = settings.get("keyspaceSuffix", "")
    }


    void init(String whelkName) {
        keyspace = setupKeyspace(whelkName+"_"+this.id + this.keyspaceSuffix)
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
                .put("comparator_type", "CompositeType(LongType, IntegerType, UTF8Type)")
                .build());
            }


        } catch (Exception ex) {
            log.error("Failed to create columnfamily: ${ex.message}", ex)
        }
        try {
            if (!ksp.describeKeyspace().getColumnFamily(CF_DOCUMENT_META_NAME)) {
                log.info("Creating columnfamily $CF_DOCUMENT_META_NAME")
                ksp.createColumnFamily(CF_DOCUMENT_META, ImmutableMap.builder()
                .put("column_metadata", ImmutableMap.builder()
                /*
                    .put(COL_NAME_YEAR, ImmutableMap.builder()
                        .put("validation_class", "IntegerType")
                        .put("index_name",       COL_NAME_YEAR)
                        .put("index_type",       "KEYS")
                        .build())
                        */
                    .put(COL_NAME_DATASET, ImmutableMap.builder()
                        .put("validation_class", "UTF8Type")
                        .put("index_name",       COL_NAME_DATASET+"_idx")
                        .put("index_type",       "KEYS")
                        .build())
                    .put(COL_NAME_DATA, ImmutableMap.builder()
                        .put("validation_class", "BytesType")
                        .build())
                    .put(COL_NAME_ENTRY, ImmutableMap.builder()
                        .put("validation_class", "UTF8Type")
                        .build())
                    .put(COL_NAME_TIMESTAMP, ImmutableMap.builder()
                        .put("validation_class", "DateType")
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
        return store(doc.identifier, doc, checkDigest)
    }

    boolean store(String key, Document doc, boolean checkDigest, boolean checkExisting = true, boolean checkContentType = true) {
        log.debug("[${this.id}] Received document ${doc.identifier} with contenttype ${doc.contentType}");
        if (doc && (!checkContentType || handlesContent(doc.contentType) )) {
            def existingDocument = (checkExisting ? get(new URI(doc.identifier)) : null)
            if (existingDocument) {
                log.debug("existing meta: ${existingDocument.meta} / ${doc.meta}")
            }
            if (checkDigest &&
                existingDocument?.meta == doc.meta &&
                doc.entry?.checksum == existingDocument?.entry?.checksum) {
                throw new DocumentException(DocumentException.IDENTICAL_DOCUMENT, "Identical document already stored.")
            }

            int version = (existingDocument ? existingDocument.version + 1 : 1)
            log.trace("Setting document version: $version")
            doc.withVersion(version)

            if (versioningStorage && existingDocument) {
                doc.version = version
                // Create versions
                def versions = existingDocument.entry.versions ?: [:]
                def lastVersion = existingDocument.version as String

                versions[lastVersion] = ["timestamp" : existingDocument.timestamp]
                if (existingDocument?.entry?.deleted) {
                    versions.get(lastVersion).put("deleted",true)
                } else {
                    versions.get(lastVersion).put("checksum",existingDocument.entry.checksum)
                }
                doc.entry.versions = versions
            }

            // Commence saving
            String dataset = (doc.entry?.dataset ? doc.entry.dataset : "default")
            log.trace("Saving document ${key} with dataset $dataset")

            try {
                writeDocument(key, dataset, doc)
            } catch (BadRequestException bre) {
                log.error("Error when saving: ${bre.message}", bre)
                throw bre
            } catch (ConnectionException ce) {
                log.error("Connection failed", ce)
                return false
            } catch (Exception e) {
                log.error("Error", e)
                throw e
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

    void writeDocument(String key, String dataset, Document doc) {
        boolean success = false
        while (!success) {
            try {
                MutationBatch mutation = keyspace.prepareMutationBatch()

                mutation.withRow(CF_DOCUMENT, key).putColumn(new DocumentEntry(doc.version, doc.timestamp, COL_NAME_DATA), doc.data, null)
                mutation.withRow(CF_DOCUMENT, key).putColumn(new DocumentEntry(doc.version, doc.timestamp, COL_NAME_ENTRY), doc.metadataAsJson.getBytes("UTF-8"), null)
                mutation.withRow(CF_DOCUMENT, key).putColumn(new DocumentEntry(doc.version, doc.timestamp, COL_NAME_DATASET), dataset.getBytes("UTF-8"), null)
                mutation.execute()

                mutation = keyspace.prepareMutationBatch()
                mutation.withRow(CF_DOCUMENT, ROW_KEY_TIMESTAMP).putColumn(new DocumentEntry(doc.version, doc.timestamp, COL_NAME_IDENTIFIER), key.getBytes("UTF-8"), null)
                mutation.execute()

                mutation = keyspace.prepareMutationBatch()
                mutation.withRow(CF_DOCUMENT_META, key)
                .putColumn(COL_NAME_DATA, doc.data, null)
                .putColumn(COL_NAME_ENTRY, doc.metadataAsJson, null)
                .putColumn(COL_NAME_DATASET, dataset, null)
                .putColumn(COL_NAME_TIMESTAMP, doc.timestamp, null)

                mutation.execute()
                success = true
            } catch (com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException nhe) {
                log.warn("No available cassandra host. Holding for a second ...", ce)
                Thread.sleep(1000)
            }
        }
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
        log.trace("Requested version is $version")
        boolean success = false
        while (!success) {
            try {

                OperationResult<ColumnList<DocumentEntry>> operation = keyspace.prepareQuery(CF_DOCUMENT).getKey(uri.toString()).execute()

                ColumnList<DocumentEntry> res = operation.getResult()

                log.trace("Get operation result size: ${res.size()}")

                if (res.size() > 0) {
                    log.trace("Digging up a document with identifier $uri from ${this.id}.")
                        document = new Document()
                        boolean foundCorrectVersion = false
                        for (r in res) {
                            DocumentEntry e = r.name
                            log.trace("Found docentry with version ${e.version}")
                            if (!version || e.version == version as int) {
                                foundCorrectVersion = true
                                document.withTimestamp(e.timestamp)
                                if (e.field == COL_NAME_ENTRY) {
                                    document.withMetaEntry(r.getStringValue())
                                }
                                if (e.field == COL_NAME_DATA) {
                                    document.withData(r.getByteArrayValue())
                                }
                            }
                        }
                    if (!foundCorrectVersion) {
                        document = null
                    }
                }
                success = true
            } catch (com.netflix.astyanax.connectionpool.exceptions.OperationTimeoutException ote) {
                log.warn("Get operation timed out. Holding for a second ...", ce)
                Thread.sleep(1000)
            }
        }
        log.trace("Returning document ${document?.identifier} (${document?.contentType}, version ${document?.version})")
        return document
    }


    @Override
    void delete(URI uri) {
        log.debug("Deleting document $uri")
        if (versioningStorage) {
            try {
                store(uri.toString(), createTombstone(uri), true, true, false)
            } catch (DocumentException de) {
                if (de.exceptionType == DocumentException.IDENTICAL_DOCUMENT) {
                    log.info("Document already deleted. Ignoring.")
                } else {
                    throw de
                }
            }
        }
        try {
            MutationBatch m = keyspace.prepareMutationBatch()
            if (!versioningStorage) {
                m.withRow(CF_DOCUMENT, uri.toString()).delete()
            }
            m.withRow(CF_DOCUMENT_META, uri.toString()).delete()
            def result = m.execute()
        } catch (ConnectionException ce) {
            throw new WhelkRuntimeException("Failed to delete document with identifier $uri", ce)
        }
    }

    Document createTombstone(uri) {
        def tombstone = new Document().withIdentifier(uri).withData("DELETED ENTRY")
        tombstone.entry['deleted'] = true
        return tombstone
    }

    @Override
    Iterable<Document> getAll(String dataset = null, Date since = null) {
        return new Iterable<Document>() {
            Iterator<Document> iterator() {
                def query
                try {
                    if (dataset) {
                        log.debug("Using query: dataset=$dataset")
                        query = keyspace.prepareQuery(CF_DOCUMENT_META)
                            .searchWithIndex()
                            .addExpression()
                            .whereColumn(COL_NAME_DATASET).equals().value(dataset)
                        if (since) {
                            log.debug("Adding ts > $since to query")
                            query = query
                                .addExpression()
                                .whereColumn(COL_NAME_TIMESTAMP).greaterThanEquals().value(since)
                        }
                    } else if (since) {
                        query = keyspace.prepareQuery(CF_DOCUMENT)
                            .getKey(ROW_KEY_TIMESTAMP)
                            .withColumnRange(
                                documentSerializer.buildRange().greaterThanEquals(since.getTime()).build()
                            )
                    } else if (since) {
                        log.debug("Searching year: ${since.getAt(Calendar.YEAR)}")
                        query = keyspace.prepareQuery(CF_DOCUMENT_META)
                            .searchWithIndex()
                            .addExpression()
                            .whereColumn(COL_NAME_YEAR).equals().value(since.getAt(Calendar.YEAR))
                            .addExpression()
                            .whereColumn(COL_NAME_TIMESTAMP).greaterThanEquals().value(since)
                    } else {
                        log.debug("Using allrows()")
                        query = keyspace.prepareQuery(CF_DOCUMENT).getAllRows()
                    }
                    if (query instanceof IndexQuery) {
                        query = query
                            .autoPaginateRows(true)
                            .setRowLimit(100)
                    }
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
        (ctype == "*/*" || !this.contentTypes || this.contentTypes.contains(ctype))
    }

    class CassandraIterator implements Iterator<Document> {

        Set<String> identifiers = new HashSet<String>()
        def documentQueue = [].asSynchronized()

        private Iterator iter
        def query

        boolean refilling = false

        CassandraIterator(q) {
            this.query = q
            iter = query.execute().getResult().iterator()
        }


        public boolean hasNext() {
            if (!documentQueue.size()) {
                refill()
            }
            return documentQueue.size()
        }

        public Document next() {
            while (refilling) {
                log.debug("Hold a moment, refilling queue.")
                Thread.sleep(1000)
            }
            def doc = documentQueue.pop()
            log.trace("Next yielded ${doc.identifier} with version ${doc.version}")
            return doc
        }

        void refill() {
            log.trace("Refilling")
            refilling = true
            def doc
            while (refilling) {
                try {
                    boolean deserizalisatonResult = false
                    while (!deserizalisatonResult && iter.hasNext()) {
                        (deserizalisatonResult, doc) = deserializeDocument(iter.next())
                    }
                    if (doc) {
                        documentQueue.push(doc)
                    }
                    refilling = false
                } catch (Exception ce) {
                    log.warn("Cassandra threw exception ${ce.class.name}: ${ce.message}. Holding for a second ...",ce)
                    Thread.sleep(1000)
                }
            }

        }

        /*
        public boolean oldhasNext() {
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
                    log.warn("Cassandra threw exception ${ce.class.name}: ${ce.message}. Holding for a second ...", ce)
                    Thread.sleep(1000)
                }
            }
            return hn
        }


        public Document oldnext() {
            boolean success = false
            def doc
            while (!success) {
                try {
                    boolean deserizalisatonResult = false
                    while (!deserizalisatonResult && iter.hasNext()) {
                        (deserizalisatonResult, doc) = deserializeDocument(iter.next())
                    }
                    log.trace("doc is $doc")
                    success = true
                    log.trace("Next yielded ${doc.identifier} with version ${doc.version}")
                } catch (Exception ce) {
                    log.warn("Cassandra threw exception ${ce.class.name}: ${ce.message}. Holding for a second ...",ce)
                    Thread.sleep(1000)
                }
            }
            return doc
        }
        */

        def deserializeDocument(res) {
            String id = null
            Document doc
            if (res instanceof ThriftColumnImpl) {
                log.trace("Found hit for timestamp search")
                DocumentEntry e = res.name
                if (e.field == COL_NAME_IDENTIFIER) {
                    id = res.getStringValue()
                    if (identifiers.contains(id)) {
                        log.trace("We have already loaded $id")
                        return [false, doc]
                    }
                    doc = get(id)
                }
            } else {
                log.trace("Found hit for index/matchall search")
                id = res.key
                if (id == ROW_KEY_TIMESTAMP || id == identifiers.contains(id)) {
                    if (log.isTraceEnabled()) {
                        if (id == ROW_KEY_TIMESTAMP) {
                            log.trace("Not looking for timestamp")
                        } else {
                            log.trace("We have already loaded $id")
                        }
                    }
                    return [false, doc]
                }
                doc = new Document().withIdentifier(id)

                for (c in res.columns) {
                    def field
                    if (c.name instanceof DocumentEntry) {
                        DocumentEntry e = c.name
                        doc.withTimestamp(e.timestamp)
                        field = e.field
                    } else {
                        field = c.name
                    }
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
            }
            identifiers << id
            log.trace("Deserialized document has identifier: ${doc.identifier}")
            return [true, doc]
        }

        void remove() {}
    }
}
