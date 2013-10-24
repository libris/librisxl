package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

/*
import me.prettyprint.hector.api.*
import me.prettyprint.hector.api.factory.*
import me.prettyprint.hector.api.ddl.*
import me.prettyprint.hector.api.exceptions.*
import me.prettyprint.cassandra.serializers.*
import me.prettyprint.cassandra.service.*
import me.prettyprint.cassandra.service.template.*
*/

import com.netflix.astyanax.*
import com.netflix.astyanax.impl.*
import com.netflix.astyanax.model.*
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException
import com.netflix.astyanax.connectionpool.*
import com.netflix.astyanax.connectionpool.impl.*
import com.netflix.astyanax.serializers.*
import com.netflix.astyanax.thrift.*

import com.google.common.collect.ImmutableMap

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.plugin.*

@Log
class CassandraStorage extends BasicPlugin implements Storage {

    Keyspace keyspace
    String requiredContentType

    ColumnFamily<String,Map> CF_DOCUMENT = new ColumnFamily<String, String>(
        "Document",
        StringSerializer.get(),
        StringSerializer.get(),
    )

    void init(String whelkName) {
        String cassandra_host = System.getProperty("cassandra.host")
        String cassandra_cluster = System.getProperty("cassandra.cluster")
        log.debug("Configuring Cassandra at ${cassandra_host}")
        log.debug("Setting up context.")
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
            .forCluster(cassandra_cluster)
            .forKeyspace(whelkName)
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
        keyspace = context.getClient();

        try {
            keyspace.describeKeyspace()
        } catch (Exception e) {
            log.debug("Creating keyspace.")
            keyspace.createKeyspace(
                ImmutableMap.<String, Object>builder()
                .put("strategy_options", ImmutableMap.<String, Object>builder()
                .put("replication_factor", "1")
                .build())
                .put("strategy_class",     "SimpleStrategy")
                .build()
            )

            log.debug("Creating columnfamily.")
            keyspace.createColumnFamily(CF_DOCUMENT, null)
        }
    }

    CassandraStorage() {}
    CassandraStorage(String rct) {
        this.requiredContentType = rct
    }


    @Override
    boolean store(Document doc, String whelkPrefix) {
        log.debug("Received document ${doc.identifier} with contenttype ${doc.contentType}")
        if (doc && (!requiredContentType || requiredContentType == doc.contentType)) {
            MutationBatch m = keyspace.prepareMutationBatch()
            m.withRow(CF_DOCUMENT, doc.identifier)
                .putColumn("data", new String(doc.data, "UTF-8"))
                .putColumn("entry", doc.metadataAsJson)
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
                log.debug("This storage does not handle document with type ${doc.contentType}")
            }
        }
        return false
    }

    @Override
    Document get(URI uri, String whelkPrefix) {
        OperationResult<ColumnList<String>> operation = keyspace.prepareQuery(CF_DOCUMENT).getKey(uri.toString()).execute()
        ColumnList<String> res = operation.getResult()

        log.debug("Operation result size: ${res.size()}")

        if (res.size() > 0) {
            log.debug("Digging up a document with identifier $uri from storage.")
            return new Document()
            .withIdentifier(uri)
            .withData(res.getColumnByName("data").getByteArrayValue())
            .withMetaEntry(res.getColumnByName("entry").getStringValue())
        }
        return null
    }

    @Override
    void delete(URI uri, String prefix) {
    }

    @Override
    Iterable<Document> getAll(String whelkPrefix) {
        return null
    }

}

/*
@Log
class HectorCassandraStorage extends BasicPlugin implements Storage {

    int REPLICATION_FACTOR = 1
    String COLUMN_FAMILY_NAME = "Resource"

    String requiredContentType

    Keyspace ksp
    ColumnFamilyTemplate<String, String> cft

    HectorCassandraStorage() {}
    HectorCassandraStorage(String rct) {
        this.requiredContentType = rct
    }

    void init(String whelkName) {
        String cassandra_host = System.getProperty("cassandra.host")
        String cassandra_cluster = System.getProperty("cassandra.cluster")
        log.info("Initializing cassandra storage in cluster $cassandra_cluster at $cassandra_host.")
        Cluster cluster = HFactory.getOrCreateCluster(cassandra_cluster, cassandra_host+":9160")
        log.debug("Found cluster: $cluster")
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(whelkName, COLUMN_FAMILY_NAME, ComparatorType.BYTESTYPE)
        log.debug("cfDef: $cfDef")
        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(whelkName)
        log.debug("keyspaceDef: $keyspaceDef")
        if (keyspaceDef == null) {
            KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(whelkName, ThriftKsDef.DEF_STRATEGY_CLASS, REPLICATION_FACTOR, Arrays.asList(cfDef))
            log.debug("new keyspace: $newKeyspace")
            cluster.addKeyspace(newKeyspace, true)
            keyspaceDef = cluster.describeKeyspace(whelkName)
            log.debug("Retrieved keyspacedef: $keyspaceDef")
        }
        ksp = HFactory.createKeyspace(whelkName, cluster)
        log.debug("ksp: $ksp")
        cft = new ThriftColumnFamilyTemplate<String, String>(ksp, COLUMN_FAMILY_NAME, StringSerializer.get(), StringSerializer.get())
    }

    @Override
    boolean store(Document doc, String whelkPrefix) {
        if (doc && (!requiredContentType || requiredContentType == doc.contentType)) {
            ColumnFamilyUpdater<String, String> updater = cft.createUpdater(doc.identifier)
            updater.setByteArray("data", doc.data)
            updater.setLong("timestamp", doc.timestamp)
            updater.setString("contentType", doc.contentType)

            try {
                cft.update(updater)
                log.debug("Stored document ${doc.identifier} ...")
            } catch (HectorException e) {
                log.error("Exception: ${e.message}", e)
            }
            return true
        } else {
            if (!doc) {
                log.warn("Received null document. No attempt to store.")
            } else if (log.isDebugEnabled()) {
                log.debug("This storage does not handle document with type ${doc.contentType}")
            }
        }
        return false
    }

    @Override
    Document get(URI uri, String whelkPrefix) {
        Document document
        try {
            ColumnFamilyResult<String, String> res = cft.queryColumns([uri.toString()])
            log.debug("res: $res")
            if (res?.hasResults()) {
                document = new Document().withIdentifier(uri).withData(res.getByteArray("data")).withContentType(res.getString("contentType"))
                document.setTimestamp(res.getLong("timestamp"))
            }
        } catch (HectorException e) {
            log.error("Exception: ${e.message}", e)
        }
        return document
    }

    @Override
    void delete(URI uri, String prefix) {
        try {
            cft.deleteRow(uri.toString())
        } catch (HectorException e) {
            log.error("Exception: ${e.message}", e)
        }
    }

    @Override
    Iterable<Document> getAll(String whelkPrefix) {
        return null
    }
}

@Entity
@DiscriminatorValue("document")
class CassandraDocument {
    @Id
    String identifier
    @Column(name="data")
    byte[] data
    @Column(name="entry")
    Map entry
    @Column(name="meta")
    Map meta

    CassandraDocument(final Document d) {
        this.identifier = d.identifier
        this.data = d.data
        this.entry = d.entry
        this.meta = d.meta
    }

    Document toDocument() {
        return new Document(identifier=this.identifier, data=this.data, entry=this.entry, meta=this.meta)
    }
}
*/
