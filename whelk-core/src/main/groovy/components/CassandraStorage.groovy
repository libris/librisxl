package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import me.prettyprint.hector.api.*
import me.prettyprint.hector.api.factory.*
import me.prettyprint.hector.api.ddl.*
import me.prettyprint.hector.api.exceptions.*
import me.prettyprint.cassandra.serializers.*
import me.prettyprint.cassandra.service.*
import me.prettyprint.cassandra.service.template.*

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*

@Log
class CassandraStorage extends BasicPlugin implements Storage {

    int REPLICATION_FACTOR = 1
    String COLUMN_FAMILY_NAME = "Resource"

    Keyspace ksp
    ColumnFamilyTemplate<String, String> cft

    CassandraStorage() {}

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
    void store(Document doc, String whelkPrefix) {
        if (doc) {
            ColumnFamilyUpdater<String, String> updater = cft.createUpdater(doc.identifier.toString())
            updater.setByteArray("data", doc.data)
            updater.setLong("timestamp", doc.timestamp)
            updater.setString("contentType", doc.contentType)

            try {
                cft.update(updater)
                log.debug("Stored document ${doc.identifier} ...")
            } catch (HectorException e) {
                log.error("Exception: ${e.message}", e)
            }
        } else {
            log.warn("Received null document. No attempt to store.")
        }
    }

    @Override
    Document get(URI uri, String whelkPrefix) {
        Document document
        try {
            ColumnFamilyResult<String, String> res = cft.queryColumns([uri.toString()])
            log.debug("res: $res")
            if (res?.hasResults()) {
                document = new BasicDocument().withIdentifier(uri).withData(res.getByteArray("data")).withContentType(res.getString("contentType"))
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
