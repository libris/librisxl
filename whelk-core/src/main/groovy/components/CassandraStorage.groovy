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
    ColumnFamilyTemplate<String, String> template

    CassandraStorage() {}

    void init(String whelkName) {
        String cassandra_host = System.getProperty("cassandra.host")
        String cassandra_cluster = System.getProperty("cassandra.cluster")
        log.info("Initializing cassandra storage in cluster $cassandra_cluster at $cassandra_host.")
        Cluster cluster = HFactory.getOrCreateCluster(cassandra_cluster, cassandra_host+":9160");
        log.debug("Found cluster: $cluster")
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(whelkName, COLUMN_FAMILY_NAME, ComparatorType.BYTESTYPE);
        log.debug("cfDef: $cfDef")
        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(whelkName);
        log.debug("keyspaceDef: $keyspaceDef")
        if (keyspaceDef == null) {
            KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(whelkName, ThriftKsDef.DEF_STRATEGY_CLASS, REPLICATION_FACTOR, Arrays.asList(cfDef));
            log.debug("new keyspace: $newKeyspace")
            cluster.addKeyspace(newKeyspace, true);
            keyspaceDef = cluster.describeKeyspace(whelkName);
            log.debug("Retrieved keyspacedef: $keyspaceDef")
        }
        ksp = HFactory.createKeyspace(whelkName, cluster);
        log.debug("ksp: $ksp")
        template = new ThriftColumnFamilyTemplate<String, String>(ksp, COLUMN_FAMILY_NAME, StringSerializer.get(), StringSerializer.get());
    }


    @Override
    void store(Document doc, String whelkPrefix) {
        // <String, String> correspond to key and Column name.
        ColumnFamilyUpdater<String, String> updater = template.createUpdater(doc.identifier.toString());
        log.debug("Storing document ...")
        updater.setByteArray("data", doc.data)
        updater.setLong("timestamp", doc.timestamp)
        updater.setString("format", doc.format)
        updater.setString("contentType", doc.contentType)

        try {
            template.update(updater);
        } catch (HectorException e) {
            log.error("Exception: ${e.message}", e)
        }
    }

    @Override
    Document get(URI uri, String whelkPrefix) {
        Document document
        try {
            ColumnFamilyResult<String, String> res = template.queryColumns(uri.toString());
            document = new BasicDocument().withIdentifier(uri).withData(res.getByteArray("data")).withFormat(res.getString("format")).withContentType(res.getString("contentType"))
            document.setTimestamp(res.getLong("timestamp"))
        } catch (HectorException e) {
            log.error("Exception: ${e.message}", e)
        }
        return document
    }

    @Override
    void delete(URI uri, String prefix) {
        try {
            template.deleteColumn("key", "column name");
        } catch (HectorException e) {
            // do something
        }
    }

    @Override
    Iterable<Document> getAll(String whelkPrefix) {
        return null
    }
}
