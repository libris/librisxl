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
    Keyspace ksp
    ColumnFamilyTemplate<String, String> template

    CassandraStorage() {}

    void init(String whelkName) {
        log.info("Initializing cassandra storage.")
        Cluster cluster = HFactory.getOrCreateCluster("test-cluster","localhost:9160");
        log.debug("Found cluster: $cluster")
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("MyKeyspace", "ColumnFamilyName", ComparatorType.BYTESTYPE);
        log.debug("cfDef: $cfDef")
        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace("MyKeyspace");
        log.debug("keyspaceDef: $keyspaceDef")
        if (keyspaceDef == null) {
            KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("MyKeyspace", ThriftKsDef.DEF_STRATEGY_CLASS, REPLICATION_FACTOR, Arrays.asList(cfDef));
            log.debug("new keyspace: $newKeyspace")
            cluster.addKeyspace(newKeyspace, true);
            keyspaceDef = cluster.describeKeyspace("MyKeyspace");
            log.debug("Retrieved keyspacedef: $keyspaceDef")
        }
        ksp = HFactory.createKeyspace("MyKeyspace", cluster);
        log.debug("ksp: $ksp")
        template = new ThriftColumnFamilyTemplate<String, String>(ksp, "ColumnFamilyName", StringSerializer.get(), StringSerializer.get());
    }


    @Override
    void store(Document doc, String whelkPrefix) {
        // <String, String> correspond to key and Column name.
        ColumnFamilyUpdater<String, String> updater = template.createUpdater("a key");
        updater.setString("domain", "www.datastax.com");
        updater.setLong("time", System.currentTimeMillis());
        log.debug("Storing document ...")

        try {
            template.update(updater);
        } catch (HectorException e) {
            log.error("Exception: ${e.message}", e)
        }
    }

    @Override
    Document get(URI uri, String whelkPrefix) {
        try {
            ColumnFamilyResult<String, String> res = template.queryColumns("a key");
            String value = res.getString("domain");
            // value should be "www.datastax.com" as per our previous insertion.
        } catch (HectorException e) {
            // do something ...
        }
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
