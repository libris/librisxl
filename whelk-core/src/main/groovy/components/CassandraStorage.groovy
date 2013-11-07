package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import com.netflix.astyanax.*
import com.netflix.astyanax.impl.*
import com.netflix.astyanax.model.*
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
    String requiredContentType

    final static String CF_DOCUMENT_NAME = "document"
    final static String COL_NAME_IDENTIFIER = "identifier"
    final static String COL_NAME_DATA = "data"
    final static String COL_NAME_ENTRY = "entry"
    final static String COL_NAME_DATASET = "dataset"
    final static String CREATE_TABLE_STATEMENT =
      String.format("CREATE TABLE %s (%s varchar, %s blob, %s varchar, %s varchar, PRIMARY KEY (%s)) WITH COMPACT STORAGE",
          CF_DOCUMENT_NAME, COL_NAME_IDENTIFIER, COL_NAME_DATA, COL_NAME_ENTRY, COL_NAME_DATASET,
          COL_NAME_IDENTIFIER)

    final static String CREATE_INDEX_STATEMENT =
        String.format("CREATE INDEX %s ON %s (%s)",
            COL_NAME_DATASET, CF_DOCUMENT_NAME, COL_NAME_DATASET)

      ColumnFamily<String,String> CF_DOCUMENT = ColumnFamily.newColumnFamily(
          CF_DOCUMENT_NAME,
          StringSerializer.get(),
          StringSerializer.get())

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
              def r = keyspace.describeKeyspace()
              log.debug("keyspace: $r")
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

              log.debug("Creating tables and indexes.")

              log.debug("CQL: "+CREATE_TABLE_STATEMENT)
              def result = keyspace
                  .prepareQuery(CF_DOCUMENT)
                  .withCql(CREATE_TABLE_STATEMENT)
                  .execute();
              log.debug("CQL: "+CREATE_INDEX_STATEMENT)
              result = keyspace
                  .prepareQuery(CF_DOCUMENT)
                  .withCql(CREATE_INDEX_STATEMENT)
                  .execute();
          }
      }

    CassandraStorage() {}
    CassandraStorage(String rct) {
        this.requiredContentType = rct
    }


    @Override
    boolean store(Document doc) {
        log.debug("Received document ${doc.identifier} with contenttype ${doc.contentType}")
        if (doc && (!requiredContentType || requiredContentType == doc.contentType)) {
            MutationBatch m = keyspace.prepareMutationBatch()
            String dataset = (doc.entry?.dataset ? doc.entry.dataset : "default")
            log.debug("Saving document ${doc.identifier} with dataset $dataset")
            m.withRow(CF_DOCUMENT, doc.identifier)
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
                log.debug("This storage does not handle document with type ${doc.contentType}")
            }
        }
        return false
    }

    @Override
    Document get(URI uri) {
        OperationResult<ColumnList<String>> operation = keyspace.prepareQuery(CF_DOCUMENT).getKey(uri.toString()).execute()
        ColumnList<String> res = operation.getResult()

        log.debug("Operation result size: ${res.size()}")

        if (res.size() > 0) {
            log.debug("Digging up a document with identifier $uri from storage.")
            return new Document()
                .withIdentifier(res.getColumnByName(COL_NAME_IDENTIFIER).getStringValue())
                .withData(res.getColumnByName(COL_NAME_DATA).getByteArrayValue())
                .withMetaEntry(res.getColumnByName(COL_NAME_ENTRY).getStringValue())
        }
        return null
    }

    @Override
    void delete(URI uri) {
        log.debug("Deleting document $uri")
        MutationBatch m = keyspace.prepareMutationBatch()
        m.withRow(CF_DOCUMENT, uri.toString()).delete()
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
                Rows<String, String> rows
                try {
                    def q
                    if (dataset) {
                        log.debug("Using query: dataset=$dataset")
                        q = keyspace.prepareQuery(CF_DOCUMENT).searchWithIndex()
                            .addExpression().whereColumn("dataset")
                            .equals().value(dataset)
                    } else {
                        log.debug("Using allrows()")
                        q = keyspace.prepareQuery(CF_DOCUMENT).getAllRows()
                            .setExceptionCallback(new ExceptionCallback() {
                                @Override
                                public boolean onException(ConnectionException e) {
                                    try {
                                        log.warn("Cassandra threw exception. Holding for a second ...")
                                        Thread.sleep(1000);
                                } catch (InterruptedException e1) {
                                }
                                return true
                                }})
                    }
                    rows = q
                    .setRowLimit(10)
                    .withColumnRange(new RangeBuilder().setLimit(10).build())
                    .execute().getResult()
                } catch (ConnectionException e) {
                    log.error("Cassandra Query failed.", e)
                    throw e
                }
                return new CassandraIterator(rows)
            }
        }
    }

}

@Log
class CassandraIterator implements Iterator<Document> {

    private Iterator iter

    CassandraIterator(Rows rows) {
        iter = rows.iterator()
    }

    public boolean hasNext() {
        return iter.hasNext()
    }

    public Document next() {
        Row<String,String> row = iter.next()
        def doc = new Document()
        .withIdentifier(row.getKey())
        .withData(row.columns.getColumnByName("data").byteValue)
        .withMetaEntry(row.columns.getColumnByName("entry").stringValue)
        log.debug("Next yielded ${doc.identifier}")
        return doc
    }

    void remove() {}
}
