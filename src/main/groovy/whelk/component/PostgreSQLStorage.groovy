package whelk.component

import groovy.util.logging.Slf4j as Log

import java.sql.*
import org.apache.commons.dbcp2.*
import org.postgresql.PGStatement

import whelk.*

@Log
class PostgreSQLStorage extends AbstractSQLStorage {

    String mainTableName, versionsTableName

    String jdbcDriver = "org.postgresql.Driver"

    // SQL statements
    protected String UPSERT_DOCUMENT, INSERT_DOCUMENT_VERSION, GET_DOCUMENT, GET_DOCUMENT_VERSION, GET_ALL_DOCUMENT_VERSIONS, GET_DOCUMENT_BY_ALTERNATE_ID, LOAD_ALL_STATEMENT, LOAD_ALL_STATEMENT_WITH_DATASET, DELETE_DOCUMENT_STATEMENT

    PostgreSQLStorage(String componentId = null, Map settings) {
        this.contentTypes = settings.get('contentTypes', null)
        this.versioning = settings.get('versioning', false)
        this.connectionUrl = settings.get("databaseUrl")
        this.mainTableName = settings.get('tableName', null)
        id = componentId
    }

    void componentBootstrap(String str) {
        log.info("Bootstrapping ${this.id}")
        if (!this.mainTableName) {
            this.mainTableName = str
        }
        if (versioning) {
            this.versionsTableName = mainTableName+VERSION_STORAGE_SUFFIX
        }
        UPSERT_DOCUMENT = "WITH upsert AS (UPDATE $mainTableName SET data = ?, dataset = ?, modified = ?, entry = ?, meta = ? WHERE identifier = ? RETURNING *) " +
            "INSERT INTO $mainTableName (identifier, data, dataset, modified, entry, meta) SELECT ?,?,?,?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)"


        INSERT_DOCUMENT_VERSION = "INSERT INTO $versionsTableName (identifier,data,checksum,modified,entry,meta) SELECT ?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM $versionsTableName WHERE identifier = ? AND checksum = ?)"

        GET_DOCUMENT = "SELECT identifier,data,entry,meta FROM $mainTableName WHERE identifier = ?"
        GET_DOCUMENT_VERSION = "SELECT identifier,data,entry,meta FROM $versionsTableName WHERE identifier = ? AND checksum = ?"
        GET_ALL_DOCUMENT_VERSIONS = "SELECT identifier,data,entry,meta FROM $versionsTableName WHERE identifier = ? ORDER BY modified"
        GET_DOCUMENT_BY_ALTERNATE_ID = "SELECT identifier,data,entry,meta FROM $mainTableName WHERE entry @> '{ \"alternateIdentifiers\": [?] }'"
        LOAD_ALL_STATEMENT = "SELECT identifier,data,entry,meta FROM $mainTableName WHERE modified >= ? AND modified <= ? AND identifier != ?  ORDER BY modified LIMIT 2000"
        LOAD_ALL_STATEMENT_WITH_DATASET = "SELECT identifier,data,entry,meta FROM $mainTableName WHERE modified >= ? AND modified <= ? AND identifier != ? AND dataset = ? ORDER BY modified LIMIT 2000"
        DELETE_DOCUMENT_STATEMENT = "DELETE FROM $mainTableName WHERE identifier = ?"
    }

    @Override
    void createTables() {
        Connection connection = connectionPool.getConnection()
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS $mainTableName ("
            +"identifier text primary key,"
            +"data bytea,"
            +"dataset text not null,"
            +"modified timestamp,"
            +"entry jsonb,"
            +"meta jsonb"
            +")");
        if (versioning) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS $versionsTableName ("
                +"id serial,"
                +"identifier text not null,"
                +"data bytea,"
                +"checksum char(32) not null,"
                +"modified timestamp,"
                +"entry jsonb,"
                +"meta jsonb,"
                +"UNIQUE (identifier, checksum)"
                +")");
            }
        stmt.close()
        connection.close()
    }

    @Override
    boolean store(Document doc, boolean withVersioning = versioning) {
        log.debug("Document ${doc.identifier} checksum before save: ${doc.checksum}")
        if (versioning && withVersioning) {
            if (!saveVersion(doc)) {
                return true // Same document already in storage.
            }
        }
        log.debug("Saving document ${doc.identifier} (with checksum: ${doc.checksum})")
        Connection connection = connectionPool.getConnection()
        PreparedStatement insert = connection.prepareStatement(UPSERT_DOCUMENT)
        try {
            insert.setBytes(1, doc.data)
            insert.setString(2, doc.dataset)
            insert.setTimestamp(3, new Timestamp(doc.modified))
            insert.setObject(4, doc.entryAsJson, java.sql.Types.OTHER)
            insert.setObject(5, doc.metaAsJson, java.sql.Types.OTHER)
            insert.setString(6, doc.identifier)
            insert.setString(7, doc.identifier)
            insert.setBytes(8, doc.data)
            insert.setString(9, doc.dataset)
            insert.setTimestamp(10, new Timestamp(doc.modified))
            insert.setObject(11, doc.entryAsJson, java.sql.Types.OTHER)
            insert.setObject(12, doc.metaAsJson, java.sql.Types.OTHER)
            insert.executeUpdate()
            return true
        } catch (Exception e) {
            log.error("Failed to save document: ${e.message}")
            throw e
        } finally {
            insert.close()
            connection.close()
        }
        return false
    }

    boolean saveVersion(Document doc) {
        Connection connection = connectionPool.getConnection()
        PreparedStatement insvers = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
        try {
            log.debug("Trying to save a version of ${doc.identifier} with checksum ${doc.checksum}. Modified: ${doc.modified}")
            insvers.setString(1, doc.identifier)
            insvers.setBytes(2, doc.data)
            insvers.setString(3, doc.checksum)
            insvers.setTimestamp(4, new Timestamp(doc.modified))
            insvers.setObject(5, doc.entryAsJson, java.sql.Types.OTHER)
            insvers.setObject(6, doc.metaAsJson, java.sql.Types.OTHER)
            insvers.setString(7, doc.identifier)
            insvers.setString(8, doc.checksum)
            int updated =  insvers.executeUpdate()
            log.debug("${updated > 0 ? 'New version saved.' : 'Already had same version'}")
            return (updated > 0)
        } catch (Exception e) {
            log.error("Failed to save document version: ${e.message}")
            throw e
        } finally {
            insvers.close()
            connection.close()
        }
    }

    @Override
    void bulkStore(final List docs) {
        log.debug("Bulk store requested. Versioning set to $versioning")
        if (!docs || docs.isEmpty()) {
            return
        }
        Connection connection = connectionPool.getConnection()
        PreparedStatement batch = connection.prepareStatement(UPSERT_DOCUMENT)
        PreparedStatement ver_batch = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
        try {
            docs.each { doc ->
                if (versioning) {
                    ver_batch.setString(1, doc.identifier)
                    ver_batch.setBytes(2, doc.data)
                    ver_batch.setString(3, doc.checksum)
                    ver_batch.setTimestamp(4, new Timestamp(doc.modified))
                    ver_batch.setObject(5, doc.entryAsJson, java.sql.Types.OTHER)
                    ver_batch.setObject(6, doc.metaAsJson, java.sql.Types.OTHER)
                    ver_batch.setString(7, doc.identifier)
                    ver_batch.setString(8, doc.checksum)
                    ver_batch.addBatch()
                }

                batch.setBytes(1, doc.data)
                batch.setString(2, doc.dataset)
                batch.setTimestamp(3, new Timestamp(doc.modified))
                batch.setObject(4, doc.entryAsJson, java.sql.Types.OTHER)
                batch.setObject(5, doc.metaAsJson, java.sql.Types.OTHER)
                batch.setString(6, doc.identifier)
                batch.setString(7, doc.identifier)
                batch.setBytes(8, doc.data)
                batch.setString(9, doc.dataset)
                batch.setTimestamp(10, new Timestamp(doc.modified))
                batch.setObject(11, doc.entryAsJson, java.sql.Types.OTHER)
                batch.setObject(12, doc.metaAsJson, java.sql.Types.OTHER)
                batch.addBatch()
            }
            ver_batch.executeBatch()
            batch.executeBatch()
        } catch (Exception e) {
            log.error("Failed to save batch: ${e.message}")
            throw e
        } finally {
            batch.close()
            ver_batch.close()
            connection.close()
        }
    }

    Document load(String id) {
        return load(id, null)
    }

    @Override
    Document load(String id, String version) {
        Document doc = null
        if (version && version.isInteger()) {
            int v = version.toInteger()
            def docList = loadAllVersions(id)
            if (v < docList.size()) {
                doc = docList[v]
            }
        } else if (version) {
            doc = loadFromSql(id, version, GET_DOCUMENT_VERSION)
        } else {
            doc = loadFromSql(id, null, GET_DOCUMENT)
        }
        return doc
    }

    private Document loadFromSql(String id, String checksum, String sql) {
        Document doc = null
        Connection connection = connectionPool.getConnection()
        PreparedStatement selectstmt
        ResultSet rs
        try {
            selectstmt = connection.prepareStatement(sql)
            if (id) {
                selectstmt.setString(1, id)
            }
            if (checksum) {
                selectstmt.setString(2, checksum)
            }
            rs = selectstmt.executeQuery()
            if (rs.next()) {
                doc = whelk.createDocument(rs.getBytes("data"), mapper.readValue(rs.getString("entry"), Map), mapper.readValue(rs.getString("meta"), Map))
            } else {
                log.trace("No results returned for get($id)")
            }
        } finally {
            if (rs) {
                rs.close()
            }
            if (selectstmt) {
                selectstmt.close()
            }
            connection.close()
        }
        return doc
    }

    @Override
    Document loadByAlternateIdentifier(String identifier) {
        String sql = GET_DOCUMENT_BY_ALTERNATE_ID.replace("?", '"' + identifier + '"')
        return loadFromSql(null, null, sql)
    }

    @Override
    Iterable<Document> loadAll() {
        return loadAll(null,null,null)
    }

    @Override
    List<Document> loadAllVersions(String identifier) {
        Connection connection = connectionPool.getConnection()
        PreparedStatement selectstmt
        ResultSet rs
        List<Document> docList = []
        try {
            selectstmt = connection.prepareStatement(GET_ALL_DOCUMENT_VERSIONS)
            selectstmt.setString(1, identifier)
            rs = selectstmt.executeQuery()
            int v = 0
            while (rs.next()) {
                def doc = whelk.createDocument(rs.getBytes("data"), mapper.readValue(rs.getString("entry"), Map), mapper.readValue(rs.getString("meta"), Map))
                doc.version = v++
                docList << doc
            }
        } finally {
            rs.close()
            selectstmt.close()
            connection.close()
        }
        return docList
    }

    Iterable<Document> loadAll(String dataset, Date since = null, Date until = null) {
        return new Iterable<Document>() {
            def results = new LinkedHashSet<Document>()
            Iterator<Document> iterator() {
                Iterator listIterator = null
                Connection connection = connectionPool.getConnection()
                PreparedStatement loadAllStatement
                long untilTS = until?.getTime() ?: PGStatement.DATE_POSITIVE_INFINITY

                if (dataset) {
                    loadAllStatement = connection.prepareStatement(LOAD_ALL_STATEMENT_WITH_DATASET)
                } else {
                    loadAllStatement = connection.prepareStatement(LOAD_ALL_STATEMENT)
                }

                return new Iterator<Document>() {
                    long lastModified = since?.getTime() ?: 0L
                    String lastIdentifier = ''
                    int nums = 0

                    public boolean hasNext() {
                        if (results.isEmpty()) {
                            log.debug("Getting results from postgres")
                            listIterator = null
                            loadAllStatement.setTimestamp(1, new Timestamp(lastModified))
                            loadAllStatement.setTimestamp(2, new Timestamp(untilTS))
                            loadAllStatement.setString(3, lastIdentifier)
                            if (dataset) {
                                loadAllStatement.setString(4, dataset)
                            }
                            ResultSet rs = loadAllStatement.executeQuery()
                            while (rs.next()) {
                                def doc = whelk.createDocument(rs.getBytes("data"), mapper.readValue(rs.getString("entry"), Map), mapper.readValue(rs.getString("meta"), Map))
                                lastModified = doc.modified
                                lastIdentifier = doc.identifier
                                results << doc
                            }
                            log.debug("Number of results in list: ${results.size()}")
                            listIterator = results.iterator()
                        }
                        return !results.isEmpty()
                    }
                    public Document next() {
                        if (listIterator == null) {
                            listIterator = results.iterator()
                        }
                        Document d = listIterator.next()
                        if (!listIterator.hasNext()) {
                            listIterator = null
                            results = new LinkedHashSet<Document>()
                        }
                        log.trace("Got document ${d?.identifier}")
                        return d
                    }
                    public void remove() { throw new UnsupportedOperationException(); }
                }
            }
        }
    }

    @Override
    void remove(String identifier, String dataset) {
        if (versioning) {
            log.debug("Creating tombstone record with id ${identifier}")
            store(createTombstone(identifier, dataset))
        } else {
            Connection connection = connectionPool.getConnection()
            PreparedStatement delstmt = connection.prepareStatement(DELETE_DOCUMENT_STATEMENT)
            try {
                delstmt.setString(1, identifier)
                delstmt.executeUpdate()
            } finally {
                delstmt.close()
                connection.close()
            }
        }
    }

    public void close() {
        log.info("Closing down postgresql connections.")
        try {
            statement.cancel()
            if (resultSet != null) {
                resultSet.close()
            }
        } catch (SQLException e) {
            log.warn("Exceptions on close. These are safe to ignore.", e)
        } finally {
            try {
                statement.close()
                conn.close()
            } catch (SQLException e) {
                log.warn("Exceptions on close. These are safe to ignore.", e)
            } finally {
                resultSet = null
                statement = null
                conn = null
            }
        }
    }
}
