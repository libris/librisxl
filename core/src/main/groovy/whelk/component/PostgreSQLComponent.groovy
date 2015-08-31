package whelk.component

import groovy.util.logging.Slf4j as Log

import java.sql.*

import org.apache.commons.dbcp2.*
import org.postgresql.PGStatement
import org.codehaus.jackson.map.*

import whelk.*

@Log
class PostgreSQLComponent {

    protected String mainTableName, versionsTableName

    protected BasicDataSource connectionPool

    public final static DocumentFactory docFactory = new DocumentFactory()
    public final static mapper = new ObjectMapper()

    protected boolean readOnly = false
    protected boolean versioning = true
    protected String connectionUrl = null

    // SQL statements
    protected String UPSERT_DOCUMENT, INSERT_DOCUMENT_VERSION, GET_DOCUMENT, GET_DOCUMENT_VERSION, GET_ALL_DOCUMENT_VERSIONS, GET_DOCUMENT_BY_ALTERNATE_ID, LOAD_ALL_DOCUMENTS, LOAD_ALL_DOCUMENTS_WITH_LINKS, LOAD_ALL_DOCUMENTS_WITH_LINKS_BY_DATASET, LOAD_ALL_DOCUMENTS_BY_DATASET, DELETE_DOCUMENT_STATEMENT, STATUS_OF_DOCUMENT

    PostgreSQLComponent(String sqlUrl, String sqlMaintable, String sqlUsername, String sqlPassword) {
        //this.contentTypes = ["application/ld+json", "application/json", "application/x-marc-json"]
        this.connectionUrl = sqlUrl // props.getProperty("sql.url")
        this.mainTableName = sqlMaintable // props.getProperty("sql.maintable")

        String username = sqlUsername // props.getProperty("sql.username")
        String password = sqlPassword // props.getProperty("sql.password")

        log.info("Connecting to sql database at $connectionUrl")
        connectionPool = new BasicDataSource();

        if (username != null) {
            connectionPool.setUsername(username)
            connectionPool.setPassword(password)
        }
        connectionPool.setDriverClassName("org.postgresql.Driver")
        connectionPool.setUrl(connectionUrl)
        connectionPool.setInitialSize(10)
        connectionPool.setMaxTotal(40)
        connectionPool.setDefaultAutoCommit(true)
        // Setting up sql-statements
        if (versioning) {
            this.versionsTableName = mainTableName + "__versions"
        }
        UPSERT_DOCUMENT = "WITH upsert AS (UPDATE $mainTableName SET data = ?, manifest = ?, modified = ?, deleted = ? WHERE id = ? RETURNING *) " +
            "INSERT INTO $mainTableName (id, data, manifest, deleted) SELECT ?,?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)"


        INSERT_DOCUMENT_VERSION = "INSERT INTO $versionsTableName (id, data, manifest, checksum) SELECT ?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM $versionsTableName WHERE id = ? AND checksum = ?)"

        GET_DOCUMENT = "SELECT id,data,manifest,created,modified,deleted FROM $mainTableName WHERE id= ?"
        GET_DOCUMENT_VERSION = "SELECT id,data,manifest FROM $versionsTableName WHERE id = ? AND checksum = ?"
        GET_ALL_DOCUMENT_VERSIONS = "SELECT id,data,manifest,created,modified,deleted FROM $versionsTableName WHERE id = ? ORDER BY modified"
        GET_DOCUMENT_BY_ALTERNATE_ID = "SELECT id,data,manifest,created,modified,deleted FROM $mainTableName WHERE manifest @> '{ \"alternateIdentifiers\": [?] }'"
        LOAD_ALL_DOCUMENTS = "SELECT id,data,manifest,created,modified,deleted FROM $mainTableName WHERE modified >= ? AND modified <= ? ORDER BY modified"
        LOAD_ALL_DOCUMENTS_BY_DATASET = "SELECT id,data,manifest,created,modified,deleted FROM $mainTableName WHERE manifest->>'dataset' = ? AND modified >= ? AND modified <= ? ORDER BY modified"
        LOAD_ALL_DOCUMENTS_WITH_LINKS = """
            SELECT l.id as parent, r.identifier as identifier, r.data as data, r.manifest as manifest, r.meta as meta
            FROM (
                SELECT * FROM (
                    SELECT identifier as id, identifier as link FROM $mainTableName
                    UNION ALL
                    SELECT identifier as id, jsonb_array_elements_text(manifest->'links') as link FROM $mainTableName
                ) AS links GROUP by id,link
            ) l JOIN $mainTableName r ON l.link = r.identifier ORDER BY l.id
            """
        LOAD_ALL_DOCUMENTS_WITH_LINKS_BY_DATASET = """
            SELECT l.id as parent, r.identifier as identifier, r.data as data, r.manifest as manifest, r.meta as meta
            FROM (
                SELECT * FROM (
                    SELECT identifier as id, identifier as link FROM $mainTableName WHERE dataset = ?
                    UNION ALL
                    SELECT identifier as id, jsonb_array_elements_text(manifest->'links') as link FROM $mainTableName WHERE dataset = ?
                ) AS links GROUP by id,link
            ) l JOIN $mainTableName r ON l.link = r.identifier ORDER BY l.id
            """

        DELETE_DOCUMENT_STATEMENT = "DELETE FROM $mainTableName WHERE id = ?"
        STATUS_OF_DOCUMENT = "SELECT modified, deleted FROM $mainTableName WHERE id = ?"
    }

    public Map status(String identifier) {
        Map statusMap = [:]
        Connection connection
        try {
            connection = connectionPool.getConnection()
            PreparedStatement statusStmt = connection.prepareStatement(STATUS_OF_DOCUMENT)
            statusStmt.setString(1, identifier)
            def rs = statusStmt.executeQuery()
            if (rs.next()) {
                statusMap['exists'] = true
                statusMap['lastUpdate'] rs.getTimestamp("modified").getTime()
                statusMap['deleted'] = rs.getBoolean("deleted")
            } else {
                log.trace("No results returned for $id")
                statusMap['exists'] = false
            }
        } finally {
            connection.close()
        }
        return statusMap
    }

    boolean store(Document doc, boolean withVersioning = versioning) {
        log.debug("Document ${doc.identifier} checksum before save: ${doc.checksum}")
        if (versioning && withVersioning) {
            if (!saveVersion(doc)) {
                return true // Same document already in storage.
            }
        }
        assert doc.dataset
        log.debug("Saving document ${doc.identifier} (with checksum: ${doc.checksum})")
        Connection connection = connectionPool.getConnection()
        PreparedStatement insert = connection.prepareStatement(UPSERT_DOCUMENT) //.replaceAll(/\{tableName\}/, mainTableName + "_" + doc.dataset))
        try {
            insert = rigUpsertStatement(insert, doc)
            insert.executeUpdate()
            return true
        } catch (Exception e) {
            log.error("Failed to save document: ${e.message}")
            throw e
        } finally {
            connection.close()
            log.debug("[store] Closed connection.")
        }
        return false
    }

    private PreparedStatement rigUpsertStatement(PreparedStatement insert, Document doc) {
        insert.setObject(1, doc.dataAsString, java.sql.Types.OTHER)
        insert.setObject(2, doc.manifestAsJson, java.sql.Types.OTHER)
        insert.setTimestamp(3, new Timestamp(doc.modified))
        insert.setBoolean(4, doc.isDeleted())
        insert.setString(5, doc.identifier)
        insert.setString(6, doc.identifier)
        insert.setObject(7, doc.dataAsString, java.sql.Types.OTHER)
        insert.setObject(8, doc.manifestAsJson, java.sql.Types.OTHER)
        insert.setBoolean(9, doc.isDeleted())

        return insert
    }

    boolean saveVersion(Document doc) {
        Connection connection = connectionPool.getConnection()
        PreparedStatement insvers = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
        try {
            log.debug("Trying to save a version of ${doc.identifier} with checksum ${doc.checksum}. Modified: ${doc.modified}")
            insvers = rigVersionStatement(insvers, doc)
            int updated =  insvers.executeUpdate()
            log.debug("${updated > 0 ? 'New version saved.' : 'Already had same version'}")
            return (updated > 0)
        } catch (Exception e) {
            log.error("Failed to save document version: ${e.message}")
            throw e
        } finally {
            connection.close()
            log.debug("[saveVersion] Closed connection.")
        }
    }

    private PreparedStatement rigVersionStatement(PreparedStatement insvers, Document doc) {
        insvers.setString(1, doc.identifier)
        insvers.setObject(2, doc.dataAsString, java.sql.Types.OTHER)
        insvers.setObject(3, doc.manifestAsJson, java.sql.Types.OTHER)
        insvers.setString(4, doc.checksum)
        insvers.setString(5, doc.identifier)
        insvers.setString(6, doc.checksum)
        return insvers
    }

    void bulkStore(final List docs, String dataset) {
        if (!docs || docs.isEmpty()) {
            return
        }
        log.debug("Bulk storing ${docs.size()} documents.")
        Connection connection = connectionPool.getConnection()
        PreparedStatement batch = connection.prepareStatement(UPSERT_DOCUMENT) //.replaceAll(/\{tableName\}/, mainTableName + "_" + dataset))
        PreparedStatement ver_batch = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
        try {
            docs.each { doc ->
                if (versioning) {
                    ver_batch = rigVersionStatement(ver_batch, doc)
                    ver_batch.addBatch()
                }

                batch = rigUpsertStatement(batch, doc)

                batch.addBatch()
            }
            ver_batch.executeBatch()
            batch.executeBatch()
            log.debug("Stored ${docs.size()} documents with dataset $dataset (versioning: ${versioning})")
        } catch (Exception e) {
            log.error("Failed to save batch: ${e.message}")
            throw e
        } finally {
            connection.close()
            log.debug("[bulkStore] Closed connection.")
        }
    }

    Document load(String id) {
        return load(id, null)
    }

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
        log.debug("loadFromSql $id ($sql)")
        Connection connection = connectionPool.getConnection()
        log.debug("Got connection.")
        PreparedStatement selectstmt
        ResultSet rs
        try {
            selectstmt = connection.prepareStatement(sql)
            log.trace("Prepared statement")
            if (id) {
                selectstmt.setString(1, id)
            }
            if (checksum) {
                selectstmt.setString(2, checksum)
            }
            rs = selectstmt.executeQuery()
            if (rs.next()) {
                def manifest = mapper.readValue(rs.getString("manifest"), Map)
                if (!checksum) {
                    manifest.put(Document.CREATED_KEY, rs.getTimestamp("created").getTime())
                    manifest.put(Document.MODIFIED_KEY, rs.getTimestamp("modified").getTime())
                    manifest.put(Document.DELETED_KEY, rs.getBoolean("deleted"))
                }
                doc = docFactory.createDocument(mapper.readValue(rs.getString("data"), Map), manifest)
            } else if (log.isTraceEnabled()) {
                log.trace("No results returned for get($id)")
            }
        } finally {
            connection.close()
        }
        return doc
    }

    Document loadByAlternateIdentifier(String identifier) {
        String sql = GET_DOCUMENT_BY_ALTERNATE_ID.replace("?", '"' + identifier + '"')
        return loadFromSql(null, null, sql)
    }

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
                def manifest = mapper.readValue(rs.getString("manifest"), Map)
                manifest.put(Document.CREATED_KEY, rs.getTimestamp("created").getTime())
                manifest.put(Document.MODIFIED_KEY, rs.getTimestamp("modified").getTime())
                manifest.put(Document.DELETED_KEY, rs.getBoolean("deleted"))
                def doc = docFactory.createDocument(mapper.readValue(rs.getString("data"), Map), manifest)
                doc.version = v++
                docList << doc
            }
        } finally {
            /*
            rs.close()
            selectstmt.close()
            */
            connection.close()
            log.debug("[loadAllVersions] Closed connection.")
        }
        return docList
    }

    Iterable<Document> loadAll(String dataset) {
        return loadAllDocuments(dataset, false)
    }

    private Iterable<Document> loadAllDocuments(String dataset, boolean withLinks, Date since = null, Date until = null) {
        log.debug("Load all called with dataset: $dataset")
        return new Iterable<Document>() {
            Iterator<Document> iterator() {
                Connection connection = connectionPool.getConnection()
                connection.setAutoCommit(false)
                PreparedStatement loadAllStatement
                long untilTS = until?.getTime() ?: PGStatement.DATE_POSITIVE_INFINITY
                long sinceTS = since?.getTime() ?: 0L

                if (dataset) {
                    if (withLinks) {
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS_WITH_LINKS_BY_DATASET)
                    } else {
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS_BY_DATASET)
                    }
                } else {
                    if (withLinks) {
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS_WITH_LINKS)
                    } else {
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS)
                    }
                }
                loadAllStatement.setFetchSize(100)
                if (dataset) {
                    loadAllStatement.setString(1, dataset)
                    if (withLinks) {
                        loadAllStatement.setString(2, dataset)
                    } else {
                        loadAllStatement.setTimestamp(2, new Timestamp(sinceTS))
                        loadAllStatement.setTimestamp(3, new Timestamp(untilTS))
                    }
                } else {
                    loadAllStatement.setTimestamp(1, new Timestamp(sinceTS))
                    loadAllStatement.setTimestamp(2, new Timestamp(untilTS))
                }
                ResultSet rs = loadAllStatement.executeQuery()

                boolean more = rs.next()
                if (!more) {
                    try {
                        connection.commit()
                        connection.setAutoCommit(true)
                    } finally {
                        connection.close()
                    }
                }

                return new Iterator<Document>() {
                    @Override
                    public Document next() {
                        Document doc
                        if (withLinks) {
                            doc = docFactory.createDocument(rs.getBytes("data"), mapper.readValue(rs.getString("manifest"), Map), mapper.readValue(rs.getString("meta"), Map), rs.getString("parent"))
                        } else {
                            def manifest = mapper.readValue(rs.getString("manifest"), Map)
                            manifest.put(Document.CREATED_KEY, rs.getTimestamp("created").getTime())
                            manifest.put(Document.MODIFIED_KEY, rs.getTimestamp("modified").getTime())
                            manifest.put(Document.DELETED_KEY, rs.getBoolean("deleted"))
                            doc = docFactory.createDocument(mapper.readValue(rs.getString("data"), Map), manifest)
                        }
                        more = rs.next()
                        if (!more) {
                            try {
                                connection.commit()
                                connection.setAutoCommit(true)
                            } finally {
                                connection.close()
                            }
                        }
                        return doc
                    }

                    @Override
                    public boolean hasNext() {
                        return more
                    }
                }
            }
        }
    }

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
                /*
                delstmt.close()
                */
                connection.close()
                log.debug("[remove] Closed connection.")
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

    /*
    public Map getStatus() {
        def status = [:]
        status['mainTable'] = mainTableName
        status['versioning'] = versioning
        if (versioning) {
            status['versionsTableName'] = versionsTableName
        }
        status['contentTypes'] = contentTypes
        status['databaseUrl'] = connectionUrl
        return status
    }
    */
}
