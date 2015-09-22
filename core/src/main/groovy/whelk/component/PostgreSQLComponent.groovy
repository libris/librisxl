package whelk.component

import groovy.util.logging.Slf4j as Log
import org.apache.commons.dbcp2.BasicDataSource
import org.codehaus.jackson.map.ObjectMapper
import org.postgresql.PGStatement
import whelk.Document
import whelk.JsonLd
import whelk.Location

import java.security.MessageDigest
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.Types

@Log
class PostgreSQLComponent implements Storage {

    protected BasicDataSource connectionPool

    public final static mapper = new ObjectMapper()

    private final static LOCATION_PRECURSOR = "/resource"

    protected boolean versioning = true

    // SQL statements
    protected String UPSERT_DOCUMENT, INSERT_DOCUMENT_VERSION, GET_DOCUMENT, GET_DOCUMENT_VERSION, GET_ALL_DOCUMENT_VERSIONS, GET_DOCUMENT_BY_ALTERNATE_ID, LOAD_ALL_DOCUMENTS, LOAD_ALL_DOCUMENTS_WITH_LINKS, LOAD_ALL_DOCUMENTS_WITH_LINKS_BY_DATASET, LOAD_ALL_DOCUMENTS_BY_DATASET, DELETE_DOCUMENT_STATEMENT, STATUS_OF_DOCUMENT

    PostgreSQLComponent(String sqlUrl, String sqlMaintable) {
        URI connURI = new URI(sqlUrl.substring(5)) // Cut the "jdbc:"-part of the sqlUrl.

        String mainTableName = sqlMaintable
        String versionsTableName = mainTableName + "__versions"

        log.info("Connecting to sql database at $sqlUrl")
        connectionPool = new BasicDataSource();

        if (connURI.getUserInfo() != null) {
            String username = connURI.getUserInfo().split(":")[0]
            log.trace("Setting connectionPool username: $username")
            connectionPool.setUsername(username)
            try {
                String password = connURI.getUserInfo().split(":")[1]
                log.trace("Setting connectionPool password: $password")
                connectionPool.setPassword(password)
            } catch (ArrayIndexOutOfBoundsException aioobe) {
                log.debug("No password part found i connect url userinfo.")
            }
        }
        connectionPool.setDriverClassName("org.postgresql.Driver")
        connectionPool.setUrl(sqlUrl.replaceAll(":\\/\\/\\w+:*.*@", ":\\/\\/")) // Remove the password part from the url or it won't be able to connect
        connectionPool.setInitialSize(10)
        connectionPool.setMaxTotal(40)
        connectionPool.setDefaultAutoCommit(true)

        // Setting up sql-statements
        UPSERT_DOCUMENT = "WITH upsert AS (UPDATE $mainTableName SET data = ?, manifest = ?, deleted = ?, modified = ? WHERE id = ? RETURNING *) " +
            "INSERT INTO $mainTableName (id, data, manifest, deleted) SELECT ?,?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)"


        INSERT_DOCUMENT_VERSION = "INSERT INTO $versionsTableName (id, data, manifest, checksum, modified) SELECT ?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM $versionsTableName WHERE id = ? ORDER BY modified DESC LIMIT 1) AS last WHERE last.checksum = ?)"// (SELECT 1 FROM $versionsTableName WHERE id = ? AND checksum = ?)" +

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
        STATUS_OF_DOCUMENT = "SELECT created, modified, deleted FROM $mainTableName WHERE id = ?"
    }



    public Map status(String identifier, Connection connection = null) {
        Map statusMap = [:]
        boolean newConnection = (connection == null)
        try {
            if (newConnection) {
                connection = connectionPool.getConnection()
            }
            PreparedStatement statusStmt = connection.prepareStatement(STATUS_OF_DOCUMENT)
            statusStmt.setString(1, identifier)
            def rs = statusStmt.executeQuery()
            if (rs.next()) {
                statusMap['exists'] = true
                statusMap['created'] = new Date(rs.getTimestamp("created").getTime())
                statusMap['modified'] = new Date(rs.getTimestamp("modified").getTime())
                statusMap['deleted'] = rs.getBoolean("deleted")
            } else {
                log.trace("No results returned for $identifier")
                statusMap['exists'] = false
            }
        } finally {
            if (newConnection) {
                connection.close()
            }
        }
        return statusMap
    }

    @Override
    Document store(Document doc) {
        return store(doc, true)
    }

    Document store(Document doc, boolean flatten) {
        assert doc.dataset
        if (flatten) {
            log.trace("Flattening ${doc.id}")
            doc.data = JsonLd.flatten(doc.data)
        }
        Connection connection = connectionPool.getConnection()
        connection.setAutoCommit(false)
        try {
            calculateChecksum(doc)
            Date now = new Date()
            if (versioning) {
                if (!saveVersion(doc, connection, now)) {
                    return doc// Same document already in storage.
                }
            }
            PreparedStatement insert = connection.prepareStatement(UPSERT_DOCUMENT)
            insert = rigUpsertStatement(insert, doc, now)
            insert.executeUpdate()
            connection.commit()
            def status = status(doc.identifier, connection)
            doc.setCreated(status['created'])
            doc.setModified(status['modified'])
            log.debug("Saved document ${doc.identifier} with timestamps ${doc.created} / ${doc.modified}")
            return doc
        } catch (Exception e) {
            log.error("Failed to save document: ${e.message}")
            connection.rollback()
            throw e
        } finally {
            connection.close()
            log.debug("[store] Closed connection.")
        }
        return null
    }

    private PreparedStatement rigUpsertStatement(PreparedStatement insert, Document doc, Date modTime) {
        insert.setObject(1, doc.dataAsString, java.sql.Types.OTHER)
        insert.setObject(2, doc.manifestAsJson, java.sql.Types.OTHER)
        insert.setBoolean(3, doc.isDeleted())
        insert.setTimestamp(4, new Timestamp(modTime.getTime()))
        insert.setString(5, doc.identifier)
        insert.setString(6, doc.identifier)
        insert.setObject(7, doc.dataAsString, java.sql.Types.OTHER)
        insert.setObject(8, doc.manifestAsJson, java.sql.Types.OTHER)
        insert.setBoolean(9, doc.isDeleted())

        return insert
    }

    boolean saveVersion(Document doc, Connection connection, Date modTime) {
        PreparedStatement insvers = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
        try {
            log.debug("Trying to save a version of ${doc.identifier} with checksum ${doc.checksum}. Modified: $modTime")
            insvers = rigVersionStatement(insvers, doc, modTime)
            int updated =  insvers.executeUpdate()
            log.debug("${updated > 0 ? 'New version saved.' : 'Already had same version'}")
            return (updated > 0)
        } catch (Exception e) {
            log.error("Failed to save document version: ${e.message}")
            throw e
        }
    }

    private PreparedStatement rigVersionStatement(PreparedStatement insvers, Document doc, Date modTime) {
        insvers.setString(1, doc.identifier)
        insvers.setObject(2, doc.dataAsString, Types.OTHER)
        insvers.setObject(3, doc.manifestAsJson, Types.OTHER)
        insvers.setString(4, doc.checksum)
        insvers.setTimestamp(5, new Timestamp(modTime.getTime()))
        insvers.setString(6, doc.identifier)
        insvers.setString(7, doc.checksum)
        return insvers
    }

    @Override
    void bulkStore(final List docs) {
        if (!docs || docs.isEmpty()) {
            return
        }
        log.debug("Bulk storing ${docs.size()} documents.")
        Connection connection = connectionPool.getConnection()
        PreparedStatement batch = connection.prepareStatement(UPSERT_DOCUMENT)
        PreparedStatement ver_batch = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
        try {
            docs.each { doc ->
                Date now = new Date()
                log.trace("Flattening ${doc.id}")
                doc.data = JsonLd.flatten(doc.data)
                calculateChecksum(doc)
                if (versioning) {
                    ver_batch = rigVersionStatement(ver_batch, doc, now)
                    ver_batch.addBatch()
                }

                batch = rigUpsertStatement(batch, doc, now)

                batch.addBatch()
            }
            ver_batch.executeBatch()
            batch.executeBatch()
            log.debug("Stored ${docs.size()} documents with dataset ${docs.first().dataset} (versioning: ${versioning})")
        } catch (Exception e) {
            log.error("Failed to save batch: ${e.message}")
            throw e
        } finally {
            connection.close()
            log.debug("[bulkStore] Closed connection.")
        }
    }

    String calculateChecksum(Document doc) {
        log.trace("Calculating checksum with manifest: ${doc.manifest}")
        MessageDigest m = MessageDigest.getInstance("MD5")
        m.reset()
        byte[] databytes = mapper.writeValueAsBytes(doc.data)
        // Remove created and modified from manifest in preparation for checksum calculation
        Date created = doc.manifest.remove(Document.CREATED_KEY)
        Date modified = doc.manifest.remove(Document.MODIFIED_KEY)
        doc.manifest.remove(Document.CHECKUM_KEY)
        byte[] manifestbytes= mapper.writeValueAsBytes(doc.manifest)
        byte[] checksumbytes = new byte[databytes.length + manifestbytes.length];
        System.arraycopy(databytes, 0, checksumbytes, 0, databytes.length);
        System.arraycopy(manifestbytes, 0, checksumbytes, databytes.length, manifestbytes.length);
        m.update(checksumbytes)
        byte[] digest = m.digest()
        BigInteger bigInt = new BigInteger(1,digest)
        String hashtext = bigInt.toString(16)
        log.debug("calculated checksum: $hashtext")
        doc.manifest[Document.CHECKUM_KEY] = hashtext
        // Reinsert created and modified
        doc.setCreated(created)
        doc.setModified(modified)
    }

    // TODO: Update to real locate
    @Override
    Location locate(String uri) {
        log.debug("Locating $uri")
        if (uri) {
            def doc = load(uri)
            if (doc) {
                return new Location(doc)
            }

            String identifier = new URI(uri).getPath().toString()
            log.trace("Nothing found at identifier $identifier")

            if (identifier.startsWith(LOCATION_PRECURSOR)) {
                identifier = identifier.substring(LOCATION_PRECURSOR.length())
                log.trace("New place to look: $identifier")
            }
            log.debug("Checking if new identifier (${identifier}) has something to get")
            if (load(identifier, null, [], false)) {
                return new Location().withURI(new URI(identifier)).withResponseCode(303)
            }

            log.debug("Check alternate identifiers.")
            doc = loadByAlternateIdentifier(uri)
            if (doc) {
                return new Location().withURI(new URI(doc.identifier)).withResponseCode(301)
            }
        }

        return null
    }

    @Override
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
            log.trace("Executing query")
            rs = selectstmt.executeQuery()
            log.trace("Executed query")
            if (rs.next()) {
                log.trace("next")
                def manifest = mapper.readValue(rs.getString("manifest"), Map)
                log.trace("About to create document")
                doc = new Document(rs.getString("id"), mapper.readValue(rs.getString("data"), Map), manifest)
                if (!checksum) {
                    doc.setCreated(rs.getTimestamp("created").getTime())
                    doc.setModified(rs.getTimestamp("modified").getTime())
                    doc.deleted = rs.getBoolean("deleted")
                }
                log.trace("Created document with id ${doc.id}")
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
                def doc = assembleDocument(rs)
                doc.version = v++
                docList << doc
            }
        } finally {
            connection.close()
            log.debug("[loadAllVersions] Closed connection.")
        }
        return docList
    }

    Iterable<Document> loadAll(String dataset) {
        return loadAllDocuments(dataset, false)
    }

    private Document assembleDocument(ResultSet rs) {
        Document doc = new Document(mapper.readValue(rs.getString("data"), Map), mapper.readValue(rs.getString("manifest"), Map))
        doc.setCreated(rs.getTimestamp("created").getTime())
        doc.setModified(rs.getTimestamp("modified").getTime())
        doc.setDeleted(rs.getBoolean("deleted"))
        return doc
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
                            doc = assembleDocument(rs)
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
            store(createTombstone(identifier, dataset), false)
        } else {
            Connection connection = connectionPool.getConnection()
            PreparedStatement delstmt = connection.prepareStatement(DELETE_DOCUMENT_STATEMENT)
            try {
                delstmt.setString(1, identifier)
                delstmt.executeUpdate()
            } finally {
                connection.close()
                log.debug("[remove] Closed connection.")
            }
        }
    }


    protected Document createTombstone(id, dataset) {
        def tombstone = new Document(id, ["@type":"Tombstone"]).withContentType("application/ld+json").withDataset(dataset)
        tombstone.setDeleted(true)
        return tombstone
    }

}
