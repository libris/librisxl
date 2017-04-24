package whelk.component

import groovy.util.logging.Slf4j as Log
import org.apache.commons.dbcp2.BasicDataSource
import org.codehaus.jackson.map.ObjectMapper
import org.postgresql.PGStatement
import org.postgresql.util.PSQLException
import whelk.Document
import whelk.JsonLd
import whelk.Location
import whelk.exception.StorageCreateFailedException

import java.sql.*
import java.util.Date
import java.util.regex.Matcher
import java.util.regex.Pattern

@Log
class PostgreSQLComponent {

    /**
     * Interface for performing atomic document updates
     */
    public interface UpdateAgent {
        public void update(Document doc)
    }

    private BasicDataSource connectionPool
    static String driverClass = "org.postgresql.Driver"

    public final static mapper = new ObjectMapper()

    boolean versioning = true

    // SQL statements
    protected String UPSERT_DOCUMENT, UPDATE_DOCUMENT, INSERT_DOCUMENT,
                     INSERT_DOCUMENT_VERSION, GET_DOCUMENT,
                     GET_DOCUMENT_VERSION, GET_ALL_DOCUMENT_VERSIONS,
                     GET_DOCUMENT_BY_SAMEAS_ID, LOAD_ALL_DOCUMENTS,
                     LOAD_ALL_DOCUMENTS_BY_COLLECTION,
                     DELETE_DOCUMENT_STATEMENT, STATUS_OF_DOCUMENT,
                     LOAD_ID_FROM_ALTERNATE, INSERT_IDENTIFIERS,
                     LOAD_RECORD_IDENTIFIERS, LOAD_THING_IDENTIFIERS, DELETE_IDENTIFIERS, LOAD_COLLECTIONS,
                     GET_DOCUMENT_FOR_UPDATE, GET_CONTEXT, GET_RECORD_ID_BY_THING_ID, GET_DEPENDENCIES, GET_DEPENDERS
    protected String LOAD_SETTINGS, SAVE_SETTINGS
    protected String DELETE_DEPENDENCIES, INSERT_DEPENDENCIES
    protected String QUERY_LD_API
    protected String FIND_BY, COUNT_BY
    protected String GET_SYSTEMID_BY_IRI

    // Deprecated
    protected String LOAD_ALL_DOCUMENTS_WITH_LINKS, LOAD_ALL_DOCUMENTS_WITH_LINKS_BY_COLLECTION

    // Query defaults
    static final int DEFAULT_PAGE_SIZE = 50

    // Query idiomatic data
    static final Map<StorageType, String> SQL_PREFIXES = [
            (StorageType.JSONLD)                       : "data->'@graph'",
            (StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS): "data->'descriptions'",
            (StorageType.MARC21_JSON)                  : "data->'fields'"
    ]

    String mainTableName

    // for testing
    PostgreSQLComponent() {}

    PostgreSQLComponent(String sqlUrl, String sqlMaintable) {
        mainTableName = sqlMaintable
        String idTableName = mainTableName + "__identifiers"
        String versionsTableName = mainTableName + "__versions"
        String settingsTableName = mainTableName + "__settings"
        String dependenciesTableName = mainTableName + "__dependencies"


        connectionPool = new BasicDataSource()

        if (sqlUrl) {
            URI connURI = new URI(sqlUrl.substring(5)) // Cut the "jdbc:"-part of the sqlUrl.

            log.info("Connecting to sql database at ${sqlUrl}, using driver $driverClass")
            if (connURI.getUserInfo() != null) {
                String username = connURI.getUserInfo().split(":")[0]
                log.trace("Setting connectionPool username: $username")
                connectionPool.setUsername(username)
                try {
                    String password = connURI.getUserInfo().split(":")[1]
                    log.trace("Setting connectionPool password: $password")
                    connectionPool.setPassword(password)
                } catch (ArrayIndexOutOfBoundsException aioobe) {
                    log.debug("No password part found in connect url userinfo.")
                }
            }
            connectionPool.setDriverClassName(driverClass)
            connectionPool.setUrl(sqlUrl.replaceAll(":\\/\\/\\w+:*.*@", ":\\/\\/"))
            // Remove the password part from the url or it won't be able to connect
            connectionPool.setInitialSize(10)
            connectionPool.setMaxTotal(40)
            connectionPool.setDefaultAutoCommit(true)
        }

        // Setting up sql-statements
        UPSERT_DOCUMENT = "WITH upsert AS (UPDATE $mainTableName SET data = ?, collection = ?, changedIn = ?, changedBy = ?, checksum = ?, deleted = ?, modified = ? WHERE id = ? " +
                "RETURNING *) " +
                "INSERT INTO $mainTableName (id, data, collection, changedIn, changedBy, checksum, deleted) SELECT ?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)"
        UPDATE_DOCUMENT = "UPDATE $mainTableName SET data = ?, collection = ?, changedIn = ?, changedBy = ?, checksum = ?, deleted = ?, modified = ? WHERE id = ?"
        INSERT_DOCUMENT = "INSERT INTO $mainTableName (id,data,collection,changedIn,changedBy,checksum,deleted) VALUES (?,?,?,?,?,?,?)"
        DELETE_IDENTIFIERS = "DELETE FROM $idTableName WHERE id = ?"
        INSERT_IDENTIFIERS = "INSERT INTO $idTableName (id, iri, graphIndex, mainId) VALUES (?,?,?,?)"

        DELETE_DEPENDENCIES = "DELETE FROM $dependenciesTableName WHERE id = ?"
        INSERT_DEPENDENCIES = "INSERT INTO $dependenciesTableName (id, dependsOnId) VALUES (?,?)"

        INSERT_DOCUMENT_VERSION = "INSERT INTO $versionsTableName (id, data, collection, changedIn, changedBy, checksum, modified, deleted) SELECT ?,?,?,?,?,?,?,? " +
                "WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM $versionsTableName WHERE id = ? " +
                "ORDER BY modified DESC LIMIT 1) AS last WHERE last.checksum = ?)"

        GET_DOCUMENT = "SELECT id,data,created,modified,deleted FROM $mainTableName WHERE id= ?"
        GET_DOCUMENT_FOR_UPDATE = "SELECT id,data,created,modified,deleted FROM $mainTableName WHERE id= ? FOR UPDATE"
        GET_DOCUMENT_VERSION = "SELECT id,data FROM $versionsTableName WHERE id = ? AND checksum = ?"
        GET_ALL_DOCUMENT_VERSIONS = "SELECT id,data,data->>'created' AS created,modified" +
                "FROM $versionsTableName WHERE id = ? ORDER BY modified"
        GET_DOCUMENT_BY_SAMEAS_ID = "SELECT id,data,created,modified,deleted FROM $mainTableName " +
                "WHERE data->'@graph' @> ?"
        GET_RECORD_ID_BY_THING_ID = "SELECT id FROM $idTableName WHERE iri = ? AND graphIndex = 1"
        LOAD_ALL_DOCUMENTS = "SELECT id,data,created,modified,deleted FROM $mainTableName WHERE modified >= ? AND modified <= ?"
        LOAD_COLLECTIONS = "SELECT DISTINCT collection FROM $mainTableName"
        LOAD_ALL_DOCUMENTS_BY_COLLECTION = "SELECT id,data,created,modified,deleted FROM $mainTableName " +
                "WHERE modified >= ? AND modified <= ? AND collection = ?"
        LOAD_RECORD_IDENTIFIERS = "SELECT iri from $idTableName WHERE id = ? AND graphIndex = 0"
        LOAD_THING_IDENTIFIERS = "SELECT iri from $idTableName WHERE id = ? AND graphIndex = 1"

        DELETE_DOCUMENT_STATEMENT = "DELETE FROM $mainTableName WHERE id = ?"
        STATUS_OF_DOCUMENT = "SELECT t1.id AS id, created, modified, deleted FROM $mainTableName t1 " +
                "JOIN $idTableName t2 ON t1.id = t2.id WHERE t2.iri = ?"
        GET_CONTEXT = "SELECT data FROM $mainTableName WHERE id IN (SELECT id FROM $idTableName WHERE iri = 'https://id.kb.se/vocab/context')"
        GET_DEPENDENCIES = "SELECT id FROM $dependenciesTableName WHERE dependsOnId = ?"
        GET_DEPENDERS = "SELECT dependsOnId FROM $dependenciesTableName WHERE id = ?"

        // Queries
        QUERY_LD_API = "SELECT id,data,created,modified,deleted FROM $mainTableName WHERE deleted IS NOT TRUE AND "

        // SQL for settings management
        LOAD_SETTINGS = "SELECT key,settings FROM $settingsTableName where key = ?"
        SAVE_SETTINGS = "WITH upsertsettings AS (UPDATE $settingsTableName SET settings = ? WHERE key = ? RETURNING *) " +
                "INSERT INTO $settingsTableName (key, settings) SELECT ?,? WHERE NOT EXISTS (SELECT * FROM upsertsettings)"

        FIND_BY = "SELECT id, data, created, modified, deleted " +
                  "FROM $mainTableName " +
                  "WHERE data->'@graph' @> ? " +
                  "OR data->'@graph' @> ? " +
                  "LIMIT ? OFFSET ?"

        COUNT_BY = "SELECT count(*) " +
                   "FROM $mainTableName " +
                   "WHERE data->'@graph' @> ? " +
                   "OR data->'@graph' @> ?"

        GET_SYSTEMID_BY_IRI = "SELECT id FROM $idTableName WHERE iri = ?"
     }


    public Map status(URI uri, Connection connection = null) {
        Map statusMap = [:]
        boolean newConnection = (connection == null)
        try {
            if (newConnection) {
                connection = getConnection()
            }
            PreparedStatement statusStmt = connection.prepareStatement(STATUS_OF_DOCUMENT)
            statusStmt.setString(1, uri.toString())
            def rs = statusStmt.executeQuery()
            if (rs.next()) {
                statusMap['id'] = rs.getString("id")
                statusMap['uri'] = Document.BASE_URI.resolve(rs.getString("id"))
                statusMap['exists'] = true
                statusMap['created'] = new Date(rs.getTimestamp("created").getTime())
                statusMap['modified'] = new Date(rs.getTimestamp("modified").getTime())
                statusMap['deleted'] = rs.getBoolean("deleted")
                log.trace("StatusMap: $statusMap")
            } else {
                log.debug("No results returned for $uri")
                statusMap['exists'] = false
            }
        } finally {
            if (newConnection) {
                connection.close()
            }
        }
        log.debug("Loaded status for ${uri}: $statusMap")
        return statusMap
    }

    public List<String> loadCollections() {
        Connection connection = getConnection()
        PreparedStatement collectionStatement = connection.prepareStatement(LOAD_COLLECTIONS)
        ResultSet collectionResults = collectionStatement.executeQuery()
        List<String> collections = []
        while (collectionResults.next()) {
            String c = collectionResults.getString("collection")
            if (c) {
                collections.add(c)
            }
        }
        return collections
    }

    boolean store(Document doc, boolean upsert, String changedIn, String changedBy, String collection, boolean deleted) {
        store(doc, upsert, false, changedIn, changedBy, collection, deleted)
    }

    boolean store(Document doc, boolean upsert, boolean minorUpdate, String changedIn, String changedBy, String collection, boolean deleted) {
        log.debug("Saving ${doc.getShortId()}, ${changedIn}, ${changedBy}, ${collection}")
        Connection connection = getConnection()
        connection.setAutoCommit(false)
        try {
            Date now = new Date()
            PreparedStatement insert = connection.prepareStatement((upsert ? UPSERT_DOCUMENT : INSERT_DOCUMENT))

            if (minorUpdate) {
                now = status(doc.getURI(), connection)['modified']
            }
            if (upsert) {
                if (!saveVersion(doc, connection, now, changedIn, changedBy, collection, deleted)) {
                    return // Same document already in storage.
                }
                insert = rigUpsertStatement(insert, doc, now, changedIn, changedBy, collection, deleted)
                insert.executeUpdate()
            } else {
                insert = rigInsertStatement(insert, doc, changedIn, changedBy, collection, deleted)
                insert.executeUpdate()
                saveVersion(doc, connection, now, changedIn, changedBy, collection, deleted)
            }
            saveIdentifiers(doc, connection)
            saveDependencies(doc, connection)
            connection.commit()
            def status = status(doc.getURI(), connection)
            if (status.exists) {
                doc.setCreated(status['created'])
                doc.setModified(status['modified'])
            }


            log.debug("Saved document ${doc.getShortId()} with timestamps ${doc.created} / ${doc.modified}")
            return true
        } catch (PSQLException psqle) {
            log.error("SQL failed: ${psqle.message}")
            connection.rollback()
            if (psqle.serverErrorMessage.message.startsWith("duplicate key value violates unique constraint")) {
                Pattern messageDetailPattern = Pattern.compile(".+\\((.+)\\)\\=\\((.+)\\).+", Pattern.DOTALL)
                Matcher m = messageDetailPattern.matcher(psqle.message)
                String duplicateId = doc.getShortId()
                if (m.matches()) {
                    log.debug("Problem is that ${m.group(1)} already contains value ${m.group(2)}")
                    duplicateId = m.group(2)
                }
                throw new StorageCreateFailedException(duplicateId)
            } else {
                throw psqle
            }
        } catch (Exception e) {
            log.error("Failed to save document: ${e.message}. Rolling back.")
            connection.rollback()
            throw e
        } finally {
            connection.close()
            log.debug("[store] Closed connection.")
        }
        return false
    }

    String getContext() {
        Connection connection
        PreparedStatement selectStatement
        ResultSet resultSet

        try {
            connection = getConnection()
            selectStatement = connection.prepareStatement(GET_CONTEXT)
            resultSet = selectStatement.executeQuery()

            if (resultSet.next()) {
                return resultSet.getString(1)
            }
            return null
        }
        finally {
            try {
                resultSet.close()
            } catch (Exception e) { /* ignore */
            }
            try {
                selectStatement.close()
            } catch (Exception e) { /* ignore */
            }
            try {
                connection.close()
            } catch (Exception e) { /* ignore */
            }
        }
    }

    /**
     * Take great care that the actions taken by your UpdateAgent are quick and not reliant on IO. The row will be
     * LOCKED while the update is in progress.
     */
    public Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, String collection, boolean deleted, UpdateAgent updateAgent) {
        log.debug("Saving (atomic update) ${id}")

        // Resources to be closed
        Connection connection = getConnection()
        PreparedStatement selectStatement
        PreparedStatement updateStatement
        ResultSet resultSet

        Document doc = null

        connection.setAutoCommit(false)
        try {
            selectStatement = connection.prepareStatement(GET_DOCUMENT_FOR_UPDATE)
            selectStatement.setString(1, id)
            resultSet = selectStatement.executeQuery()
            if (!resultSet.next())
                throw new SQLException("There is no document with the id: " + id)

            doc = assembleDocument(resultSet)

            // Performs the callers updates on the document
            updateAgent.update(doc)

            Date modTime = new Date()
            if (minorUpdate) {
                modTime = new Date(resultSet.getTimestamp("modified").getTime())
            }
            updateStatement = connection.prepareStatement(UPDATE_DOCUMENT)
            rigUpdateStatement(updateStatement, doc, modTime, changedIn, changedBy, collection, deleted)
            updateStatement.execute()

            // The versions and identifiers tables are NOT under lock. Synchronization is only maintained on the main table.
            saveVersion(doc, connection, modTime, changedIn, changedBy, collection, deleted)
            saveIdentifiers(doc, connection)
            saveDependencies(doc, connection)
            connection.commit()
            log.debug("Saved document ${doc.getShortId()} with timestamps ${doc.created} / ${doc.modified}")
        } catch (PSQLException psqle) {
            log.error("SQL failed: ${psqle.message}")
            connection.rollback()
            if (psqle.serverErrorMessage.message.startsWith("duplicate key value violates unique constraint")) {
                throw new StorageCreateFailedException()
            } else {
                throw psqle
            }
        } catch (Exception e) {
            log.error("Failed to save document: ${e.message}. Rolling back.")
            connection.rollback()
            throw e
        } finally {
            try {
                resultSet.close()
            } catch (Exception e) {
            }
            try {
                selectStatement.close()
            } catch (Exception e) {
            }
            try {
                updateStatement.close()
            } catch (Exception e) {
            }
            try {
                connection.close()
            } catch (Exception e) {
            }
            log.debug("[store] Closed connection.")
        }

        return doc
    }

    /**
     * Given a document, look up all it's dependencies (links/references) and return a list of those referencess that
     * have Libris system IDs (fnrgls). You were probably looking for getDependencies() which is much more efficient
     * for a document that is already saved in lddb!
     */
    public List<String> calculateDependenciesSystemIDs(Document doc) {
        Connection connection = getConnection()
        try {
            return _calculateDependenciesSystemIDs(doc, connection)
        } finally {
            connection.close()
        }
    }

    private List<String> _calculateDependenciesSystemIDs(Document doc, Connection connection) {
        List<String> dependencies = []
        for (String iri : doc.getExternalRefs()) {
            if (!iri.startsWith("http"))
                continue
            PreparedStatement getSystemId
            try {
                getSystemId = connection.prepareStatement(GET_SYSTEMID_BY_IRI)
                getSystemId.setString(1, iri)
                ResultSet rs
                try {
                    rs = getSystemId.executeQuery()
                    if (rs.next())
                        dependencies.add(rs.getString(1))
                } finally {
                    if (rs != null) rs.close()
                }
            } finally {
                if (getSystemId != null) getSystemId.close()
            }
        }
        return dependencies
    }

    private void saveDependencies(Document doc, Connection connection) {
        List dependencies = _calculateDependenciesSystemIDs(doc, connection)

        // Clear out old dependencies
        PreparedStatement removeDependencies = connection.prepareStatement(DELETE_DEPENDENCIES)
        try {
            removeDependencies.setString(1, doc.getShortId())
            int numRemoved = removeDependencies.executeUpdate()
            log.debug("Removed $numRemoved dependencies for id ${doc.getShortId()}")
        } finally { removeDependencies.close() }

        // Insert the dependency list
        PreparedStatement insertDependencies = connection.prepareStatement(INSERT_DEPENDENCIES)
        for (String dependsOnId : dependencies) {
            insertDependencies.setString(1, doc.getShortId())
            insertDependencies.setString(2, dependsOnId)
            insertDependencies.addBatch()
        }
        try {
            insertDependencies.executeBatch()
        } catch(BatchUpdateException bue) {
            log.error("Failed saving dependencies for ${doc.getShortId()}")
            throw bue.getNextException()
        } finally { insertDependencies.close() }
    }

    private void saveIdentifiers(Document doc, Connection connection) {
        PreparedStatement removeIdentifiers = connection.prepareStatement(DELETE_IDENTIFIERS)
        removeIdentifiers.setString(1, doc.getShortId())
        int numRemoved = removeIdentifiers.executeUpdate()
        log.debug("Removed $numRemoved identifiers for id ${doc.getShortId()}")
        PreparedStatement altIdInsert = connection.prepareStatement(INSERT_IDENTIFIERS)
        for (altId in doc.getRecordIdentifiers()) {
            altIdInsert.setString(1, doc.getShortId())
            altIdInsert.setString(2, altId)
            altIdInsert.setInt(3, 0) // record id -> graphIndex = 0
            if (altId == doc.getCompleteId())
                altIdInsert.setBoolean(4, true) // Main ID
            else
                altIdInsert.setBoolean(4, false) // alternative ID, not the main ID
            altIdInsert.addBatch()
        }
        for (altThingId in doc.getThingIdentifiers()) {
            altIdInsert.setString(1, doc.getShortId())
            altIdInsert.setString(2, altThingId)
            altIdInsert.setInt(3, 1) // thing id -> graphIndex = 1
            if (altThingId == doc.getThingIdentifiers()[0])
                altIdInsert.setBoolean(4, true) // Main ID
            else
                altIdInsert.setBoolean(4, false) // alternative ID
            altIdInsert.addBatch()
        }
        try {
            altIdInsert.executeBatch()
        } catch (BatchUpdateException bue) {
            log.error("Failed saving identifiers for ${doc.getShortId()}")
            throw bue.getNextException()
        }
    }

    private PreparedStatement rigInsertStatement(PreparedStatement insert, Document doc, String changedIn, String changedBy, String collection, boolean deleted) {
        insert.setString(1, doc.getShortId())
        insert.setObject(2, doc.dataAsString, java.sql.Types.OTHER)
        insert.setString(3, collection)
        insert.setString(4, changedIn)
        insert.setString(5, changedBy)
        insert.setString(6, doc.getChecksum())
        insert.setBoolean(7, deleted)
        return insert
    }

    private void rigUpdateStatement(PreparedStatement update, Document doc, Date modTime, String changedIn, String changedBy, String collection, boolean deleted) {
        update.setObject(1, doc.dataAsString, java.sql.Types.OTHER)
        update.setString(2, collection)
        update.setString(3, changedIn)
        update.setString(4, changedBy)
        update.setString(5, doc.getChecksum())
        update.setBoolean(6, deleted)
        update.setTimestamp(7, new Timestamp(modTime.getTime()))
        update.setObject(8, doc.getShortId(), java.sql.Types.OTHER)
    }

    private PreparedStatement rigUpsertStatement(PreparedStatement insert, Document doc, Date modTime, String changedIn, String changedBy, String collection, boolean deleted) {

        insert.setObject(1, doc.dataAsString, java.sql.Types.OTHER)

        insert.setString(2, collection)
        insert.setString(3, changedIn)
        insert.setString(4, changedBy)
        insert.setString(5, doc.getChecksum())

        insert.setBoolean(6, deleted)
        insert.setTimestamp(7, new Timestamp(modTime.getTime()))
        insert.setString(8, doc.getShortId())
        insert.setString(9, doc.getShortId())
        insert.setObject(10, doc.dataAsString, java.sql.Types.OTHER)

        insert.setString(11, collection)
        insert.setString(12, changedIn)
        insert.setString(13, changedBy)
        insert.setString(14, doc.getChecksum())

        insert.setBoolean(15, deleted)

        return insert
    }

    boolean saveVersion(Document doc, Connection connection, Date modTime, String changedIn, String changedBy, String collection, boolean deleted) {
        if (versioning) {
            PreparedStatement insvers = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
            try {
                log.debug("Trying to save a version of ${doc.getShortId() ?: ""} with checksum ${doc.getChecksum()}. Modified: $modTime")
                insvers = rigVersionStatement(insvers, doc, modTime, changedIn, changedBy, collection, deleted)
                int updated = insvers.executeUpdate()
                log.debug("${updated > 0 ? 'New version saved.' : 'Already had same version'}")
                return (updated > 0)
            } catch (Exception e) {
                log.error("Failed to save document version: ${e.message}")
                throw e
            }
        } else {
            return false
        }
    }

    private PreparedStatement rigVersionStatement(PreparedStatement insvers, Document doc, Date modTime, String changedIn, String changedBy, String collection, deleted) {
        insvers.setString(1, doc.getShortId())
        insvers.setObject(2, doc.dataAsString, Types.OTHER)
        insvers.setString(3, collection)
        insvers.setString(4, changedIn)
        insvers.setString(5, changedBy)
        insvers.setString(6, doc.getChecksum())
        insvers.setTimestamp(7, new Timestamp(modTime.getTime()))
        insvers.setBoolean(8, deleted)
        insvers.setString(9, doc.getShortId())
        insvers.setString(10, doc.getChecksum())
        return insvers
    }

    boolean bulkStore(
            final List<Document> docs, boolean upsert, String changedIn, String changedBy, String collection) {
        if (!docs || docs.isEmpty()) {
            return true
        }
        log.trace("Bulk storing ${docs.size()} documents.")
        Connection connection = getConnection()
        connection.setAutoCommit(false)
        PreparedStatement batch = connection.prepareStatement((upsert ? UPSERT_DOCUMENT : INSERT_DOCUMENT))
        PreparedStatement ver_batch = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
        try {
            docs.each { doc ->
                Date now = new Date()
                if (versioning) {
                    ver_batch = rigVersionStatement(ver_batch, doc, now, changedIn, changedBy, collection, false)
                    ver_batch.addBatch()
                }
                if (upsert) {
                    batch = rigUpsertStatement(batch, doc, now, changedIn, changedBy, collection, false)
                } else {
                    batch = rigInsertStatement(batch, doc, changedIn, changedBy, collection, false)
                }
                batch.addBatch()
                saveIdentifiers(doc, connection)
                saveDependencies(doc, connection)
            }
            batch.executeBatch()
            ver_batch.executeBatch()
            connection.commit()
            log.debug("Stored ${docs.size()} documents in collection ${collection} (versioning: ${versioning})")
            return true
        } catch (Exception e) {
            log.error("Failed to save batch: ${e.message}. Rolling back..", e)
            if (e instanceof SQLException) {
                log.error("Note: next exception was: ${e.nextException.message}.", e.nextException)
            }
            connection.rollback()
        } finally {
            connection.close()
            log.trace("[bulkStore] Closed connection.")
        }
        return false
    }

    Map<String, Object> query(Map queryParameters, String collection, StorageType storageType) {
        log.debug("Performing query with type $storageType : $queryParameters")
        long startTime = System.currentTimeMillis()
        Connection connection = getConnection()
        // Extract LDAPI parameters
        String pageSize = queryParameters.remove("_pageSize")?.first() ?: "" + DEFAULT_PAGE_SIZE
        String page = queryParameters.remove("_page")?.first() ?: "1"
        String sort = queryParameters.remove("_sort")?.first()
        queryParameters.remove("_where") // Not supported
        queryParameters.remove("_orderBy") // Not supported
        queryParameters.remove("_select") // Not supported

        def (whereClause, values) = buildQueryString(queryParameters, collection, storageType)

        int limit = pageSize as int
        int offset = (Integer.parseInt(page) - 1) * limit

        StringBuilder finalQuery = new StringBuilder("${values ? QUERY_LD_API + whereClause : (collection ? LOAD_ALL_DOCUMENTS_BY_COLLECTION : LOAD_ALL_DOCUMENTS) + " AND deleted IS NOT true"} OFFSET $offset LIMIT $limit")

        if (sort) {
            finalQuery.append(" ORDER BY ${translateSort(sort, storageType)}")
        }
        log.debug("QUERY: ${finalQuery.toString()}")
        log.debug("QUERY VALUES: $values")
        PreparedStatement query = connection.prepareStatement(finalQuery.toString())
        int i = 1
        for (value in values) {
            query.setObject(i++, value, java.sql.Types.OTHER)
        }
        if (!values) {
            query.setTimestamp(1, new Timestamp(0L))
            query.setTimestamp(2, new Timestamp(PGStatement.DATE_POSITIVE_INFINITY))
            if (collection) {
                query.setString(3, collection)
            }
        }
        try {
            ResultSet rs = query.executeQuery()
            Map results = new HashMap<String, Object>()
            List items = []
            while (rs.next()) {
                Map data = mapper.readValue(rs.getString("data"), Map)
                Document doc = new Document(data)
                doc.setId(rs.getString("id"))
                doc.setCreated(rs.getTimestamp("created"))
                doc.setModified(rs.getTimestamp("modified"))
                log.trace("Created document with id ${doc.getShortId()}")
                items.add(doc.data)
            }
            results.put("startIndex", offset)
            results.put("itemsPerPage", (limit > items.size() ? items.size() : limit))
            results.put("duration", "PT" + (System.currentTimeMillis() - startTime) / 1000 + "S")
            results.put("items", items)
            return results
        } finally {
            connection.close()
        }
    }

    def buildQueryString(Map queryParameters, String collection, StorageType storageType) {
        boolean firstKey = true
        List values = []

        StringBuilder whereClause = new StringBuilder("(")

        if (collection) {
            whereClause.append("collection = ?")
            values.add(collection)
            firstKey = false
        }

        for (entry in queryParameters) {
            if (!firstKey) {
                whereClause.append(" AND ")
            }
            String key = entry.key
            boolean firstValue = true
            whereClause.append("(")
            for (value in entry.value) {
                if (!firstValue) {
                    whereClause.append(" OR ")
                }
                def (sqlKey, sqlValue) = translateToSql(key, value, storageType)
                whereClause.append(sqlKey)
                values.add(sqlValue)
                firstValue = false
            }
            whereClause.append(")")
            firstKey = false
        }
        whereClause.append(")")
        return [whereClause.toString(), values]
    }

    protected String translateSort(String keys, StorageType storageType) {
        StringBuilder jsonbPath = new StringBuilder()
        for (key in keys.split(",")) {
            if (jsonbPath.length() > 0) {
                jsonbPath.append(", ")
            }
            String direction = "ASC"
            int elementIndex = 0
            for (element in key.split("\\.")) {
                if (elementIndex == 0) {
                    jsonbPath.append(SQL_PREFIXES.get(storageType, "") + "->")
                } else {
                    jsonbPath.append("->")
                }
                if (storageType == StorageType.MARC21_JSON && elementIndex == 1) {
                    jsonbPath.append("'subfields'->")
                }

                if (element.charAt(0) == '-') {
                    direction = "DESC"
                    element = element.substring(1)
                }
                jsonbPath.append("'${element}'")
                elementIndex++
            }
            jsonbPath.append(" " + direction)
        }
        return jsonbPath.toString()

    }

    // TODO: Adapt to real flat JSON
    protected translateToSql(String key, String value, StorageType storageType) {
        def keyElements = key.split("\\.")
        StringBuilder jsonbPath = new StringBuilder(SQL_PREFIXES.get(storageType, ""))
        if (storageType == StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS) {
            if (keyElements[0] == "entry") {
                jsonbPath.append("->'entry'")
                value = mapper.writeValueAsString([(keyElements.last()): value])
            } else {
                // If no elements in key, assume "items"
                jsonbPath.append("->'items'")
                Map jsonbQueryStructure = [:]
                Map nextMap = null
                for (int i = (keyElements[0] == "items" ? 1 : 0); i < keyElements.length - 1; i++) {
                    nextMap = [:]
                    jsonbQueryStructure.put(keyElements[i], nextMap)
                }
                if (nextMap == null) {
                    nextMap = jsonbQueryStructure
                }
                nextMap.put(keyElements.last(), value)
                value = mapper.writeValueAsString([jsonbQueryStructure])
            }
        }
        if (storageType == StorageType.MARC21_JSON) {
            if (keyElements.length == 1) {
                // Probably search in control field
                value = mapper.writeValueAsString([[(keyElements[0]): value]])
            } else {
                value = mapper.writeValueAsString([[(keyElements[0]): ["subfields": [[(keyElements[1]): value]]]]])

            }
        }

        jsonbPath.append(" @> ?")

        return [jsonbPath.toString(), value]
    }

    // TODO: Update to real locate
    Location locate(String identifier, boolean loadDoc) {
        log.debug("Locating $identifier")
        if (identifier) {
            if (identifier.startsWith("/")) {
                identifier = identifier.substring(1)
            }

            def doc = load(identifier)
            if (doc) {
                return new Location(doc)
            }

            URI uri = Document.BASE_URI.resolve(identifier)

            log.debug("Finding location status for $uri")

            def docStatus = status(uri)
            if (docStatus.exists && !docStatus.deleted) {
                if (loadDoc) {
                    return new Location(load(docStatus.id)).withResponseCode(301)
                } else {
                    return new Location().withId(docStatus.id).withURI(docStatus.uri).withResponseCode(301)
                }
            }

            log.debug("Check sameAs identifiers.")
            doc = loadBySameAsIdentifier(identifier)
            if (doc) {
                if (loadDoc) {
                    return new Location(doc).withResponseCode(303)
                } else {
                    return new Location().withId(doc.getShortId()).withURI(doc.getURI()).withResponseCode(303)
                }
            }
        }

        return null
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
            doc = loadFromSql(GET_DOCUMENT_VERSION, [1: id, 2: version])
        } else {
            doc = loadFromSql(GET_DOCUMENT, [1: id])
        }
        return doc
    }

    List<String> getDependencies(String id) {
        return getDependencyData(id, GET_DEPENDENCIES)
    }

    List<String> getDependers(String id) {
        return getDependencyData(id, GET_DEPENDERS)
    }

    private List<String> getDependencyData(String id, String query) {
        Connection connection
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(query)
            preparedStatement.setString(1, id)
            rs = preparedStatement.executeQuery()
            List<String> dependecies = []
            while (rs.next()) {
                dependecies.add( rs.getString(1) )
            }
            return dependecies
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
            if (connection != null)
                connection.close()
        }
    }

    String getSystemIdByThingId(String thingId) {
        Connection connection
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(GET_RECORD_ID_BY_THING_ID)
            preparedStatement.setString(1, thingId)
            rs = preparedStatement.executeQuery()
            if (rs.next()) {
                return rs.getString(1)
            }
            return null
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
            if (connection != null)
                connection.close()
        }
    }

    private Document loadFromSql(String sql, Map<Integer, Object> parameters) {
        Document doc = null
        log.debug("loadFromSql $parameters ($sql)")
        Connection connection = getConnection()
        log.debug("Got connection.")
        PreparedStatement selectstmt
        ResultSet rs
        try {
            selectstmt = connection.prepareStatement(sql)
            log.trace("Prepared statement")
            for (items in parameters) {
                if (items.value instanceof String) {
                    selectstmt.setString(items.key, items.value)
                }
                if (items.value instanceof Map || items.value instanceof List) {
                    selectstmt.setObject(items.key, mapper.writeValueAsString(items.value), java.sql.Types.OTHER)
                }
            }
            log.trace("Executing query")
            rs = selectstmt.executeQuery()
            log.trace("Executed query.")
            if (rs.next()) {
                log.trace("next")
                doc = assembleDocument(rs)
                log.trace("Created document with id ${doc.getShortId()}")
            } else if (log.isTraceEnabled()) {
                log.trace("No results returned for get($id)")
            }
        } finally {
            connection.close()
        }

        return doc
    }


    Document loadBySameAsIdentifier(String identifier) {
        log.debug("Using loadBySameAsIdentifier")
        //return loadFromSql(GET_DOCUMENT_BY_SAMEAS_ID, [1:[["sameAs":["@id":identifier]]], 2:["sameAs":["@id":identifier]]]) // This one is for descriptionsbased data
        return loadFromSql(GET_DOCUMENT_BY_SAMEAS_ID, [1: [["sameAs": ["@id": identifier]]]])
    }

    List<Document> loadAllVersions(String identifier) {
        Connection connection = getConnection()
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

    Iterable<Document> loadAll(String collection) {
        return loadAllDocuments(collection, false)
    }

    private Document assembleDocument(ResultSet rs) {

        Document doc = new Document(mapper.readValue(rs.getString("data"), Map))
        doc.setModified(new Date(rs.getTimestamp("modified").getTime()))

        doc.setDeleted(rs.getBoolean("deleted"))

        try {
            doc.setCreated(new Date(rs.getTimestamp("created")?.getTime()))
        } catch (SQLException sqle) {
            log.trace("Resultset didn't have created. Probably a version request.")
        }

        for (altId in loadRecordIdentifiers(doc.id)) {
            doc.addRecordIdentifier(altId)
        }
        for (altId in loadThingIdentifiers(doc.id)) {
            doc.addThingIdentifier(altId)
        }
        return doc

    }

    private List<String> loadRecordIdentifiers(String id) {
        List<String> identifiers = []
        Connection connection = getConnection()
        PreparedStatement loadIds = connection.prepareStatement(LOAD_RECORD_IDENTIFIERS)
        try {
            loadIds.setString(1, id)
            ResultSet rs = loadIds.executeQuery()
            while (rs.next()) {
                identifiers << rs.getString("iri")
            }
        } finally {
            connection.close()
        }
        return identifiers
    }

    private List<String> loadThingIdentifiers(String id) {
        List<String> identifiers = []
        Connection connection = getConnection()
        PreparedStatement loadIds = connection.prepareStatement(LOAD_THING_IDENTIFIERS)
        try {
            loadIds.setString(1, id)
            ResultSet rs = loadIds.executeQuery()
            while (rs.next()) {
                identifiers << rs.getString("iri")
            }
        } finally {
            connection.close()
        }
        return identifiers
    }

    private Iterable<Document> loadAllDocuments(String collection, boolean withLinks, Date since = null, Date until = null) {
        log.debug("Load all called with collection: $collection")
        return new Iterable<Document>() {
            Iterator<Document> iterator() {
                Connection connection = getConnection()
                connection.setAutoCommit(false)
                PreparedStatement loadAllStatement
                long untilTS = until?.getTime() ?: PGStatement.DATE_POSITIVE_INFINITY
                long sinceTS = since?.getTime() ?: 0L

                if (collection) {
                    if (withLinks) {
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS_WITH_LINKS_BY_COLLECTION)
                    } else {
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS_BY_COLLECTION)
                    }
                } else {
                    if (withLinks) {
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS_WITH_LINKS + " ORDER BY modified")
                    } else {
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS + " ORDER BY modified")
                    }
                }
                loadAllStatement.setFetchSize(100)
                loadAllStatement.setTimestamp(1, new Timestamp(sinceTS))
                loadAllStatement.setTimestamp(2, new Timestamp(untilTS))
                if (collection) {
                    loadAllStatement.setString(3, collection)
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
                        doc = assembleDocument(rs)
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

    boolean remove(String identifier, String changedIn, String changedBy, String collection) {
        if (versioning) {
            log.debug("Marking document with ID ${identifier} as deleted.")
            try {
                storeAtomicUpdate(identifier, false, changedIn, changedBy, collection, true,
                    { Document doc ->
                        // Add a tombstone marker (without removing anything) perhaps?
                    })
                return true
            } catch (Throwable e) {
                log.warn("Could not mark document with ID ${identifier} as deleted: ${e}")
                return false
            }
        } else {
            throw new whelk.exception.WhelkException(
                    "Actually deleting data from lddb is currently not supported, because doing so would" +
                            "make the APIX-exporter (which will pickup the delete after the fact) not know what to delete in Voyager," +
                            "which is unacceptable as long as Voyager still lives.")
        }
    }


    protected Document createTombstone(String id) {
        // FIXME verify that this is correct behavior
        String fullId = Document.BASE_URI.resolve(id).toString()
        return new Document(["@graph": [["@id": fullId, "@type": "Tombstone"]]])
    }

    public Map loadSettings(String key) {
        Connection connection = getConnection()
        PreparedStatement selectstmt
        ResultSet rs
        Map settings = [:]
        try {
            selectstmt = connection.prepareStatement(LOAD_SETTINGS)
            selectstmt.setString(1, key)
            rs = selectstmt.executeQuery()
            if (rs.next()) {
                settings = mapper.readValue(rs.getString("settings"), Map)
            } else if (log.isTraceEnabled()) {
                log.trace("No settings found for $key")
            }
        } finally {
            connection.close()
        }

        return settings
    }

    public void saveSettings(String key, final Map settings) {
        Connection connection = getConnection()
        PreparedStatement savestmt
        try {
            String serializedSettings = mapper.writeValueAsString(settings)
            log.debug("Saving settings for ${key}: $serializedSettings")
            savestmt = connection.prepareStatement(SAVE_SETTINGS)
            savestmt.setObject(1, serializedSettings, Types.OTHER)
            savestmt.setString(2, key)
            savestmt.setString(3, key)
            savestmt.setObject(4, serializedSettings, Types.OTHER)
            savestmt.executeUpdate()
        } finally {
            connection.close()
        }
    }

    Connection getConnection() {
        return connectionPool.getConnection()
    }

    List<Document> findByRelation(String relation, String reference,
                                  int limit, int offset) {
        Connection connection = getConnection()
        PreparedStatement find = connection.prepareStatement(FIND_BY)

        find = rigFindByRelationStatement(find, relation, reference, limit, offset)

        try {
            return executeFindByQuery(find)
        } finally {
            connection.close()
        }
    }

    List<Document> findByRelation(String relation, String reference) {
        int limit = DEFAULT_PAGE_SIZE
        int offset = 0

        findByRelation(relation, reference, limit, offset)
    }

    int countByRelation(String relation, String reference) {
        Connection connection = getConnection()
        PreparedStatement count = connection.prepareStatement(COUNT_BY)

        count = rigCountByRelationStatement(count, relation, reference)

        try {
            return executeCountByQuery(count)
        } finally {
            connection.close()
        }
    }

    List<Document> findByQuotation(String identifier, int limit, int offset) {
        Connection connection = getConnection()
        PreparedStatement find = connection.prepareStatement(FIND_BY)

        find = rigFindByQuotationStatement(find, identifier, limit, offset)

        try {
            return executeFindByQuery(find)
        } finally {
            connection.close()
        }

    }

    List<Document> findByQuotation(String identifier) {
        int limit = DEFAULT_PAGE_SIZE
        int offset = 0

        findByQuotation(identifier, limit, offset)
    }

    int countByQuotation(String identifier) {
        Connection connection = getConnection()
        PreparedStatement count = connection.prepareStatement(COUNT_BY)

        count = rigCountByQuotationStatement(count, identifier)

        try {
            return executeCountByQuery(count)
        } finally {
            connection.close()
        }

    }

    List<Document> findByValue(String relation, String value, int limit,
                               int offset) {
        Connection connection = getConnection()
        PreparedStatement find = connection.prepareStatement(FIND_BY)

        find = rigFindByValueStatement(find, relation, value, limit, offset)

        try {
            return executeFindByQuery(find)
        } finally {
            connection.close()
        }
    }

    List<Document> findByValue(String relation, String value) {
        int limit = DEFAULT_PAGE_SIZE
        int offset = 0

        findByValue(relation, value, limit, offset)
    }

    int countByValue(String relation, String value) {
        Connection connection = getConnection()
        PreparedStatement count = connection.prepareStatement(COUNT_BY)

        count = rigCountByValueStatement(count, relation, value)

        try {
            return executeCountByQuery(count)
        } finally {
            connection.close()
        }
    }

    private List<Document> executeFindByQuery(PreparedStatement query) {
        log.debug("Executing find query: ${query}")

        ResultSet rs = query.executeQuery()

        List<Document> docs = []

        while (rs.next()) {
            docs << assembleDocument(rs)
        }

        return docs
    }

    private int executeCountByQuery(PreparedStatement query) {
        log.debug("Executing count query: ${query}")

        ResultSet rs = query.executeQuery()

        int result = 0

        if (rs.next()) {
            result = rs.getInt('count')
        }

        return result
    }

    private PreparedStatement rigFindByRelationStatement(PreparedStatement find,
                                                         String relation,
                                                         String reference,
                                                         int limit,
                                                         int offset) {
        List refQuery = [[(relation): ["@id": reference]]]
        List refsQuery = [[(relation): [["@id": reference]]]]

        return rigFindByStatement(find, refQuery, refsQuery, limit, offset)
    }

    private PreparedStatement rigCountByRelationStatement(PreparedStatement find,
                                                          String relation,
                                                          String reference) {
        List refQuery = [[(relation): ["@id": reference]]]
        List refsQuery = [[(relation): [["@id": reference]]]]

        return rigCountByStatement(find, refQuery, refsQuery)
    }

    private PreparedStatement rigFindByQuotationStatement(PreparedStatement find,
                                                          String identifier,
                                                          int limit,
                                                          int offset) {
        List refQuery = [["@graph": ["@id": identifier]]]
        List sameAsQuery = [["@graph": [["@sameAs": [["@id": identifier]]]]]]

        return rigFindByStatement(find, refQuery, sameAsQuery, limit, offset)
    }

    private PreparedStatement rigCountByQuotationStatement(PreparedStatement find,
                                                           String identifier) {
        List refQuery = [["@graph": ["@id": identifier]]]
        List sameAsQuery = [["@graph": [["@sameAs": [["@id": identifier]]]]]]

        return rigCountByStatement(find, refQuery, sameAsQuery)
    }

    private PreparedStatement rigFindByValueStatement(PreparedStatement find,
                                                      String relation,
                                                      String value,
                                                      int limit,
                                                      int offset) {
        List valueQuery = [[(relation): value]]
        List valuesQuery = [[(relation): [value]]]

        return rigFindByStatement(find, valueQuery, valuesQuery, limit, offset)
    }

    private PreparedStatement rigCountByValueStatement(PreparedStatement find,
                                                       String relation,
                                                       String value) {
        List valueQuery = [[(relation): value]]
        List valuesQuery = [[(relation): [value]]]

        return rigCountByStatement(find, valueQuery, valuesQuery)
    }

    private PreparedStatement rigFindByStatement(PreparedStatement find,
                                                 List firstCondition,
                                                 List secondCondition,
                                                 int limit,
                                                 int offset) {
      find.setObject(1, mapper.writeValueAsString(firstCondition),
                     java.sql.Types.OTHER)
      find.setObject(2, mapper.writeValueAsString(secondCondition),
                     java.sql.Types.OTHER)
      find.setInt(3, limit)
      find.setInt(4, offset)
      return find
    }

    private PreparedStatement rigCountByStatement(PreparedStatement find,
                                                  List firstCondition,
                                                  List secondCondition) {
      find.setObject(1, mapper.writeValueAsString(firstCondition),
                     java.sql.Types.OTHER)
      find.setObject(2, mapper.writeValueAsString(secondCondition),
                     java.sql.Types.OTHER)
      return find
    }
}
