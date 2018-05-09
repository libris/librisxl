package whelk.component

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import whelk.util.LegacyIntegrationTools

import static groovy.transform.TypeCheckingMode.SKIP
import groovy.util.logging.Log4j2 as Log
import groovy.json.StringEscapeUtils
import org.apache.commons.dbcp2.BasicDataSource
import org.codehaus.jackson.map.ObjectMapper
import org.postgresql.PGStatement
import org.postgresql.util.PSQLException
import whelk.Document
import whelk.IdType
import whelk.JsonLd
import whelk.Location
import whelk.exception.StorageCreateFailedException
import whelk.exception.TooHighEncodingLevelException
import whelk.filter.LinkFinder

import java.sql.*
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import java.util.regex.Matcher
import java.util.regex.Pattern

@Log
@CompileStatic
class PostgreSQLComponent {

    /**
     * Interface for performing atomic document updates
     */
    public interface UpdateAgent {
        public void update(Document doc)
    }

    private BasicDataSource connectionPool
    static String driverClass = "org.postgresql.Driver"

    public final static ObjectMapper mapper = new ObjectMapper()

    boolean versioning = true

    /**
     * This value is sensitive. It must be strictly larger than the maxConnections parameter set in tomcat.
     * This is necessary in order to not have the very expensive deadlock/connection-starvation prevention code
     * in here.
     */
    final int MAX_CONNECTION_COUNT = 300

    // SQL statements
    protected String UPDATE_DOCUMENT, INSERT_DOCUMENT,
                     INSERT_DOCUMENT_VERSION, GET_DOCUMENT, GET_EMBELLISHED_DOCUMENT,
                     GET_DOCUMENT_VERSION, GET_ALL_DOCUMENT_VERSIONS,
                     GET_DOCUMENT_VERSION_BY_MAIN_ID,
                     GET_ALL_DOCUMENT_VERSIONS_BY_MAIN_ID,
                     GET_DOCUMENT_BY_SAMEAS_ID, LOAD_ALL_DOCUMENTS,
                     LOAD_ALL_DOCUMENTS_BY_COLLECTION,
                     DELETE_DOCUMENT_STATEMENT, STATUS_OF_DOCUMENT,
                     LOAD_ID_FROM_ALTERNATE, INSERT_IDENTIFIERS,
                     LOAD_RECORD_IDENTIFIERS, LOAD_THING_IDENTIFIERS, DELETE_IDENTIFIERS, LOAD_COLLECTIONS,
                     GET_DOCUMENT_FOR_UPDATE, GET_CONTEXT, GET_RECORD_ID_BY_THING_ID, GET_DEPENDENCIES, GET_DEPENDERS,
                     GET_DOCUMENT_BY_MAIN_ID, GET_RECORD_ID, GET_THING_ID, GET_MAIN_ID, GET_ID_TYPE
    protected String LOAD_SETTINGS, SAVE_SETTINGS
    protected String GET_DEPENDENCIES_OF_TYPE, GET_DEPENDERS_OF_TYPE
    protected String DELETE_DEPENDENCIES, INSERT_DEPENDENCIES
    protected String QUERY_LD_API
    protected String FIND_BY, COUNT_BY
    protected String GET_SYSTEMID_BY_IRI
    protected String GET_DOCUMENT_BY_IRI
    protected String GET_MINMAX_MODIFIED
    protected String UPDATE_MINMAX_MODIFIED
    protected String GET_LEGACY_PROFILE
    protected String INSERT_EMBELLISHED_DOCUMENT
    protected String DELETE_EMBELLISHED_DOCUMENT

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

    LinkFinder linkFinder

    class AcquireLockException extends RuntimeException { AcquireLockException(String s) { super(s) } }

    class ConflictingHoldException extends RuntimeException { ConflictingHoldException(String s) { super(s) } }

    // for testing
    PostgreSQLComponent() {}

    PostgreSQLComponent(Properties properties) {
        setup(properties.getProperty("sqlUrl"), properties.getProperty("sqlMaintable"))
    }

    PostgreSQLComponent(String sqlUrl, String sqlMaintable) {
        setup(sqlUrl, sqlMaintable)
    }

    private void setup(String sqlUrl, String sqlMaintable) {
        mainTableName = sqlMaintable
        String idTableName = mainTableName + "__identifiers"
        String versionsTableName = mainTableName + "__versions"
        String settingsTableName = mainTableName + "__settings"
        String dependenciesTableName = mainTableName + "__dependencies"
        String profilesTableName = mainTableName + "__profiles"
        String embellishedTableName = mainTableName + "__embellished"

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
            connectionPool.setInitialSize(1)
            connectionPool.setMaxTotal(MAX_CONNECTION_COUNT)
            connectionPool.setDefaultAutoCommit(true)
        }

        if (sqlUrl != null)
            this.linkFinder = new LinkFinder(this)

        // Setting up sql-statements
        UPDATE_DOCUMENT = "UPDATE $mainTableName SET data = ?, collection = ?, changedIn = ?, changedBy = ?, checksum = ?, deleted = ?, modified = ? WHERE id = ?"
        INSERT_DOCUMENT = "INSERT INTO $mainTableName (id,data,collection,changedIn,changedBy,checksum,deleted) VALUES (?,?,?,?,?,?,?)"
        DELETE_IDENTIFIERS = "DELETE FROM $idTableName WHERE id = ?"
        INSERT_IDENTIFIERS = "INSERT INTO $idTableName (id, iri, graphIndex, mainId) VALUES (?,?,?,?)"

        DELETE_DEPENDENCIES = "DELETE FROM $dependenciesTableName WHERE id = ?"
        INSERT_DEPENDENCIES = "INSERT INTO $dependenciesTableName (id, relation, dependsOnId) VALUES (?,?,?)"

        INSERT_DOCUMENT_VERSION = "INSERT INTO $versionsTableName (id, data, collection, changedIn, changedBy, checksum, created, modified, deleted) SELECT ?,?,?,?,?,?,?,?,? " +
                "WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM $versionsTableName WHERE id = ? " +
                "ORDER BY modified DESC LIMIT 1) AS last WHERE last.checksum = ?)"

        INSERT_EMBELLISHED_DOCUMENT = "INSERT INTO $embellishedTableName (id, data) VALUES (?,?)"
        DELETE_EMBELLISHED_DOCUMENT = "DELETE FROM $embellishedTableName WHERE id = ?"

        GET_DOCUMENT = "SELECT id,data,created,modified,deleted FROM $mainTableName WHERE id= ?"
        GET_EMBELLISHED_DOCUMENT = "SELECT data from lddb__embellished where id = ?"
        GET_DOCUMENT_FOR_UPDATE = "SELECT id,data,collection,created,modified,deleted,changedBy FROM $mainTableName WHERE id= ? FOR UPDATE"
        GET_DOCUMENT_VERSION = "SELECT id,data FROM $versionsTableName WHERE id = ? AND checksum = ?"
        GET_DOCUMENT_VERSION_BY_MAIN_ID = "SELECT id,data FROM $versionsTableName " +
                                          "WHERE id = (SELECT id FROM $idTableName " +
                                                      "WHERE iri = ? AND mainid = 't') " +
                                          "AND checksum = ?"
        GET_ALL_DOCUMENT_VERSIONS = "SELECT id,data,deleted,created,modified " +
                "FROM $versionsTableName WHERE id = ? ORDER BY modified"
        GET_ALL_DOCUMENT_VERSIONS_BY_MAIN_ID = "SELECT id,data,deleted,created,modified " +
                                               "FROM $versionsTableName " +
                                               "WHERE id = (SELECT id FROM $idTableName " +
                                                           "WHERE iri = ? AND mainid = 't') " +
                                               "ORDER BY modified"
        GET_DOCUMENT_BY_SAMEAS_ID = "SELECT id,data,created,modified,deleted FROM $mainTableName " +
                "WHERE data->'@graph' @> ?"
        GET_RECORD_ID_BY_THING_ID = "SELECT id FROM $idTableName WHERE iri = ? AND graphIndex = 1"
        GET_DOCUMENT_BY_MAIN_ID = "SELECT id,data,created,modified,deleted " +
                                  "FROM $mainTableName " +
                                  "WHERE id = (SELECT id FROM $idTableName " +
                                              "WHERE mainid = 't' AND iri = ?)"
        GET_RECORD_ID = "SELECT iri FROM $idTableName " +
                        "WHERE graphindex = 0 AND mainid = 't' " +
                        "AND id = (SELECT id FROM $idTableName WHERE iri = ?)"
        GET_THING_ID = "SELECT iri FROM $idTableName " +
                        "WHERE graphindex = 1 AND mainid = 't' " +
                        "AND id = (SELECT id FROM $idTableName WHERE iri = ?)"
        GET_MAIN_ID = "SELECT t2.iri FROM $idTableName t1 " +
                      "JOIN $idTableName t2 " +
                      "ON t2.id = t1.id " +
                      "AND t2.graphindex = t1.graphindex " +
                      "WHERE t1.iri = ? AND t2.mainid = true;"
        GET_ID_TYPE = "SELECT graphindex, mainid FROM $idTableName " +
                      "WHERE iri = ?"
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
        GET_DEPENDERS = "SELECT id, relation FROM $dependenciesTableName WHERE dependsOnId = ?"
        GET_DEPENDENCIES = "SELECT dependsOnId, relation FROM $dependenciesTableName WHERE id = ?"
        GET_DEPENDERS_OF_TYPE = "SELECT id FROM $dependenciesTableName WHERE dependsOnId = ? AND relation = ?"
        GET_DEPENDENCIES_OF_TYPE = "SELECT dependsOnId FROM $dependenciesTableName WHERE id = ? AND relation = ?"
        GET_MINMAX_MODIFIED = "SELECT MIN(modified), MAX(modified) from $mainTableName WHERE id IN (?)"
        UPDATE_MINMAX_MODIFIED = "WITH dependsOn AS (SELECT modified FROM $dependenciesTableName JOIN $mainTableName ON " + dependenciesTableName + ".dependsOnId = " + mainTableName+ ".id WHERE " + dependenciesTableName + ".id = ? UNION SELECT modified FROM $mainTableName WHERE id = ?) " +
                "UPDATE $mainTableName SET depMinModified = (SELECT MIN(modified) FROM dependsOn), depMaxModified = (SELECT MAX(modified) FROM dependsOn) WHERE id = ?"

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
        GET_DOCUMENT_BY_IRI = "SELECT data FROM lddb INNER JOIN lddb__identifiers ON lddb.id = lddb__identifiers.id WHERE lddb__identifiers.iri = ?"

        GET_LEGACY_PROFILE = "SELECT profile FROM $profilesTableName WHERE library_id = ?"
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
        Connection connection
        PreparedStatement collectionStatement
        ResultSet collectionResults
        try {
            connection = getConnection()
            collectionStatement = connection.prepareStatement(LOAD_COLLECTIONS)
            collectionResults = collectionStatement.executeQuery()
            List<String> collections = []
            while (collectionResults.next()) {
                String c = collectionResults.getString("collection")
                if (c) {
                    collections.add(c)
                }
            }
            return collections
        } finally {
            if (collectionResults != null)
                collectionResults.close()
            if (collectionStatement != null)
                collectionStatement.close()
            if (connection != null)
                connection.close()
        }
    }

    boolean createDocument(Document doc, String changedIn, String changedBy, String collection, boolean deleted) {
        log.debug("Saving ${doc.getShortId()}, ${changedIn}, ${changedBy}, ${collection}")

        if (linkFinder != null)
            linkFinder.normalizeIdentifiers(doc)

        Connection connection = getConnection()
        connection.setAutoCommit(false)

        /*
        If we're writing a holding post, obtain a (write) lock on the linked bibpost, and hold it until writing has finished.
        While under lock: first check that there is not already a holding for this sigel/bib-id combination.
         */
        RowLock lock = null

        try {
            if (collection == "hold") {
                String holdingFor = doc.getHoldingFor()
                if (holdingFor == null) {
                    log.warn("Was asked to save a holding post linked to a bib post that could not be located: " + doc.getHoldingFor() + " (so, did nothing).")
                    return false
                }
                String holdingForRecordId = getRecordId(holdingFor)
                if (holdingForRecordId == null) {
                    log.warn("Was asked to save a holding post linked to a bib post that could not be located: " + doc.getHoldingFor() + " (so, did nothing).")
                    return false
                }
                String holdingForSystemId = holdingForRecordId.substring(Document.BASE_URI.toString().length())
                if (holdingForSystemId == null) {
                    log.warn("Was asked to save a holding post linked to a bib post that could not be located: " + doc.getHoldingFor() + " (so, did nothing).")
                    return false
                }
                lock = acquireRowLock(holdingForSystemId)

                if (getHoldingForBibAndSigel(holdingFor, doc.getHeldBy(), connection) != null)
                    throw new ConflictingHoldException("Already exists a holding post for ${doc.getHeldBy()} and bib: $holdingFor")
            }

            Date now = new Date()
            PreparedStatement insert = connection.prepareStatement(INSERT_DOCUMENT)

            insert = rigInsertStatement(insert, doc, changedIn, changedBy, collection, deleted)
            insert.executeUpdate()
            connection.commit()
            Document savedDoc = load(doc.getShortId(), connection)
            Date createdAt = parseDate(savedDoc.getCreated())
            Date modifiedAt = parseDate(savedDoc.getModified())
            saveVersion(doc, connection, createdAt, modifiedAt, changedIn, changedBy, collection, deleted)
            refreshDerivativeTables(doc, connection, deleted)
            for (Tuple2<String, String> depender : getDependers(doc.getShortId())) {
                updateMinMaxDepModified((String) depender.get(0), connection)
            }

            connection.commit()
            def status = status(doc.getURI(), connection)
            if (status.exists) {
                doc.setCreated((Date) status['created'])
                doc.setModified((Date) status['modified'])
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
            if (lock != null)
                releaseRowLock(lock)
            connection.close()
            log.debug("[store] Closed connection.")
        }
        return false
    }

    @Canonical
    private class RowLock {
        Connection connection
        PreparedStatement statement
        ResultSet resultSet
    }

    /**
     * HERE BE DRAGONS.
     * Locks the row with the given ID in the database (for updates), until releaseRowLock is called.
     * It is absolutely essential that releaseRowLock be explicitly called after each call to this function.
     * Preferably this should be done in a try/finally block.
     */
    RowLock acquireRowLock(String id) {
        Connection connection = getConnection()
        PreparedStatement lockStatement

        connection.setAutoCommit(false)
        lockStatement = connection.prepareStatement(GET_DOCUMENT_FOR_UPDATE)
        lockStatement.setString(1, id)
        ResultSet resultSet = lockStatement.executeQuery()
        if (!resultSet.next())
            throw new AcquireLockException("There is no document with the id $id (So no lock could be acquired)")

        log.debug("Row lock aquired for $id")
        return new RowLock(connection, lockStatement, resultSet)
    }

    void releaseRowLock(RowLock rowlock) {
        try { rowlock.connection.rollback() } catch (Exception e) {}
        try { rowlock.resultSet.close() } catch (Exception e) {}
        try { rowlock.statement.close() } catch (Exception e) {}
        try { rowlock.connection.close() } catch (Exception e) {}
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
     * Remove both the document at 'remainingID' and the one at 'disapperaingID' and replace them
     * with 'remainingDocument' at 'remainingID'.
     *
     * Will throw exception if 'remainingDocument' does not internally have id set to 'remainingID'
     *
     * The replacement is done atomically within a transaction.
     */
    public void mergeExisting(String remainingID, String disappearingID, Document remainingDocument, String changedIn,
                              String changedBy, String collection, JsonLd jsonld) {
        Connection connection = getConnection()
        connection.setAutoCommit(false)
        PreparedStatement selectStatement
        PreparedStatement updateStatement
        ResultSet resultSet

        try {
            if (! remainingDocument.getCompleteId().equals(remainingID))
                throw new RuntimeException("Bad merge argument, remaining document must have the remaining ID.")

            String remainingSystemID = getSystemIdByIri(remainingID, connection)
            String disappearingSystemID = getSystemIdByIri(disappearingID, connection)

            if (remainingSystemID.equals(disappearingSystemID))
                throw new SQLException("Cannot self-merge.")

            // Update the remaining record
            selectStatement = connection.prepareStatement(GET_DOCUMENT_FOR_UPDATE)
            selectStatement.setString(1, remainingSystemID)
            resultSet = selectStatement.executeQuery()
            if (!resultSet.next())
                throw new SQLException("There is no document with the id: " + remainingID)
            Date createdTime = new Date(resultSet.getTimestamp("created").getTime())
            Document documentToBeReplaced = assembleDocument(resultSet)
            if (documentToBeReplaced.deleted)
                throw new SQLException("Not allowed to merge deleted record: " + remainingID)
            resultSet.close()
            Date modTime = new Date()
            updateStatement = connection.prepareStatement(UPDATE_DOCUMENT)
            rigUpdateStatement(updateStatement, remainingDocument, modTime, changedIn, changedBy, collection, false)
            updateStatement.execute()
            saveVersion(remainingDocument, connection, createdTime, modTime, changedIn, changedBy, collection, false)
            refreshDerivativeTables(remainingDocument, connection, false)

            // Update dependers on the remaining record
            List<Tuple2<String, String>> dependers = getDependers(remainingDocument.getShortId())
            for (Tuple2<String, String> depender : dependers) {
                String dependerShortId = depender.get(0)
                updateMinMaxDepModified((String) dependerShortId, connection)
            }

            // Update the disappearing record
            selectStatement = connection.prepareStatement(GET_DOCUMENT_FOR_UPDATE)
            selectStatement.setString(1, disappearingSystemID)
            resultSet = selectStatement.executeQuery()
            if (!resultSet.next())
                throw new SQLException("There is no document with the id: " + disappearingID)
            Document disappearingDocument = assembleDocument(resultSet)
            if (disappearingDocument.deleted)
                throw new SQLException("Not allowed to merge deleted record: " + disappearingID)
            disappearingDocument.setDeleted(true)
            createdTime = new Date(resultSet.getTimestamp("created").getTime())
            resultSet.close()
            updateStatement = connection.prepareStatement(UPDATE_DOCUMENT)
            rigUpdateStatement(updateStatement, disappearingDocument, modTime, changedIn, changedBy, collection, true)
            updateStatement.execute()
            saveVersion(disappearingDocument, connection, createdTime, modTime, changedIn, changedBy, collection, true)
            saveIdentifiers(disappearingDocument, connection, true, true)
            saveDependencies(disappearingDocument, connection)

            // Update dependers on the disappearing record
            dependers = getDependers(disappearingSystemID)
            for (Tuple2<String, String> depender : dependers) {
                String dependerShortId = depender.get(0)
                updateMinMaxDepModified((String) dependerShortId, connection)
                selectStatement = connection.prepareStatement(GET_DOCUMENT_FOR_UPDATE)
                selectStatement.setString(1, dependerShortId)
                resultSet = selectStatement.executeQuery()
                if (!resultSet.next())
                    throw new SQLException("There is no document with the id: " + dependerShortId + ", the document was to be updated because of merge of " + remainingID + " and " + disappearingID)
                Document dependerDoc = assembleDocument(resultSet)
                if (linkFinder != null)
                    linkFinder.normalizeIdentifiers(dependerDoc, connection)
                updateStatement = connection.prepareStatement(UPDATE_DOCUMENT)
                String dependerCollection = LegacyIntegrationTools.determineLegacyCollection(dependerDoc, jsonld)
                rigUpdateStatement(updateStatement, dependerDoc, modTime, changedIn, changedBy, dependerCollection, false)
                updateStatement.execute()
                updateStatement.close()
                refreshDerivativeTables(dependerDoc, connection, false)
            }

            // All done
            connection.commit()
        } catch (Throwable e) {
            connection.rollback()
            throw e
        } finally {
            if (resultSet != null)
                resultSet.close()
            if (selectStatement != null)
                selectStatement.close()
            if (updateStatement != null)
                updateStatement.close()
            if (connection != null) {
                connection.close()
            }
        }
    }

    /**
     * Take great care that the actions taken by your UpdateAgent are quick and not reliant on IO. The row will be
     * LOCKED while the update is in progress.
     */
    public Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, UpdateAgent updateAgent) {
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

            String collection = resultSet.getString("collection")
            String oldChangedBy = resultSet.getString("changedBy")
            if (changedBy == null)
                changedBy = oldChangedBy

            // Performs the callers updates on the document
            Document preUpdateDoc = doc.clone()
            updateAgent.update(doc)
            if (linkFinder != null)
                linkFinder.normalizeIdentifiers(doc)
            verifyDocumentURIupdates(preUpdateDoc, doc)

            boolean deleted = doc.getDeleted()

            Date createdTime = new Date(resultSet.getTimestamp("created").getTime())
            Date modTime = new Date()
            if (minorUpdate) {
                modTime = new Date(resultSet.getTimestamp("modified").getTime())
            }
            updateStatement = connection.prepareStatement(UPDATE_DOCUMENT)
            rigUpdateStatement(updateStatement, doc, modTime, changedIn, changedBy, collection, deleted)
            updateStatement.execute()

            // The versions and identifiers tables are NOT under lock. Synchronization is only maintained on the main table.
            saveVersion(doc, connection, createdTime, modTime, changedIn, changedBy, collection, deleted)
            refreshDerivativeTables(doc, connection, deleted)
            for (Tuple2<String, String> depender : getDependers(doc.getShortId())) {
                updateMinMaxDepModified((String) depender.get(0), connection)
            }
            updateMinMaxDepModified(doc.getShortId(), connection)
            connection.commit()
            log.debug("Saved document ${doc.getShortId()} with timestamps ${doc.created} / ${doc.modified}")
        } catch (PSQLException psqle) {
            log.error("SQL failed: ${psqle.message}")
            connection.rollback()
            if (psqle.serverErrorMessage.message.startsWith("duplicate key value violates unique constraint")) {
                throw new StorageCreateFailedException(id)
            } else {
                throw psqle
            }
        } catch (TooHighEncodingLevelException e) {
            connection.rollback() // KP Not needed?
            throw e
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
     * Returns if the URIs pointing to 'doc' are acceptable for an update to 'pre_update_doc',
     * otherwise throws.
     */
    private void verifyDocumentURIupdates(Document preUpdateDoc, Document postUpdateDoc) {

        // Compile list of all old IDs
        HashSet<String> oldIDs = new HashSet<>()
        oldIDs.addAll( preUpdateDoc.getRecordIdentifiers() )
        oldIDs.addAll( preUpdateDoc.getThingIdentifiers() )

        // Compile list of all new IDs
        HashSet<String> newIDs = new HashSet<>()
        newIDs.addAll( postUpdateDoc.getRecordIdentifiers() )
        newIDs.addAll( postUpdateDoc.getThingIdentifiers() )

        // (#work identifiers integrity is not enforced, because doing so would disable breaking out works.)

        // Are any IDs missing?
        HashSet<String> missingIDs = new HashSet<>()
        missingIDs.addAll( oldIDs )
        missingIDs.removeAll( newIDs )
        if ( ! missingIDs.isEmpty() )
            throw new RuntimeException("An update of " + preUpdateDoc.getCompleteId() + " MUST contain all URIs pertaining to the original record and its main entity and their sameAs-es. Missing URIs: " + missingIDs)

        // Are any of the added IDs already in use?
        HashSet<String> addedIDs = new HashSet<>()
        addedIDs.addAll( newIDs )
        addedIDs.removeAll( oldIDs )
        for (String id : addedIDs) {
            if ( getSystemIdByIri(id) != null )
                throw new RuntimeException("An update of " + preUpdateDoc.getCompleteId() + " MUST NOT have URIs that are already in use for other records. The update contained an offending URI: " + id)
        }

        // We're ok.
        return
    }

    public refreshDerivativeTables(Document doc) {
        refreshDerivativeTables(doc, getConnection(), doc.deleted)
    }

    public refreshDerivativeTables(Document doc, Connection connection, boolean deleted) {
        saveIdentifiers(doc, connection, deleted)
        saveDependencies(doc, connection)
        removeEmbellishedDocument(doc.getShortId(), connection)
    }

    /**
     * Given a document, look up all it's dependencies (links/references) and return a list of those references that
     * have Libris system IDs (fnrgls), in String[2] form. First element is the relation and second is the link.
     * You were probably looking for getDependencies() which is much more efficient
     * for a document that is already saved in lddb!
     */
    public List<String[]> calculateDependenciesSystemIDs(Document doc) {
        Connection connection = getConnection()
        try {
            return _calculateDependenciesSystemIDs(doc, connection)
        } finally {
            connection.close()
        }
    }

    private List<String[]> _calculateDependenciesSystemIDs(Document doc, Connection connection) {
        List<String[]> dependencies = []
        for (String[] reference : doc.getRefsWithRelation()) {
            String relation = reference[0]
            String iri = reference[1]
            if (!iri.startsWith("http"))
                continue
            PreparedStatement getSystemId
            try {
                getSystemId = connection.prepareStatement(GET_SYSTEMID_BY_IRI)
                getSystemId.setString(1, iri)
                ResultSet rs
                try {
                    rs = getSystemId.executeQuery()
                    if (rs.next()) {
                        if (!rs.getString(1).equals(doc.getShortId())) // Exclude A -> A (self-references)
                            dependencies.add([relation, rs.getString(1)] as String[])
                    }
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

        if (!doc.deleted) { // We do not care for the dependencies of deleted documents.
            // Insert the dependency list
            PreparedStatement insertDependencies = connection.prepareStatement(INSERT_DEPENDENCIES)
            for (String[] dependsOn : dependencies) {
                insertDependencies.setString(1, doc.getShortId())
                insertDependencies.setString(2, dependsOn[0])
                insertDependencies.setString(3, dependsOn[1])
                insertDependencies.addBatch()
            }
            try {
                insertDependencies.executeBatch()
            } catch (BatchUpdateException bue) {
                log.error("Failed saving dependencies for ${doc.getShortId()}")
                throw bue.getNextException()
            } finally {
                insertDependencies.close()
            }
        }
    }

    private void updateMinMaxDepModified(String id) {
        Connection connection
        try {
            connection = getConnection()
            updateMinMaxDepModified(id, connection)
        }
        finally {
            if (connection != null)
                connection.close()
        }
    }

    private void updateMinMaxDepModified(String id, Connection connection) {
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            preparedStatement = connection.prepareStatement(UPDATE_MINMAX_MODIFIED)
            preparedStatement.setString(1, id)
            preparedStatement.setString(2, id)
            preparedStatement.setString(3, id)
            preparedStatement.execute()
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
        }

    }

    private void saveIdentifiers(Document doc, Connection connection, boolean deleted, boolean removeOnly = false) {
        PreparedStatement removeIdentifiers = connection.prepareStatement(DELETE_IDENTIFIERS)
        removeIdentifiers.setString(1, doc.getShortId())
        int numRemoved = removeIdentifiers.executeUpdate()
        log.debug("Removed $numRemoved identifiers for id ${doc.getShortId()}")

        if (removeOnly)
            return

        PreparedStatement altIdInsert = connection.prepareStatement(INSERT_IDENTIFIERS)
        for (altId in doc.getRecordIdentifiers()) {
            altIdInsert.setString(1, doc.getShortId())
            altIdInsert.setString(2, altId)
            altIdInsert.setInt(3, 0) // record id -> graphIndex = 0
            if (altId == doc.getCompleteId()) {
                altIdInsert.setBoolean(4, true) // Main ID
                altIdInsert.addBatch()
            } else if (!deleted) {
                altIdInsert.setBoolean(4, false) // alternative ID, not the main ID
                altIdInsert.addBatch()
            }
        }
        for (altThingId in doc.getThingIdentifiers()) {
            // don't re-add thing identifiers if doc is deleted
            if (!deleted) {
                altIdInsert.setString(1, doc.getShortId())
                altIdInsert.setString(2, altThingId)
                altIdInsert.setInt(3, 1) // thing id -> graphIndex = 1
                if (altThingId == doc.getThingIdentifiers()[0]) {
                    altIdInsert.setBoolean(4, true) // Main ID
                    altIdInsert.addBatch()
                } else {
                    altIdInsert.setBoolean(4, false) // alternative ID
                    altIdInsert.addBatch()
                }
            }
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

    boolean saveVersion(Document doc, Connection connection, Date createdTime,
                        Date modTime, String changedIn, String changedBy,
                        String collection, boolean deleted) {
        if (versioning) {
            PreparedStatement insvers = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
            try {
                log.debug("Trying to save a version of ${doc.getShortId() ?: ""} with checksum ${doc.getChecksum()}. Modified: $modTime")
                insvers = rigVersionStatement(insvers, doc, createdTime,
                                              modTime, changedIn, changedBy,
                                              collection, deleted)
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

    private PreparedStatement rigVersionStatement(PreparedStatement insvers,
                                                  Document doc, Date createdTime,
                                                  Date modTime, String changedIn,
                                                  String changedBy, String collection,
                                                  boolean deleted) {
        insvers.setString(1, doc.getShortId())
        insvers.setObject(2, doc.dataAsString, Types.OTHER)
        insvers.setString(3, collection)
        insvers.setString(4, changedIn)
        insvers.setString(5, changedBy)
        insvers.setString(6, doc.getChecksum())
        insvers.setTimestamp(7, new Timestamp(createdTime.getTime()))
        insvers.setTimestamp(8, new Timestamp(modTime.getTime()))
        insvers.setBoolean(9, deleted)
        insvers.setString(10, doc.getShortId())
        insvers.setString(11, doc.getChecksum())
        return insvers
    }

    boolean bulkStore(
            final List<Document> docs, String changedIn, String changedBy, String collection) {
        if (!docs || docs.isEmpty()) {
            return true
        }
        log.trace("Bulk storing ${docs.size()} documents.")
        Connection connection = getConnection()
        connection.setAutoCommit(false)
        PreparedStatement batch = connection.prepareStatement(INSERT_DOCUMENT)
        PreparedStatement ver_batch = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
        try {
            docs.each { doc ->
                if (linkFinder != null)
                    linkFinder.normalizeIdentifiers(doc)
                Date now = new Date()
                if (versioning) {
                    ver_batch = rigVersionStatement(ver_batch, doc, now, now, changedIn, changedBy, collection, false)
                    ver_batch.addBatch()
                }
                batch = rigInsertStatement(batch, doc, changedIn, changedBy, collection, false)
                batch.addBatch()
                refreshDerivativeTables(doc, connection, false)
                for (Tuple2<String, String> depender : getDependers(doc.getShortId())) {
                    updateMinMaxDepModified((String) depender.get(0), connection)
                }
            }
            batch.executeBatch()
            ver_batch.executeBatch()
            connection.commit()
            log.debug("Stored ${docs.size()} documents in collection ${collection} (versioning: ${versioning})")
            return true
        } catch (Exception e) {
            log.error("Failed to save batch: ${e.message}. Rolling back..", e)
            if (e instanceof SQLException) {
                Exception nextException = ((SQLException) e).nextException
                log.error("Note: next exception was: ${nextException.message}.", nextException)
            }
            connection.rollback()
        } finally {
            connection.close()
            log.trace("[bulkStore] Closed connection.")
        }
        return false
    }

    Map<String, Object> query(Map<String, List<String>> queryParameters, String collection, StorageType storageType) {
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

        def queryTuple = buildQueryString(queryParameters, collection, storageType)
        String whereClause = queryTuple.first
        List values = queryTuple.second

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

    Tuple2<String, List> buildQueryString(Map queryParameters, String collection, StorageType storageType) {
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
            for (String value : entry.value) {
                if (!firstValue) {
                    whereClause.append(" OR ")
                }
                def sqlKeyValue = translateToSql(key, value, storageType)
                whereClause.append(sqlKeyValue.first)
                values.add(sqlKeyValue.second)
                firstValue = false
            }
            whereClause.append(")")
            firstKey = false
        }
        whereClause.append(")")
        return new Tuple2<>(whereClause.toString(), values)
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
    protected Tuple2<String, String> translateToSql(String key, String value, StorageType storageType) {
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

        return new Tuple2<>(jsonbPath.toString(), value)
    }

    /**
     * Load document using supplied identifier as main ID
     *
     * Supplied identifier can be either record ID or thing ID.
     *
     */
    Document loadDocumentByMainId(String mainId, String version=null) {
        Document doc = null
        if (version && version.isInteger()) {
            int v = version.toInteger()
            def docList = loadAllVersionsByMainId(mainId)
            if (v < docList.size()) {
                doc = docList[v]
            }
        } else if (version) {
            doc = loadFromSql(GET_DOCUMENT_VERSION_BY_MAIN_ID,
                              [1: mainId, 2: version])
        } else {
            doc = loadFromSql(GET_DOCUMENT_BY_MAIN_ID, [1: mainId])
        }
        return doc
    }

    /**
     * Get the corresponding record main ID for supplied identifier
     *
     * Supplied identifier can be either the document ID, the thing ID, or a
     * sameAs ID.
     *
     */
    String getRecordId(String id) {
        Connection connection = getConnection()
        try {
            return getRecordOrThingId(id, GET_RECORD_ID, connection)
        } finally {
            if (connection != null)
                connection.close()
        }
    }

    String getRecordId(String id, Connection connection) {
        return getRecordOrThingId(id, GET_RECORD_ID, connection)
    }

    /**
     * Get the corresponding thing main ID for supplied identifier
     *
     * Supplied identifier can be either the document ID, the thing ID, or a
     * sameAs ID.
     *
     */
    String getThingId(String id) {
        Connection connection = getConnection()
        try {
            return getRecordOrThingId(id, GET_THING_ID, connection)
        } finally {
            if (connection != null)
                connection.close()
        }
    }

    String getThingId(String id, Connection connection) {
        return getRecordOrThingId(id, GET_THING_ID, connection)
    }

    /**
     * Get the corresponding main ID for supplied identifier
     *
     * If the supplied identifier is for the thing, return the thing main ID.
     * If the supplied identifier is for the record, return the record main ID.
     *
     */
    String getMainId(String id) {
        Connection connection = getConnection()
        try {
            return getRecordOrThingId(id, GET_MAIN_ID, connection)
        } finally {
            if (connection != null)
                connection.close()
        }
    }

    String getMainId(String id, Connection connection) {
        return getRecordOrThingId(id, GET_MAIN_ID, connection)
    }

    private String getRecordOrThingId(String id, String sql, Connection connection) {
        PreparedStatement selectstmt
        ResultSet rs
        try {
            selectstmt = connection.prepareStatement(sql)
            selectstmt.setString(1, id)
            rs = selectstmt.executeQuery()
            List<String> ids = []

            while (rs.next()) {
                ids << rs.getString('iri')
            }

            if (ids.size() > 1) {
                log.warn("Multiple main IDs found for ID ${id}")
            }

            if (ids.isEmpty()) {
                return null
            } else {
                return ids[0]
            }
        } finally {
            if (rs != null)
                rs.close()
            if (selectstmt != null)
                selectstmt.close()
        }
    }

    /**
     * Return ID type for identifier, if found.
     *
     */
    IdType getIdType(String id) {
        Connection connection = getConnection()
        PreparedStatement selectstmt
        ResultSet rs
        try {
            selectstmt = connection.prepareStatement(GET_ID_TYPE)
            selectstmt.setString(1, id)
            rs = selectstmt.executeQuery()
            if (rs.next()) {
                int graphIndex = rs.getInt('graphindex')
                boolean isMainId = rs.getBoolean('mainid')
                return determineIdType(graphIndex, isMainId)
            } else {
                return null
            }
        } finally {
            connection.close()
        }
    }

    private IdType determineIdType(int graphIndex, boolean isMainId) {
        if (graphIndex == 0) {
            if (isMainId) {
                return IdType.RecordMainId
            } else {
                return IdType.RecordSameAsId
            }
        } else if (graphIndex == 1) {
            if (isMainId) {
                return IdType.ThingMainId
            } else {
                return IdType.ThingSameAsId
            }
        } else {
            return null
        }
    }

    private void cacheEmbellishedDocument(String id, Document embellishedDocument, Connection connection) {
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            preparedStatement = connection.prepareStatement(INSERT_EMBELLISHED_DOCUMENT)
            preparedStatement.setString(1, id)
            preparedStatement.setObject(2, mapper.writeValueAsString(embellishedDocument.data), java.sql.Types.OTHER)
            preparedStatement.execute()
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
        }
    }

    private void removeEmbellishedDocument(String id, Connection connection) {
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            preparedStatement = connection.prepareStatement(DELETE_EMBELLISHED_DOCUMENT)
            preparedStatement.setString(1, id)
            preparedStatement.execute()
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
        }
    }

    Document loadEmbellished(String id, JsonLd jsonld) {
        Connection connection = getConnection()
        try {
            return loadEmbellished(id, jsonld, connection)
        } finally {
            connection.close()
        }
    }

    /**
     * Loads the embellished version of the stored document 'id'.
     * If there isn't an embellished version cached already, one will be created
     * (lazy caching).
     */
    Document loadEmbellished(String id, JsonLd jsonld, Connection connection) {
        PreparedStatement selectStatement
        ResultSet resultSet

        try {
            selectStatement = connection.prepareStatement(GET_EMBELLISHED_DOCUMENT)
            selectStatement.setString(1, id)
            resultSet = selectStatement.executeQuery()

            if (resultSet.next()) {
                return new Document(mapper.readValue(resultSet.getString("data"), Map))
            }

            // Cache-miss, embellish and store
            Document document = load(id, connection)
            List externalRefs = document.getExternalRefs()
            List convertedExternalLinks = JsonLd.expandLinks(externalRefs, (Map) jsonld.getDisplayData().get(JsonLd.getCONTEXT_KEY()))
            Map referencedData = [:]
            for (String iri : convertedExternalLinks) {
                Document externalDocument = getDocumentByIri(iri, connection)
                if (externalDocument != null)
                    referencedData.put(externalDocument.getShortId(), externalDocument.data)
            }
            jsonld.embellish(document.data, referencedData, false)
            cacheEmbellishedDocument(id, document, connection)
            return document
        }
        finally {
            try {resultSet.close()} catch (Exception e) { /* ignore */ }
            try {selectStatement.close()} catch (Exception e) { /* ignore */}
        }
    }

    Document load(String id, Connection conn = null) {
        return load(id, null, conn)
    }

    Document load(String id, String version, Connection conn = null) {
        Document doc = null
        if (version && version.isInteger()) {
            int v = version.toInteger()
            def docList = loadAllVersions(id, conn)
            if (v < docList.size()) {
                doc = docList[v]
            } else {
                // looks like version might be a checksum, try loading
                doc = loadFromSql(GET_DOCUMENT_VERSION, [1: id, 2: version], conn)
            }
        } else if (version) {
            doc = loadFromSql(GET_DOCUMENT_VERSION, [1: id, 2: version], conn)
        } else {
            doc = loadFromSql(GET_DOCUMENT, [1: id], conn)
        }
        return doc
    }

    Tuple2<Timestamp, Timestamp> getMinMaxModified(List<String> ids) {
        Connection connection
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            connection = getConnection()
            String expandedSql = GET_MINMAX_MODIFIED.replace('?', ids.collect { it -> '?' }.join(','))
            preparedStatement = connection.prepareStatement(expandedSql)
            for (int i = 0; i < ids.size(); ++i) {
                preparedStatement.setString(i+1, ids.get(i))
            }
            rs = preparedStatement.executeQuery()
            if (rs.next()) {
                Timestamp min = (Timestamp) rs.getObject(1)
                Timestamp max = (Timestamp) rs.getObject(2)
                return new Tuple2(min, max)
            }
            else
                return new Tuple2(null, null)
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

    String getSystemIdByIri(String iri) {
        Connection connection = getConnection()
        try {
            return getSystemIdByIri(iri, connection)
        } finally {
            if (connection != null)
                connection.close()
        }
    }

    String getSystemIdByIri(String iri, Connection connection) {
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            preparedStatement = connection.prepareStatement(GET_SYSTEMID_BY_IRI)
            preparedStatement.setString(1, iri)
            rs = preparedStatement.executeQuery()
            if (rs.next())
                return rs.getString(1)
            return null
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
        }
    }

    Document getDocumentByIri(String iri) {
        Connection connection = getConnection()
        try {
            return getDocumentByIri(iri, connection)
        } finally {
            if (connection != null)
                connection.close()
        }
    }

    Document getDocumentByIri(String iri, Connection connection) {
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            preparedStatement = connection.prepareStatement(GET_DOCUMENT_BY_IRI)
            preparedStatement.setString(1, iri)
            rs = preparedStatement.executeQuery()
            if (rs.next())
                return new Document(mapper.readValue(rs.getString("data"), Map))
            return null
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
        }
    }

    String getHoldingForBibAndSigel(String bibThingUri, String libraryUri, Connection connection) {
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            String sql = "SELECT id FROM $mainTableName WHERE data#>>'{@graph, 1, itemOf, @id}' = ? AND data#>>'{@graph, 1, heldBy, @id}' = ? AND deleted = false"
            preparedStatement = connection.prepareStatement(sql)
            preparedStatement.setString(1, bibThingUri)
            preparedStatement.setString(2, libraryUri)
            rs = preparedStatement.executeQuery()
            if (rs.next())
                return rs.getString(1)
            return null
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
        }
    }

    List<Tuple2<String, String>> getDependencies(String id) {
        Connection connection = getConnection()
        try {
            return getDependencyData(id, GET_DEPENDENCIES, connection)
        } finally {
            if (connection != null)
                connection.close()
        }
    }

    List<Tuple2<String, String>> getDependers(String id) {
        Connection connection = getConnection()
        try {
            return getDependencyData(id, GET_DEPENDERS, connection)
        } finally {
            if (connection != null)
                connection.close()
        }
    }

    List<Tuple2<String, String>> getDependencies(String id, Connection connection) {
        return getDependencyData(id, GET_DEPENDENCIES, connection)
    }

    List<Tuple2<String, String>> getDependers(String id, Connection connection) {
        return getDependencyData(id, GET_DEPENDERS, connection)
    }

    private List<Tuple2<String, String>> getDependencyData(String id, String query, Connection connection) {
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            preparedStatement = connection.prepareStatement(query)
            preparedStatement.setString(1, id)
            rs = preparedStatement.executeQuery()
            List<Tuple2<String, String>> dependencies = []
            while (rs.next()) {
                dependencies.add( new Tuple2<String, String>(rs.getString(1), rs.getString(2)) )
            }
            return dependencies
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
        }
    }

    List<String> getDependenciesOfType(String id, String typeOfRelation) {
        return getDependencyDataOfType(id, typeOfRelation, GET_DEPENDENCIES_OF_TYPE)
    }

    List<String> getDependersOfType(String id, String typeOfRelation) {
        return getDependencyDataOfType(id, typeOfRelation, GET_DEPENDERS_OF_TYPE)
    }

    private List<String> getDependencyDataOfType(String id, String typeOfRelation, String query) {
        Connection connection
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(query)
            preparedStatement.setString(1, id)
            preparedStatement.setString(2, typeOfRelation)
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

    /**
     * List all system IDs that match a given typed id and graph index
     * (for example: type:ISBN, value:1234, graphIndex:1 -> ksjndfkjwbr3k)
     */
    public List<String> getSystemIDsByTypedID(String idType, String idValue, int graphIndex) {
        Connection connection
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            String query = "SELECT id FROM lddb WHERE data#>'{@graph," + graphIndex + ",identifiedBy}' @> ?"
            connection = getConnection()
            preparedStatement = connection.prepareStatement(query)
            preparedStatement.setObject(1, "[{\"@type\": \"" + idType + "\", \"value\": \"" + idValue + "\"}]", java.sql.Types.OTHER)

            rs = preparedStatement.executeQuery()
            List<String> results = []
            while (rs.next()) {
                results.add( rs.getString(1) )
            }
            return results
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

    String getProfileByLibraryUri(String libraryUri) {
        Connection connection
        PreparedStatement preparedStatement
        ResultSet rs
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(GET_LEGACY_PROFILE)
            preparedStatement.setString(1, libraryUri)
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

    /**
     * Returns a list of holdings documents, for any of the passed thingIdentifiers
     */
    List<Document> getAttachedHoldings(List<String> thingIdentifiers) {
        // Build the query
        StringBuilder selectSQL = new StringBuilder("SELECT id,data,created,modified,deleted FROM ")
        selectSQL.append(mainTableName)
        selectSQL.append(" WHERE collection = 'hold' AND deleted = false AND (")
        for (int i = 0; i < thingIdentifiers.size(); ++i)
        {
            selectSQL.append(" data#>>'{@graph,1,itemOf,@id}' = ? ")

            // If this is the last id
            if (i+1 == thingIdentifiers.size())
                selectSQL.append(")")
            else
                selectSQL.append("OR")
        }

        // Assemble results
        Connection connection
        ResultSet rs
        PreparedStatement preparedStatement
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(selectSQL.toString())

            for (int i = 0; i < thingIdentifiers.size(); ++i)
            {
                preparedStatement.setString(i+1, thingIdentifiers.get(i))
            }

            rs = preparedStatement.executeQuery()
            List<Document> holdings = []
            while (rs.next()) {
                Document holding = assembleDocument(rs)
                holdings.add(holding)
            }
            return holdings
        }
        finally {
            if (rs != null)
                rs.close()
            if (preparedStatement != null)
                preparedStatement.close()
            if (connection != null)
                connection.close()
        }
        return []
    }

    private Document loadFromSql(String sql, Map parameters, Connection connection = null) {
        Document doc = null
        boolean shouldCloseConn = false
        log.debug("loadFromSql $parameters ($sql)")
        if (!connection) {
            connection = getConnection()
            shouldCloseConn = true
            log.debug("Got connection.")
        }
        PreparedStatement selectstmt
        ResultSet rs
        try {
            selectstmt = connection.prepareStatement(sql)
            log.trace("Prepared statement")
            for (items in parameters) {
                if (items.value instanceof String) {
                    selectstmt.setString((Integer) items.key, (String) items.value)
                }
                if (items.value instanceof Map || items.value instanceof List) {
                    selectstmt.setObject((Integer) items.key, mapper.writeValueAsString(items.value), java.sql.Types.OTHER)
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
                log.trace("No results returned for $selectstmt")
            }
        } finally {
            if (shouldCloseConn) {
                connection.close()
            }
        }

        return doc
    }


    Document loadBySameAsIdentifier(String identifier) {
        log.debug("Using loadBySameAsIdentifier")
        //return loadFromSql(GET_DOCUMENT_BY_SAMEAS_ID, [1:[["sameAs":["@id":identifier]]], 2:["sameAs":["@id":identifier]]]) // This one is for descriptionsbased data
        return loadFromSql(GET_DOCUMENT_BY_SAMEAS_ID, [1: [["sameAs": ["@id": identifier]]]])
    }

    List<Document> loadAllVersions(String identifier, Connection conn = null) {
        return doLoadAllVersions(identifier, GET_ALL_DOCUMENT_VERSIONS, conn)
    }

    List<Document> loadAllVersionsByMainId(String identifier) {
        return doLoadAllVersions(identifier,
                                 GET_ALL_DOCUMENT_VERSIONS_BY_MAIN_ID)
    }

    private List<Document> doLoadAllVersions(String identifier, String sql, Connection connection = null) {
        boolean shouldCloseConn = false
        if (!connection) {
            connection = getConnection()
            shouldCloseConn = true
        }
        PreparedStatement selectstmt
        ResultSet rs
        List<Document> docList = []
        try {
            selectstmt = connection.prepareStatement(sql)
            selectstmt.setString(1, identifier)
            rs = selectstmt.executeQuery()
            int v = 0
            while (rs.next()) {
                def doc = assembleDocument(rs)
                doc.version = v++
                docList << doc
            }
        } finally {
            if (shouldCloseConn) {
                connection.close()
                log.debug("[loadAllVersions] Closed connection.")
            }
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
            // FIXME better handling of null values
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

    @CompileStatic(SKIP)
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

    boolean remove(String identifier, String changedIn, String changedBy) {
        if (versioning) {
            log.debug("Marking document with ID ${identifier} as deleted.")
            try {
                storeAtomicUpdate(identifier, false, changedIn, changedBy,
                    { Document doc ->
                        doc.setDeleted(true)
                        // Add a tombstone marker (without removing anything) perhaps?
                    })
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

        // Clear out dependencies
        Connection connection = getConnection()
        try {
            PreparedStatement removeDependencies = connection.prepareStatement(DELETE_DEPENDENCIES)
            try {
                removeDependencies.setString(1, identifier)
                int numRemoved = removeDependencies.executeUpdate()
                log.debug("Removed $numRemoved dependencies for id ${identifier}")
            } finally {
                removeDependencies.close()
            }
        } finally {
            connection.close()
        }

        return true
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

    /**
     * Get a database connection.
     */
    Connection getConnection(){
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

    private String formatDate(Date date) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault())
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
    }

    private Date parseDate(String date) {
        ZonedDateTime zdt = ZonedDateTime.parse(date, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        return Date.from(zdt.toInstant())
    }
}
