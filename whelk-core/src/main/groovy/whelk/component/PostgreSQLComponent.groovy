package whelk.component

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import groovy.json.StringEscapeUtils
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import org.postgresql.PGStatement
import org.postgresql.util.PSQLException
import whelk.Document
import whelk.IdType
import whelk.JsonLd
import whelk.Storage
import whelk.exception.CancelUpdateException
import whelk.exception.StorageCreateFailedException
import whelk.exception.TooHighEncodingLevelException
import whelk.exception.LinkValidationException
import whelk.exception.WhelkException
import whelk.filter.LinkFinder
import whelk.util.LegacyIntegrationTools

import java.sql.Array
import java.sql.BatchUpdateException
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.sql.Timestamp
import java.util.regex.Matcher
import java.util.regex.Pattern

import static groovy.transform.TypeCheckingMode.SKIP
import static java.sql.Types.OTHER

/**
 *  It is important to not grab more than one connection per request/thread to avoid connection related deadlocks.
 *  i.e. get/release connection should be done in the public methods. Connections should be reused in method calls
 *  within this class.
 */

@Log
@CompileStatic
class PostgreSQLComponent implements Storage {
    public static final String PROPERTY_SQL_URL = "sqlUrl"
    public static final String PROPERTY_SQL_MAIN_TABLE_NAME = "sqlMaintable"
    public static final String PROPERTY_SQL_MAX_POOL_SIZE = "sqlMaxPoolSize"

    public static final ObjectMapper mapper = new ObjectMapper()

    private static final int DEFAULT_MAX_POOL_SIZE = 16
    private static final String UNIQUE_VIOLATION = "23505"
    private static final String driverClass = "org.postgresql.Driver"

    private HikariDataSource connectionPool
    boolean versioning = true
    boolean doVerifyDocumentIdRetention = true

    // SQL statements
    protected String UPDATE_DOCUMENT, INSERT_DOCUMENT,
                     INSERT_DOCUMENT_VERSION, GET_DOCUMENT, GET_EMBELLISHED_DOCUMENT,
                     GET_DOCUMENT_VERSION, GET_ALL_DOCUMENT_VERSIONS,
                     GET_DOCUMENT_VERSION_BY_MAIN_ID,
                     GET_ALL_DOCUMENT_VERSIONS_BY_MAIN_ID,
                     GET_DOCUMENT_BY_SAMEAS_ID, LOAD_ALL_DOCUMENTS,
                     LOAD_ALL_DOCUMENTS_BY_COLLECTION,
                     DELETE_DOCUMENT_STATEMENT, STATUS_OF_DOCUMENT,
                     INSERT_IDENTIFIERS,
                     LOAD_RECORD_IDENTIFIERS, LOAD_THING_IDENTIFIERS, DELETE_IDENTIFIERS, LOAD_COLLECTIONS,
                     GET_DOCUMENT_FOR_UPDATE, GET_CONTEXT, GET_RECORD_ID_BY_THING_ID, GET_DEPENDENCIES, GET_DEPENDERS,
                     GET_DOCUMENT_BY_MAIN_ID, GET_RECORD_ID, GET_THING_ID, GET_MAIN_ID, GET_ID_TYPE, GET_COLLECTION_BY_SYSTEM_ID
    protected String LOAD_SETTINGS, SAVE_SETTINGS
    protected String GET_DEPENDENCIES_OF_TYPE, GET_DEPENDERS_OF_TYPE
    protected String DELETE_DEPENDENCIES, INSERT_DEPENDENCIES
    protected String QUERY_LD_API
    protected String FIND_BY, COUNT_BY
    protected String GET_SYSTEMID_BY_IRI
    protected String GET_THING_MAIN_IRI_BY_SYSTEMID
    protected String GET_DOCUMENT_BY_IRI
    protected String GET_MINMAX_MODIFIED
    protected String UPDATE_MINMAX_MODIFIED
    protected String GET_LEGACY_PROFILE
    protected String INSERT_EMBELLISHED_DOCUMENT
    protected String DELETE_EMBELLISHED_DOCUMENT

    String mainTableName
    LinkFinder linkFinder
    DependencyCache dependencyCache

    class AcquireLockException extends RuntimeException { AcquireLockException(String s) { super(s) } }

    class ConflictingHoldException extends RuntimeException { ConflictingHoldException(String s) { super(s) } }

    // for testing
    PostgreSQLComponent() {}

    PostgreSQLComponent(Properties properties) {
        int maxPoolSize = properties.getProperty(PROPERTY_SQL_MAX_POOL_SIZE)
                ? Integer.parseInt(properties.getProperty(PROPERTY_SQL_MAX_POOL_SIZE))
                : DEFAULT_MAX_POOL_SIZE

        setup(properties.getProperty(PROPERTY_SQL_URL), properties.getProperty(PROPERTY_SQL_MAIN_TABLE_NAME), maxPoolSize)
    }

    PostgreSQLComponent(String sqlUrl, String sqlMainTable) {
        setup(sqlUrl, sqlMainTable, DEFAULT_MAX_POOL_SIZE)
    }

    private void setup(String sqlUrl, String sqlMainTable, int maxPoolSize) {
        mainTableName = sqlMainTable
        String idTableName = mainTableName + "__identifiers"
        String versionsTableName = mainTableName + "__versions"
        String settingsTableName = mainTableName + "__settings"
        String dependenciesTableName = mainTableName + "__dependencies"
        String profilesTableName = mainTableName + "__profiles"
        String embellishedTableName = mainTableName + "__embellished"

        if (sqlUrl) {
            HikariConfig config = new HikariConfig()
            config.setMaximumPoolSize(maxPoolSize)
            config.setAutoCommit(true)
            config.setJdbcUrl(sqlUrl.replaceAll(":\\/\\/\\w+:*.*@", ":\\/\\/"))
            config.setDriverClassName(driverClass)

            log.info("Connecting to sql database at ${config.getJdbcUrl()}, using driver $driverClass. Pool size: $maxPoolSize")
            URI connURI = new URI(sqlUrl.substring(5))
            if (connURI.getUserInfo() != null) {
                String username = connURI.getUserInfo().split(":")[0]
                log.trace("Setting connectionPool username: $username")
                config.setUsername(username)

                try {
                    String password = connURI.getUserInfo().split(":")[1]
                    log.trace("Setting connectionPool password: $password")
                    config.setPassword(password)
                } catch (ArrayIndexOutOfBoundsException ignored) {
                    log.debug("No password part found in connect url userinfo.")
                }
            }

            connectionPool = new HikariDataSource(config)

            this.linkFinder = new LinkFinder(this)
        }

        this.dependencyCache = new DependencyCache(this)

        // Setting up sql-statements
        UPDATE_DOCUMENT = "UPDATE $mainTableName SET data = ?, collection = ?, changedIn = ?, changedBy = ?, checksum = ?, deleted = ?, modified = ? WHERE id = ?"
        INSERT_DOCUMENT = "INSERT INTO $mainTableName (id,data,collection,changedIn,changedBy,checksum,deleted," +
                "created,modified,depminmodified,depmaxmodified) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
        DELETE_IDENTIFIERS = "DELETE FROM $idTableName WHERE id = ?"
        INSERT_IDENTIFIERS = "INSERT INTO $idTableName (id, iri, graphIndex, mainId) VALUES (?,?,?,?)"

        DELETE_DEPENDENCIES = "DELETE FROM $dependenciesTableName WHERE id = ?"
        INSERT_DEPENDENCIES = "INSERT INTO $dependenciesTableName (id, relation, dependsOnId) VALUES (?,?,?)"

        INSERT_DOCUMENT_VERSION = "INSERT INTO $versionsTableName (id, data, collection, changedIn, changedBy, checksum, created, modified, deleted) SELECT ?,?,?,?,?,?,?,?,? "

        INSERT_EMBELLISHED_DOCUMENT = "INSERT INTO $embellishedTableName (id, data) VALUES (?,?)"
        DELETE_EMBELLISHED_DOCUMENT = "DELETE FROM $embellishedTableName WHERE id = ?"

        GET_DOCUMENT = "SELECT id,data,created,modified,deleted FROM $mainTableName WHERE id= ?"
        GET_EMBELLISHED_DOCUMENT = "SELECT data from lddb__embellished where id = ?"
        GET_DOCUMENT_FOR_UPDATE = "SELECT id,data,collection,created,modified,deleted,changedBy FROM $mainTableName WHERE id = ? AND deleted = false FOR UPDATE"
        GET_DOCUMENT_VERSION = "SELECT id,data FROM $versionsTableName WHERE id = ? AND checksum = ?"
        GET_DOCUMENT_VERSION_BY_MAIN_ID = "SELECT id,data FROM $versionsTableName " +
                "WHERE id = (SELECT id FROM $idTableName " +
                "WHERE iri = ? AND mainid = 't') " +
                "AND checksum = ?"
        GET_ALL_DOCUMENT_VERSIONS = "SELECT id,data,deleted,created,modified " +
                "FROM $versionsTableName WHERE id = ? ORDER BY modified DESC"
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
        GET_COLLECTION_BY_SYSTEM_ID = "SELECT collection FROM lddb where id = ?"
        LOAD_ALL_DOCUMENTS = "SELECT id,data,created,modified,deleted FROM $mainTableName WHERE modified >= ? AND modified <= ?"
        // This query does the same as LOAD_COLLECTIONS = "SELECT DISTINCT collection FROM $mainTableName"
        // but much faster because postgres does not yet have 'loose indexscan' aka 'index skip scan'
        // https://wiki.postgresql.org/wiki/Loose_indexscan'
        LOAD_COLLECTIONS = "WITH RECURSIVE t AS ( " +
                "(SELECT collection FROM $mainTableName ORDER BY collection LIMIT 1) " +
                "UNION ALL " +
                "SELECT (SELECT collection FROM $mainTableName WHERE collection > t.collection ORDER BY collection LIMIT 1) " +
                "FROM t " +
                "WHERE t.collection IS NOT NULL" +
                ") " +
                "SELECT collection FROM t WHERE collection IS NOT NULL"
        LOAD_ALL_DOCUMENTS_BY_COLLECTION = "SELECT id,data,created,modified,deleted FROM $mainTableName " +
                "WHERE modified >= ? AND modified <= ? AND collection = ? AND deleted = false"
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
        UPDATE_MINMAX_MODIFIED = "WITH dependsOn AS (SELECT modified FROM $dependenciesTableName JOIN $mainTableName ON " + dependenciesTableName + ".dependsOnId = " + mainTableName + ".id WHERE " + dependenciesTableName + ".id = ? UNION SELECT modified FROM $mainTableName WHERE id = ?) " +
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

        GET_SYSTEMID_BY_IRI = "SELECT id, deleted FROM $idTableName WHERE iri = ?"
        GET_THING_MAIN_IRI_BY_SYSTEMID = "SELECT iri FROM $idTableName WHERE graphindex = 1 and mainid is true and id = ?"
        GET_DOCUMENT_BY_IRI = "SELECT lddb.id,lddb.data,lddb.created,lddb.modified,lddb.deleted FROM lddb INNER JOIN lddb__identifiers ON lddb.id = lddb__identifiers.id WHERE lddb__identifiers.iri = ?"

        GET_LEGACY_PROFILE = "SELECT profile FROM $profilesTableName WHERE library_id = ?"
    }

    private Map status(URI uri, Connection connection) {
        Map statusMap = [:]

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

        log.debug("Loaded status for ${uri}: $statusMap")
        return statusMap
    }

    List<String> loadCollections() {
        Connection connection = null
        PreparedStatement collectionStatement = null
        ResultSet collectionResults = null
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
            close(collectionResults, collectionStatement, connection)
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
        try {
            if (collection == "hold") {
                String holdingFor = doc.getHoldingFor()
                if (holdingFor == null) {
                    log.warn("Was asked to save a holding post linked to a bib post that could not be located: " + doc.getHoldingFor() + " (so, did nothing).")
                    return false
                }
                String holdingForRecordId = getRecordId(holdingFor, connection)
                if (holdingForRecordId == null) {
                    log.warn("Was asked to save a holding post linked to a bib post that could not be located: " + doc.getHoldingFor() + " (so, did nothing).")
                    return false
                }
                String holdingForSystemId = holdingForRecordId.substring(Document.BASE_URI.toString().length())
                if (holdingForSystemId == null) {
                    log.warn("Was asked to save a holding post linked to a bib post that could not be located: " + doc.getHoldingFor() + " (so, did nothing).")
                    return false
                }

                acquireRowLock(holdingForSystemId, connection)

                if (getHoldingForBibAndSigel(holdingFor, doc.getHeldBy(), connection) != null)
                    throw new ConflictingHoldException("Already exists a holding post for ${doc.getHeldBy()} and bib: $holdingFor")
            }

            if (linkFinder != null)
                linkFinder.normalizeIdentifiers(doc, connection)

            //FIXME: throw exception on null changedBy
            if (changedBy != null) {
                String creator = getDescriptionChangerId(changedBy)
                doc.setDescriptionCreator(creator)
                doc.setDescriptionLastModifier(creator)
            }

            Date now = new Date()
            doc.setCreated(now)
            doc.setModified(now)
            doc.setDeleted(deleted)

            PreparedStatement insert = connection.prepareStatement(INSERT_DOCUMENT)
            insert = rigInsertStatement(insert, doc, now, changedIn, changedBy, collection, deleted)
            insert.executeUpdate()

            saveVersion(doc, connection, now, now, changedIn, changedBy, collection, deleted)
            refreshDerivativeTables(doc, connection, deleted)
            for (Tuple2<String, String> depender : getDependers(doc.getShortId(), connection)) {
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
            connection.close()
            log.debug("[store] Closed connection.")
        }
    }

    void acquireRowLock(String id, Connection connection) {
        PreparedStatement lockStatement = connection.prepareStatement(GET_DOCUMENT_FOR_UPDATE)
        lockStatement.setString(1, id)
        ResultSet resultSet = lockStatement.executeQuery()
        if (!resultSet.next())
            throw new AcquireLockException("There is no document with the id $id (So no lock could be acquired)")

        log.debug("Row lock aquired for $id")
    }

    String getContext() {
        Connection connection = null
        PreparedStatement selectStatement = null
        ResultSet resultSet = null

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
            close(resultSet, selectStatement, connection)
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
    void mergeExisting(String remainingID, String disappearingID, Document remainingDocument, String changedIn,
                              String changedBy, String collection, JsonLd jsonld) {
        Connection connection = getConnection()
        connection.setAutoCommit(false)
        PreparedStatement selectStatement = null
        PreparedStatement updateStatement = null
        ResultSet resultSet = null

        try {
            if (remainingDocument.getCompleteId() != remainingID)
                throw new RuntimeException("Bad merge argument, remaining document must have the remaining ID.")

            String remainingSystemID = getSystemIdByIri(remainingID, connection)
            String disappearingSystemID = getSystemIdByIri(disappearingID, connection)

            if (remainingSystemID == disappearingSystemID)
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
            remainingDocument.setModified(modTime)
            updateStatement = connection.prepareStatement(UPDATE_DOCUMENT)
            rigUpdateStatement(updateStatement, remainingDocument, modTime, changedIn, changedBy, collection, false)
            updateStatement.execute()
            saveVersion(remainingDocument, connection, createdTime, modTime, changedIn, changedBy, collection, false)
            refreshDerivativeTables(remainingDocument, connection, false)

            // Update dependers on the remaining record
            List<Tuple2<String, String>> dependers = getDependers(remainingDocument.getShortId(), connection)
            for (Tuple2<String, String> depender : dependers) {
                String dependerShortId = depender.get(0)
                updateMinMaxDepModified((String) dependerShortId, connection)
                removeEmbellishedDocument(dependerShortId, connection)
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
            disappearingDocument.setModified(modTime)
            createdTime = new Date(resultSet.getTimestamp("created").getTime())
            resultSet.close()
            updateStatement = connection.prepareStatement(UPDATE_DOCUMENT)
            rigUpdateStatement(updateStatement, disappearingDocument, modTime, changedIn, changedBy, collection, true)
            updateStatement.execute()
            saveVersion(disappearingDocument, connection, createdTime, modTime, changedIn, changedBy, collection, true)
            saveIdentifiers(disappearingDocument, connection, true, true)
            saveDependencies(disappearingDocument, connection)

            // Update dependers on the disappearing record
            dependers = getDependers(disappearingSystemID, connection)
            for (Tuple2<String, String> depender : dependers) {
                String dependerShortId = depender.get(0)
                removeEmbellishedDocument(dependerShortId, connection)
                updateMinMaxDepModified((String) dependerShortId, connection)
                selectStatement = connection.prepareStatement(GET_DOCUMENT_FOR_UPDATE)
                selectStatement.setString(1, dependerShortId)
                resultSet = selectStatement.executeQuery()
                if (!resultSet.next())
                    throw new SQLException("There is no document with the id: " + dependerShortId + ", the document was to be updated because of merge of " + remainingID + " and " + disappearingID)
                Document dependerDoc = assembleDocument(resultSet)
                if (linkFinder != null)
                    linkFinder.normalizeIdentifiers(dependerDoc, connection)
                dependerDoc.setModified(modTime)
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
            close(resultSet, selectStatement, updateStatement, connection)
        }
    }

    /**
     * Take great care that the actions taken by your UpdateAgent are quick and not reliant on IO. The row will be
     * LOCKED while the update is in progress.
     */
    Document storeAtomicUpdate(String id, boolean minorUpdate, String changedIn, String changedBy, UpdateAgent updateAgent) {
        log.debug("Saving (atomic update) ${id}")

        // Resources to be closed
        Connection connection = getConnection()
        PreparedStatement selectStatement = null
        PreparedStatement updateStatement = null
        ResultSet resultSet = null

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
                linkFinder.normalizeIdentifiers(doc, connection)
            if (doVerifyDocumentIdRetention) {
                verifyDocumentIdRetention(preUpdateDoc, doc, connection)
            }

            boolean deleted = doc.getDeleted()

            Date createdTime = new Date(resultSet.getTimestamp("created").getTime())
            Date modTime = minorUpdate
                ? new Date(resultSet.getTimestamp("modified").getTime())
                : new Date()
            doc.setModified(modTime)

            if (!minorUpdate) {
                doc.setDescriptionLastModifier(getDescriptionChangerId(changedBy))
            }

            updateStatement = connection.prepareStatement(UPDATE_DOCUMENT)
            rigUpdateStatement(updateStatement, doc, modTime, changedIn, changedBy, collection, deleted)
            updateStatement.execute()

            saveVersion(doc, connection, createdTime, modTime, changedIn, changedBy, collection, deleted)
            refreshDerivativeTables(doc, connection, deleted)
            updateMinMaxDepModified(doc.getShortId(), connection)
            dependencyCache.invalidate(preUpdateDoc)
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
            connection.rollback()
            throw e
        } catch (CancelUpdateException ignored) {
            /* An exception the called lambda/closure can throw to cancel a record update. NOT an indication of an error. */
            connection.rollback()
        } catch (Exception e) {
            log.error("Failed to save document: ${e.message}. Rolling back.")
            connection.rollback()
            throw e
        } finally {
            close(resultSet, selectStatement, updateStatement, connection)
        }

        refreshDependers(doc.getShortId())

        return doc
    }

    void refreshDependers(String id) {
        Connection connection = getConnection()
        connection.setAutoCommit(false)
        List<Tuple2<String, String>> dependers = getDependers(id, connection)
        try {
            for (Tuple2<String, String> depender : dependers) {
                updateMinMaxDepModified((String) depender.get(0), connection)
                removeEmbellishedDocument((String) depender.get(0), connection)
            }
            connection.commit()
        }
        catch (Exception e) {
            connection.rollback()
            throw e
        }
        finally {
            connection.close()
        }
    }

    /**
     * Returns if the URIs pointing to 'doc' are acceptable for an update to 'pre_update_doc',
     * otherwise throws.
     */
    private void verifyDocumentIdRetention(Document preUpdateDoc, Document postUpdateDoc, Connection connection) {

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
            if ( getSystemIdByIri(id, connection) != null )
                throw new RuntimeException("An update of " + preUpdateDoc.getCompleteId() + " MUST NOT have URIs that are already in use for other records. The update contained an offending URI: " + id)
        }

        if (preUpdateDoc.getControlNumber() != postUpdateDoc.getControlNumber())
            throw new RuntimeException("An update of " + preUpdateDoc.getCompleteId() + " MUST NOT change the controlNumber of the record in question. Existing controlNumber: "
                    + preUpdateDoc.getControlNumber() + " The rejected new controlNumber: " + postUpdateDoc.getControlNumber())

        // We're ok.
    }

    void refreshDerivativeTables(Document doc) {
        refreshDerivativeTables(doc, getConnection(), doc.deleted)
    }

    void refreshDerivativeTables(Document doc, Connection connection, boolean deleted) {
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
    List<String[]> calculateDependenciesSystemIDs(Document doc) {
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
            PreparedStatement getSystemId = null
            ResultSet rs = null
            try {
                getSystemId = connection.prepareStatement(GET_SYSTEMID_BY_IRI)
                getSystemId.setString(1, iri)

                rs = getSystemId.executeQuery()
                if (rs.next()) {

                    if (rs.getBoolean(2)) // If deleted==true, then doc refers to a deleted document which is not ok.
                        throw new LinkValidationException("Record supposedly depends on deleted record: ${rs.getString(1)}, which is not allowed.")

                    if (rs.getString(1) != doc.getShortId()) // Exclude A -> A (self-references)
                        dependencies.add([relation, rs.getString(1)] as String[])
                }
            } finally {
                close(rs, getSystemId)
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
        } finally {
            close(removeDependencies)
        }

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
                close(insertDependencies)
            }
        }
    }

    private void updateMinMaxDepModified(String id, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(UPDATE_MINMAX_MODIFIED)
            preparedStatement.setString(1, id)
            preparedStatement.setString(2, id)
            preparedStatement.setString(3, id)
            preparedStatement.execute()
        }
        finally {
            close(rs, preparedStatement)
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

    private static PreparedStatement rigInsertStatement(PreparedStatement insert, Document doc, Date timestamp, String changedIn, String changedBy, String collection, boolean deleted) {
        insert.setString(1, doc.getShortId())
        insert.setObject(2, doc.dataAsString, OTHER)
        insert.setString(3, collection)
        insert.setString(4, changedIn)
        insert.setString(5, changedBy)
        insert.setString(6, doc.getChecksum())
        insert.setBoolean(7, deleted)
        insert.setTimestamp(8, new Timestamp(timestamp.getTime()))
        insert.setTimestamp(9, new Timestamp(timestamp.getTime()))
        insert.setTimestamp(10, new Timestamp(timestamp.getTime()))
        insert.setTimestamp(11, new Timestamp(timestamp.getTime()))
        return insert
    }

    private static void rigUpdateStatement(PreparedStatement update, Document doc, Date modTime, String changedIn, String changedBy, String collection, boolean deleted) {
        update.setObject(1, doc.dataAsString, OTHER)
        update.setString(2, collection)
        update.setString(3, changedIn)
        update.setString(4, changedBy)
        update.setString(5, doc.getChecksum())
        update.setBoolean(6, deleted)
        update.setTimestamp(7, new Timestamp(modTime.getTime()))
        update.setObject(8, doc.getShortId(), OTHER)
    }

    boolean saveVersion(Document doc, Connection connection, Date createdTime,
                        Date modTime, String changedIn, String changedBy,
                        String collection, boolean deleted) {
        if (versioning) {
            PreparedStatement insVersion = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
            try {
                log.debug("Trying to save a version of ${doc.getShortId() ?: ""} with checksum ${doc.getChecksum()}. Modified: $modTime")
                insVersion = rigVersionStatement(insVersion, doc, createdTime,
                                              modTime, changedIn, changedBy,
                                              collection, deleted)
                insVersion.executeUpdate()
                return true
            } catch (Exception e) {
                log.error("Failed to save document version: ${e.message}")
                throw e
            }
            finally {
                close(insVersion)
            }
        } else {
            return false
        }
    }

    private static PreparedStatement rigVersionStatement(PreparedStatement insvers,
                                                         Document doc, Date createdTime,
                                                         Date modTime, String changedIn,
                                                         String changedBy, String collection,
                                                         boolean deleted) {
        insvers.setString(1, doc.getShortId())
        insvers.setObject(2, doc.dataAsString, OTHER)
        insvers.setString(3, collection)
        insvers.setString(4, changedIn)
        insvers.setString(5, changedBy)
        insvers.setString(6, doc.getChecksum())
        insvers.setTimestamp(7, new Timestamp(createdTime.getTime()))
        insvers.setTimestamp(8, new Timestamp(modTime.getTime()))
        insvers.setBoolean(9, deleted)
        return insvers
    }

    boolean bulkStore(
            final List<Document> docs, String changedIn, String changedBy, String collection, boolean updateDepMinMax = true) {
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
                doc.setCreated(now)
                doc.setModified(now)
                doc.setDeleted(false)
                if (versioning) {
                    ver_batch = rigVersionStatement(ver_batch, doc, now, now, changedIn, changedBy, collection, false)
                    ver_batch.addBatch()
                }
                batch = rigInsertStatement(batch, doc, now, changedIn, changedBy, collection, false)
                batch.addBatch()
                refreshDerivativeTables(doc, connection, false)
                if (updateDepMinMax) {
                    for (Tuple2<String, String> depender : getDependers(doc.getShortId(), connection)) {
                        updateMinMaxDepModified((String) depender.get(0), connection)
                        removeEmbellishedDocument((String) depender.get(0), connection)
                    }
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
            close(batch, ver_batch, connection)
            log.trace("[bulkStore] Closed connection.")
        }
        return false
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
     * Get the corresponding record main URI for supplied identifier
     *
     * Supplied identifier can be either the document URI, the thing URI, or a
     * sameAs URI, BUT NOT A SYSTEM ID.
     *
     */
    String getRecordId(String id) {
        Connection connection = getConnection()
        try {
            return getRecordOrThingId(id, GET_RECORD_ID, connection)
        } finally {
            close(connection)
        }
    }

    String getRecordId(String id, Connection connection) {
        return getRecordOrThingId(id, GET_RECORD_ID, connection)
    }

    /**
     * Get the corresponding thing main ID for supplied identifier
     *
     * Supplied identifier can be either the document URI, the thing URI, or a
     * sameAs URI, BUT NOT A SYSTEM ID.
     *
     */
    String getThingId(String id) {
        Connection connection = getConnection()
        try {
            return getThingId(id, connection)
        } finally {
            close(connection)
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
            close(connection)
        }
    }

    String getMainId(String id, Connection connection) {
        return getRecordOrThingId(id, GET_MAIN_ID, connection)
    }

    private static String getRecordOrThingId(String id, String sql, Connection connection) {
        PreparedStatement selectstmt = null
        ResultSet rs = null
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
            close(rs, selectstmt)
        }
    }

    /**
     * Return ID type for identifier, if found.
     *
     */
    IdType getIdType(String id) {
        Connection connection = getConnection()
        PreparedStatement selectstmt = null
        ResultSet rs = null
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
            close(rs, selectstmt, connection)
        }
    }

    private static IdType determineIdType(int graphIndex, boolean isMainId) {
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
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(INSERT_EMBELLISHED_DOCUMENT)
            preparedStatement.setString(1, id)
            preparedStatement.setObject(2, mapper.writeValueAsString(embellishedDocument.data), OTHER)
            preparedStatement.execute()
        }
        catch (PSQLException e) {
            if (UNIQUE_VIOLATION == e.getSQLState()) {
                // Someone else cached the document before we could,
                // so we fail silently
            } else {
                throw e
            }
        }
        finally {
            close(rs, preparedStatement)
        }
    }

    private void removeEmbellishedDocument(String id, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(DELETE_EMBELLISHED_DOCUMENT)
            preparedStatement.setString(1, id)
            preparedStatement.execute()
        }
        finally {
            close(rs, preparedStatement)
        }
    }

    Document loadEmbellished(String id, JsonLd jsonld) {
        Connection connection = getConnection()
        try {
            return loadEmbellished(id, jsonld, connection)
        } finally {
            close(connection)
        }
    }

    /**
     * Loads the embellished version of the stored document 'id'.
     * If there isn't an embellished version cached already, one will be created
     * (lazy caching).
     */
    Document loadEmbellished(String id, JsonLd jsonld, Connection connection) {
        PreparedStatement selectStatement = null
        ResultSet resultSet = null

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
            close(resultSet, selectStatement)
        }
    }

    String getCollectionBySystemID(String id) {
        Connection connection = getConnection()
        try {
            return getCollectionBySystemID(id, connection)
        } finally {
            close(connection)
        }
    }

    String getCollectionBySystemID(String id, Connection connection) {
        PreparedStatement selectStatement = null
        ResultSet resultSet = null

        try {
            selectStatement = connection.prepareStatement(GET_COLLECTION_BY_SYSTEM_ID)
            selectStatement.setString(1, id)
            resultSet = selectStatement.executeQuery()

            if (resultSet.next()) {
                return resultSet.getString("collection")
            }
            return null
        }
        finally {
            close(resultSet, selectStatement)
        }
    }

    Document load(String id, Connection conn = null) {
        return load(id, null, conn)
    }

    Document load(String id, String version, Connection conn = null) {
        Document doc
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
        Connection connection = null
        PreparedStatement preparedStatement = null
        ResultSet rs = null
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
            close(rs, preparedStatement, connection)
        }
    }

    String getSystemIdByIri(String iri) {
        Connection connection = getConnection()
        try {
            return getSystemIdByIri(iri, connection)
        } finally {
            close(connection)
        }
    }

    String getSystemIdByIri(String iri, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(GET_SYSTEMID_BY_IRI)
            preparedStatement.setString(1, iri)
            rs = preparedStatement.executeQuery()
            if (rs.next())
                return rs.getString(1)
            return null
        }
        finally {
            close(rs, preparedStatement)
        }
    }

    String getThingMainIriBySystemId(String id) {
        Connection connection = null
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(GET_THING_MAIN_IRI_BY_SYSTEMID)
            preparedStatement.setString(1, id)
            rs = preparedStatement.executeQuery()
            if (rs.next())
                return rs.getString(1)
            throw new RuntimeException("No IRI found for system id $id")
        } finally {
            close(rs, preparedStatement, connection)
        }
    }

    Document getDocumentByIri(String iri) {
        Connection connection = getConnection()
        try {
            return getDocumentByIri(iri, connection)
        } finally {
            close(connection)
        }
    }

    Document getDocumentByIri(String iri, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(GET_DOCUMENT_BY_IRI)
            preparedStatement.setString(1, iri)
            rs = preparedStatement.executeQuery()
            if (rs.next())
                return assembleDocument(rs)
            return null
        }
        finally {
            close(rs, preparedStatement)
        }
    }

    String getHoldingForBibAndSigel(String bibThingUri, String libraryUri, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
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
            close(rs, preparedStatement)
        }
    }

    List<Tuple2<String, String>> getDependencies(String id) {
        Connection connection = getConnection()
        try {
            return getDependencyData(id, GET_DEPENDENCIES, connection)
        } finally {
            close(connection)
        }
    }

    List<Tuple2<String, String>> getDependers(String id) {
        Connection connection = getConnection()
        try {
            getDependers(id, connection)
        } finally {
            close(connection)
        }
    }

    List<Tuple2<String, String>> getDependers(String id, Connection connection) {
        return getDependencyData(id, GET_DEPENDERS, connection)
    }

    private static List<Tuple2<String, String>> getDependencyData(String id, String query, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(query)
            preparedStatement.setString(1, id)
            rs = preparedStatement.executeQuery()
            List<Tuple2<String, String>> dependencies = []
            while (rs.next()) {
                dependencies.add( new Tuple2<String, String>(rs.getString(1), rs.getString(2)) )
            }
            dependencies.sort { it.getFirst() }
            return dependencies
        }
        finally {
            close(rs, preparedStatement)
        }
    }

    List<String> getDependenciesOfType(String id, String typeOfRelation) {
        return getDependencyDataOfType(id, typeOfRelation, GET_DEPENDENCIES_OF_TYPE)
    }

    List<String> getDependersOfType(String id, String typeOfRelation) {
        return getDependencyDataOfType(id, typeOfRelation, GET_DEPENDERS_OF_TYPE)
    }

    Set<String> getDependenciesOfTypeByIri(String iri, String typeOfRelation) {
        return dependencyCache.getDependenciesOfType(iri, typeOfRelation)
    }

    Set<String> getDependersOfTypeByIri(String iri, String typeOfRelation) {
        return dependencyCache.getDependersOfType(iri, typeOfRelation)
    }

    private List<String> getDependencyDataOfType(String id, String typeOfRelation, String query) {
        Connection connection = null
        PreparedStatement preparedStatement = null
        ResultSet rs = null
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
            close(rs, preparedStatement, connection)
        }
    }

    /**
     * List all system IDs that match a given typed id and graph index
     * (for example: type:ISBN, value:1234, graphIndex:1 -> ksjndfkjwbr3k)
     * If type is passed as null, all types will match.
     */
    List<String> getSystemIDsByTypedID(String idType, String idValue, int graphIndex) {
        Connection connection = null
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            String query = "SELECT id FROM lddb WHERE deleted = false AND data#>'{@graph," + graphIndex + ",identifiedBy}' @> ?"
            connection = getConnection()
            preparedStatement = connection.prepareStatement(query)

            if (idType != null) {
                String escapedId = StringEscapeUtils.escapeJavaScript(idValue)
                preparedStatement.setObject(1, "[{\"@type\": \"" + idType + "\", \"value\": \"" + escapedId + "\"}]", OTHER)
            }
            else {
                String escapedId = StringEscapeUtils.escapeJavaScript(idValue)
                preparedStatement.setObject(1, "[{\"value\": \"" + escapedId + "\"}]", OTHER)
            }

            rs = preparedStatement.executeQuery()
            List<String> results = []
            while (rs.next()) {
                results.add( rs.getString(1) )
            }
            return results
        }
        finally {
            close(rs, preparedStatement, connection)
        }
    }

    String getSystemIdByThingId(String thingId) {
        Connection connection = null
        PreparedStatement preparedStatement = null
        ResultSet rs = null
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
            close(rs, preparedStatement, connection)
        }
    }

    String getProfileByLibraryUri(String libraryUri) {
        Connection connection = null
        PreparedStatement preparedStatement = null
        ResultSet rs = null
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
            close(rs, preparedStatement, connection)
        }
    }

    /**
     * Returns a list of holdings documents, for any of the passed thingIdentifiers
     */
    List<Document> getAttachedHoldings(List<String> thingIdentifiers, JsonLd jsonld) {
        // Build the query
        StringBuilder selectSQL = new StringBuilder("SELECT id FROM ")
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
        Connection connection = null
        ResultSet rs = null
        PreparedStatement preparedStatement = null
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
                String id = rs.getString("id")
                holdings.add(loadEmbellished(id, jsonld, connection))
            }
            return holdings
        }
        finally {
            close(rs, preparedStatement, connection)
        }
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
        PreparedStatement selectstmt = null
        ResultSet rs = null
        try {
            selectstmt = connection.prepareStatement(sql)
            log.trace("Prepared statement")
            for (items in parameters) {
                if (items.value instanceof String) {
                    selectstmt.setString((Integer) items.key, (String) items.value)
                }
                if (items.value instanceof Map || items.value instanceof List) {
                    selectstmt.setObject((Integer) items.key, mapper.writeValueAsString(items.value), OTHER)
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
            close(rs, selectstmt)
            if (shouldCloseConn) {
                connection.close()
            }
        }

        return doc
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
        PreparedStatement selectstmt = null
        ResultSet rs = null
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
            close(rs, selectstmt)
            if (shouldCloseConn) {
                connection.close()
                log.debug("[loadAllVersions] Closed connection.")
            }
        }
        return docList
    }

    private static Document assembleDocument(ResultSet rs) {
        Document doc = new Document(mapper.readValue(rs.getString("data"), Map))
        doc.setModified(new Date(rs.getTimestamp("modified").getTime()))
        doc.setDeleted(rs.getBoolean("deleted"))

        try {
            // FIXME better handling of null values
            doc.setCreated(new Date(rs.getTimestamp("created")?.getTime()))
        } catch (SQLException ignored) {
            log.trace("Resultset didn't have created. Probably a version request.")
        }
        
        return doc
    }

    @CompileStatic(SKIP)
    Iterable<Document> loadAll(String collection, boolean includeDeleted = false, Date since = null, Date until = null) {
        log.debug("Load all called with collection: $collection")
        return new Iterable<Document>() {
            Iterator<Document> iterator() {
                Connection connection = getConnection()
                connection.setAutoCommit(false)
                PreparedStatement loadAllStatement
                long untilTS = until?.getTime() ?: PGStatement.DATE_POSITIVE_INFINITY
                long sinceTS = since?.getTime() ?: 0L

                String sql
                if (collection) {
                    sql = LOAD_ALL_DOCUMENTS_BY_COLLECTION

                } else {
                    sql = LOAD_ALL_DOCUMENTS
                }
                if (!includeDeleted)
                    sql += " AND deleted = false"
                loadAllStatement = connection.prepareStatement(sql)
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
                    Document next() {
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
                    boolean hasNext() {
                        return more
                    }
                }
            }
        }
    }

    static Iterable<Document> iterateDocuments(ResultSet rs) {
        def conn = rs.statement.connection
        boolean more = rs.next() // rs starts at "-1"
        if (!more) {
            try {
                conn.commit()
                conn.setAutoCommit(true)
            } finally {
                conn.close()
            }
        }
        return new Iterable<Document>() {
            Iterator<Document> iterator() {
                return new Iterator<Document>() {
                    @Override
                    Document next() {
                        Document doc = assembleDocument(rs)
                        more = rs.next()
                        if (!more) {
                            try {
                                conn.commit()
                                conn.setAutoCommit(true)
                            } finally {
                                conn.close()
                            }
                        }
                        return doc
                    }

                    @Override
                    boolean hasNext() {
                        return more
                    }
                }
            }
        }
    }

    void remove(String identifier, String changedIn, String changedBy) {
        if (versioning) {
            if(!getDependers(identifier).isEmpty())
                throw new RuntimeException("Deleting depended upon records is not allowed.")

            log.debug("Marking document with ID ${identifier} as deleted.")
            try {
                storeAtomicUpdate(identifier, false, changedIn, changedBy,
                    { Document doc ->
                        doc.setDeleted(true)
                        // Add a tombstone marker (without removing anything) perhaps?
                    })
            } catch (Throwable e) {
                log.warn("Could not mark document with ID ${identifier} as deleted: ${e}")
                throw e
            }
        } else {
            throw new WhelkException(
                    "Actually deleting data from lddb is currently not supported")
        }

        // Clear out dependencies
        Connection connection = getConnection()
        PreparedStatement removeDependencies = null
        try {
            removeDependencies = connection.prepareStatement(DELETE_DEPENDENCIES)
            removeDependencies.setString(1, identifier)
            int numRemoved = removeDependencies.executeUpdate()
            log.debug("Removed $numRemoved dependencies for id ${identifier}")
        } finally {
            close(removeDependencies, connection)
        }
    }

    Map loadSettings(String key) {
        Connection connection = getConnection()
        PreparedStatement selectstmt = null
        ResultSet rs = null
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
            close(rs, selectstmt, connection)
        }

        return settings
    }

    void saveSettings(String key, final Map settings) {
        Connection connection = getConnection()
        PreparedStatement savestmt = null
        try {
            String serializedSettings = mapper.writeValueAsString(settings)
            log.debug("Saving settings for ${key}: $serializedSettings")
            savestmt = connection.prepareStatement(SAVE_SETTINGS)
            savestmt.setObject(1, serializedSettings, OTHER)
            savestmt.setString(2, key)
            savestmt.setString(3, key)
            savestmt.setObject(4, serializedSettings, OTHER)
            savestmt.executeUpdate()
        } finally {
            close(savestmt, connection)
        }
    }

    /**
     * Get a database connection.
     */
    Connection getConnection(){
        return connectionPool.getConnection()
    }

    List<Document> findByValue(String relation, String value, int limit,
                               int offset) {
        Connection connection = getConnection()
        PreparedStatement find = connection.prepareStatement(FIND_BY)

        find = rigFindByValueStatement(find, relation, value, limit, offset)

        try {
            return executeFindByQuery(find)
        } finally {
            close(find, connection)
        }
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

    private static List<Document> executeFindByQuery(PreparedStatement query) {
        log.debug("Executing find query: ${query}")

        ResultSet rs = query.executeQuery()

        List<Document> docs = []

        while (rs.next()) {
            docs << assembleDocument(rs)
        }

        return docs
    }

    private static int executeCountByQuery(PreparedStatement query) {
        log.debug("Executing count query: ${query}")

        ResultSet rs = query.executeQuery()

        int result = 0

        if (rs.next()) {
            result = rs.getInt('count')
        }

        return result
    }

    private static PreparedStatement rigFindByValueStatement(PreparedStatement find,
                                                             String relation,
                                                             String value,
                                                             int limit,
                                                             int offset) {
        List valueQuery = [[(relation): value]]
        List valuesQuery = [[(relation): [value]]]

        return rigFindByStatement(find, valueQuery, valuesQuery, limit, offset)
    }

    private static PreparedStatement rigCountByValueStatement(PreparedStatement find,
                                                              String relation,
                                                              String value) {
        List valueQuery = [[(relation): value]]
        List valuesQuery = [[(relation): [value]]]

        return rigCountByStatement(find, valueQuery, valuesQuery)
    }

    private static PreparedStatement rigFindByStatement(PreparedStatement find,
                                                        List firstCondition,
                                                        List secondCondition,
                                                        int limit,
                                                        int offset) {
      find.setObject(1, mapper.writeValueAsString(firstCondition),
                     OTHER)
      find.setObject(2, mapper.writeValueAsString(secondCondition),
                     OTHER)
      find.setInt(3, limit)
      find.setInt(4, offset)
      return find
    }

    private static PreparedStatement rigCountByStatement(PreparedStatement find,
                                                         List firstCondition,
                                                         List secondCondition) {
      find.setObject(1, mapper.writeValueAsString(firstCondition),
                     OTHER)
      find.setObject(2, mapper.writeValueAsString(secondCondition),
                     OTHER)
      return find
    }

    private String getDescriptionChangerId(String changedBy) {
        //FIXME(?): hardcoded
        // for historical reasons changedBy is the script URI for global changes
        if (changedBy.startsWith('https://libris.kb.se/sys/globalchanges/')) {
            return getDescriptionChangerId('SEK')
        }
        else if (isHttpUri(changedBy)) {
            return changedBy
        }
        else {
            return 'https://libris.kb.se/library/' + changedBy
        }
    }

    private static boolean isHttpUri(String s) {
        return s.startsWith('http://') || s.startsWith('https://')
    }

    private static void close(Object... resources = null) {
        if (resources != null) {
            for (def resource : resources) {
                try {
                    if (resource != null) {
                        if (resource instanceof Connection) {
                            resource.close()
                        }
                        if (resource instanceof Statement) {
                            resource.close()
                        }
                        if (resource instanceof ResultSet) {
                            resource.close()
                        }
                        if (resource instanceof Array) {
                            resource.free()
                        }
                    }
                } catch (Exception e) {
                    log.debug("Error closing $resource : $e")
                }
            }
        }
    }
}
