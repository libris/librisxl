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
import whelk.exception.LinkValidationException
import whelk.exception.MissingMainIriException
import whelk.exception.StorageCreateFailedException
import whelk.exception.TooHighEncodingLevelException
import whelk.exception.WhelkException
import whelk.exception.WhelkRuntimeException
import whelk.filter.LinkFinder
import whelk.util.DocumentUtil
import whelk.util.LegacyIntegrationTools

import javax.sql.DataSource
import java.sql.Array
import java.sql.BatchUpdateException
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Matcher
import java.util.regex.Pattern

import static groovy.transform.TypeCheckingMode.SKIP
import static java.sql.Types.OTHER

/**
 *  It is important to not grab more than one connection per request/thread to avoid connection related deadlocks.
 *  i.e. get/release connection should be done in the public methods. Connections should be reused in method calls
 *  within this class.
 *
 *  See also getOuterConnection() and createAdditionalConnectionPool() for clients that need to hold their own
 *  connection while calling into this class.
 */

@Log
@CompileStatic
class PostgreSQLComponent implements Storage {
    public static final String PROPERTY_SQL_URL = "sqlUrl"
    public static final String PROPERTY_SQL_MAX_POOL_SIZE = "sqlMaxPoolSize"

    public static final ObjectMapper mapper = new ObjectMapper()

    private static final int DEFAULT_MAX_POOL_SIZE = 16
    private static final String driverClass = "org.postgresql.Driver"

    // SQL statements
    private static final String UPDATE_DOCUMENT = """
            UPDATE lddb 
            SET data = ?, collection = ?, changedIn = ?, changedBy = ?, checksum = ?, deleted = ?, modified = ? 
            WHERE id = ?
            """.stripIndent()

    private static final String INSERT_DOCUMENT = """
            INSERT INTO lddb (id, data, collection, changedIn, changedBy, checksum, deleted, created, modified)
            VALUES (?,?,?,?,?,?,?,?,?)
            """.stripIndent()

    private static final String INSERT_DOCUMENT_VERSION = """
            INSERT INTO lddb__versions (id, data, collection, changedIn, changedBy, checksum, created, modified, deleted)
            SELECT ?,?,?,?,?,?,?,?,?
            """.stripIndent()

    private static final String GET_DOCUMENT =
            "SELECT id, data, created, modified, deleted FROM lddb WHERE id = ?"

    private static final String GET_DOCUMENT_BY_MAIN_ID = """
            SELECT id, data, created, modified, deleted 
            FROM lddb 
            WHERE id = (SELECT id FROM lddb__identifiers WHERE mainid = 't' AND iri = ?)
            """.stripIndent()

    private static final String GET_DOCUMENT_BY_IRI = """
            SELECT lddb.id, lddb.data, lddb.created, lddb.modified, lddb.deleted 
            FROM lddb INNER JOIN lddb__identifiers ON lddb.id = lddb__identifiers.id
            WHERE lddb__identifiers.iri = ?
            """.stripIndent()

    private static final String GET_DOCUMENT_FOR_UPDATE =
            "SELECT id, data, collection, created, modified, deleted, changedBy FROM lddb WHERE id = ? FOR UPDATE"

    private static final String GET_DOCUMENT_VERSION =
            "SELECT id, data FROM lddb__versions WHERE id = ? AND checksum = ?"

    private static final String GET_DOCUMENT_VERSION_BY_MAIN_ID = """
            SELECT id, data 
            FROM lddb__versions 
            WHERE id = (SELECT id FROM lddb__identifiers WHERE iri = ? AND mainid = 't') 
            AND checksum = ?
            """.stripIndent()

    private static final String GET_ALL_DOCUMENT_VERSIONS = """
            SELECT id, data, deleted, created, modified 
            FROM lddb__versions
            WHERE id = ? 
            ORDER BY modified DESC
            """.stripIndent()

    private static final String GET_ALL_DOCUMENT_VERSIONS_BY_MAIN_ID = """
            SELECT id, data, deleted, created, modified 
            FROM lddb__versions 
            WHERE id = (SELECT id FROM lddb__identifiers WHERE iri = ? AND mainid = 't')
            ORDER BY modified
            """.stripIndent()

    private static final String LOAD_ALL_DOCUMENTS =
            "SELECT id, data, created, modified, deleted FROM lddb WHERE modified >= ? AND modified <= ?"

    private static final String LOAD_ALL_DOCUMENTS_BY_COLLECTION = """
            SELECT id, data, created, modified, deleted
            FROM lddb 
            WHERE modified >= ? AND modified <= ? AND collection = ? AND deleted = false
            """.stripIndent()

    private static final String STATUS_OF_DOCUMENT = """
            SELECT t1.id AS id, created, modified, deleted 
            FROM lddb t1 
            JOIN lddb__identifiers t2 ON t1.id = t2.id WHERE t2.iri = ?
            """.stripIndent()

    private static final String DELETE_DEPENDENCIES =
            "DELETE FROM lddb__dependencies WHERE id = ?"

    private static final String INSERT_DEPENDENCIES =
            "INSERT INTO lddb__dependencies (id, relation, dependsOnId) VALUES (?, ?, ?)"

    private static final String FOLLOW_DEPENDENCIES = """
            WITH RECURSIVE deps(i) AS (  
                    VALUES (?, null)  
                UNION
                    SELECT d.dependsonid, d.relation 
                    FROM lddb__dependencies d 
                    INNER JOIN deps deps1 ON d.id = i AND d.relation NOT IN (€)
            ) SELECT * FROM deps
            """.stripIndent()

    private static final String FOLLOW_DEPENDERS = """
            WITH RECURSIVE deps(i) AS (  
                    VALUES (?, null) 
                UNION  
                    SELECT d.id, d.relation 
                    FROM lddb__dependencies d
                    INNER JOIN deps deps1 ON d.dependsonid = i AND d.relation NOT IN (€) 
            ) SELECT * FROM deps
            """.stripIndent()

    private static final String GET_INCOMING_LINK_COUNT =
            "SELECT COUNT(id) FROM lddb__dependencies WHERE dependsOnId = ?"

    private static final String GET_DEPENDERS_OF_TYPE =
            "SELECT id FROM lddb__dependencies WHERE dependsOnId = ? AND relation = ?"

    private static final String GET_DEPENDENCIES_OF_TYPE =
            "SELECT dependsOnId FROM lddb__dependencies WHERE id = ? AND relation = ?"

    private static final String UPSERT_CARD = """
            INSERT INTO lddb__cards (id, data, checksum, changed)
            VALUES (?, ?, ?, ?) 
            ON CONFLICT (id) DO UPDATE 
            SET (data, checksum, changed) = (EXCLUDED.data, EXCLUDED.checksum, EXCLUDED.changed) 
            WHERE lddb__cards.checksum != EXCLUDED.checksum
            """.stripIndent()

    private static final String UPDATE_CARD =
            "UPDATE lddb__cards SET (data, checksum, changed) = (?, ?, ?) WHERE id = ? AND checksum != ?"

    private static final String GET_CARD =
            "SELECT data FROM lddb__cards WHERE ID = ?"

    private static final String BULK_LOAD_CARDS =
            "SELECT in_id as id, data from unnest(?) as in_id LEFT JOIN lddb__cards c ON in_id = c.id"

    private static final String DELETE_CARD =
            "DELETE FROM lddb__cards WHERE ID = ?"

    private static final String CARD_EXISTS =
            "SELECT EXISTS(SELECT 1 from lddb__cards where id = ?)"

    private static final String IS_CARD_CHANGED =
            "SELECT card.changed >= doc.modified FROM lddb__cards card, lddb doc WHERE doc.id = card.id AND doc.id = ?"

    private static final String INSERT_IDENTIFIERS =
            "INSERT INTO lddb__identifiers (id, iri, graphIndex, mainId) VALUES (?, ?, ?, ?)"

    private static final String DELETE_IDENTIFIERS =
            "DELETE FROM lddb__identifiers WHERE id = ?"

    private static final String GET_RECORD_ID_BY_THING_ID =
            "SELECT id FROM lddb__identifiers WHERE iri = ? AND graphIndex = 1"

    private static final String GET_RECORD_ID = """
            SELECT iri 
            FROM lddb__identifiers 
            WHERE graphindex = 0 AND mainid = 't' AND id = (SELECT id FROM lddb__identifiers WHERE iri = ?)
            """.stripIndent()

    private static final String GET_THING_ID = """
            SELECT iri 
            FROM lddb__identifiers 
            WHERE graphindex = 1 AND mainid = 't' AND id = (SELECT id FROM lddb__identifiers WHERE iri = ?)
            """.stripIndent()

    private static final String GET_MAIN_ID = """
            SELECT t2.iri
            FROM lddb__identifiers t1
            JOIN lddb__identifiers t2 ON t2.id = t1.id AND t2.graphindex = t1.graphindex
            WHERE t1.iri = ? AND t2.mainid = true
            """.stripIndent()

    private static final String GET_SYSTEMID_BY_IRI = """
            SELECT lddb__identifiers.id, lddb.deleted
            FROM lddb__identifiers 
            JOIN lddb ON lddb__identifiers.id = lddb.id WHERE lddb__identifiers.iri = ? 
            """.stripIndent()

    private static final String GET_SYSTEMIDS_BY_IRIS = """
            SELECT lddb__identifiers.iri, lddb__identifiers.id, lddb.deleted
            FROM lddb__identifiers, lddb, unnest(?) as in_iri
            WHERE lddb__identifiers.iri = in_iri
            AND lddb.id = lddb__identifiers.id
            """.stripIndent()

    private static final String GET_THING_MAIN_IRI_BY_SYSTEMID =
            "SELECT iri FROM lddb__identifiers WHERE graphindex = 1 and mainid is true and id = ?"

    private static final String GET_ID_TYPE =
            "SELECT graphindex, mainid FROM lddb__identifiers WHERE iri = ?"

    private static final String GET_COLLECTION_BY_SYSTEM_ID =
            "SELECT collection FROM lddb where id = ?"

    /** This query does the same as LOAD_COLLECTIONS = "SELECT DISTINCT collection FROM lddb"
        but much faster because postgres does not yet have 'loose indexscan' aka 'index skip scan'
        https://wiki.postgresql.org/wiki/Loose_indexscan' */
    private static final String LOAD_COLLECTIONS = """
            WITH RECURSIVE t AS (
                    (SELECT collection FROM lddb ORDER BY collection LIMIT 1) 
                UNION ALL 
                    SELECT (SELECT collection FROM lddb WHERE collection > t.collection ORDER BY collection LIMIT 1) 
                    FROM t WHERE t.collection IS NOT NULL
            ) SELECT collection FROM t WHERE collection IS NOT NULL
            """.stripIndent()

    private static final String GET_CONTEXT = """
            SELECT data 
            FROM lddb 
            WHERE id IN (SELECT id FROM lddb__identifiers WHERE iri = 'https://id.kb.se/vocab/context')
            """.stripIndent()

    private static final String GET_LEGACY_PROFILE =
            "SELECT profile FROM lddb__profiles WHERE library_id = ?"

    private HikariDataSource connectionPool
    private HikariDataSource outerConnectionPool

    boolean versioning = true
    boolean doVerifyDocumentIdRetention = true

    LinkFinder linkFinder
    DependencyCache dependencyCache
    JsonLd jsonld

    private AtomicLong cardsUpdated = new AtomicLong()

    class AcquireLockException extends RuntimeException { AcquireLockException(String s) { super(s) } }

    class ConflictingHoldException extends RuntimeException { ConflictingHoldException(String s) { super(s) } }

    // for testing
    PostgreSQLComponent() {}

    PostgreSQLComponent(Properties properties) {
        int maxPoolSize = properties.getProperty(PROPERTY_SQL_MAX_POOL_SIZE)
                ? Integer.parseInt(properties.getProperty(PROPERTY_SQL_MAX_POOL_SIZE))
                : DEFAULT_MAX_POOL_SIZE

        setup(properties.getProperty(PROPERTY_SQL_URL), maxPoolSize)
    }

    PostgreSQLComponent(String sqlUrl) {
        setup(sqlUrl, DEFAULT_MAX_POOL_SIZE)
    }

    private void setup(String sqlUrl, int maxPoolSize) {

        if (sqlUrl) {
            HikariConfig config = new HikariConfig()
            config.setMaximumPoolSize(maxPoolSize)
            config.setAutoCommit(true)
            config.setJdbcUrl(sqlUrl.replaceAll(":\\/\\/\\w+:*.*@", ":\\/\\/"))
            config.setDriverClassName(driverClass)
            config.setConnectionTimeout(0)

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
    }

    void logStats() {
        log.info("Cards created or changed: $cardsUpdated")
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

    int getPoolSize() {
        return connectionPool?.getMaximumPoolSize()
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

        Connection connection = getConnection()
        connection.setAutoCommit(false)

        /*
        If we're writing a holding post, obtain a (write) lock on the linked bibpost, and hold it until writing has finished.
        While under lock: first check that there is not already a holding for this sigel/bib-id combination.
         */
        try {
            normalizeDocumentForStorage(doc, connection)

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
            if (psqle.serverErrorMessage?.message?.startsWith("duplicate key value violates unique constraint")) {
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

    /**
     * This is a variant of createDocument that does no or minimal denormalization or indexing.
     * It should NOT be used to create records in a production environment. Its intended purpose is
     * to be used when copying data from one xl environment to another.
     */
    boolean quickCreateDocument(Document doc, String changedIn, String changedBy, String collection) {
        Connection connection = getConnection()
        connection.setAutoCommit(false)
        try {
            Date now = new Date()
            PreparedStatement insert = connection.prepareStatement(INSERT_DOCUMENT)
            insert = rigInsertStatement(insert, doc, now, changedIn, changedBy, collection, false)
            insert.executeUpdate()

            saveVersion(doc, connection, now, now, changedIn, changedBy, collection, false)
            refreshDerivativeTables(doc, connection, false)

            connection.commit()
            return true
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

    /*
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
            normalizeDocumentForStorage(doc, connection)

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
            dependencyCache.invalidate(preUpdateDoc)
            connection.commit()
            log.debug("Saved document ${doc.getShortId()} with timestamps ${doc.created} / ${doc.modified}")
        } catch (PSQLException psqle) {
            log.error("SQL failed: ${psqle.message}")
            connection.rollback()
            if (psqle.serverErrorMessage?.message?.startsWith("duplicate key value violates unique constraint")) {
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

        return doc
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

        if (jsonld) {
            if (deleted) {
                deleteCard(doc, connection)
            } else {
                updateCard(new CardEntry(doc), connection)
            }
        }
    }

    /**
     * Rewrite card. To be used when card definitions have changed
     */
    void refreshCardData(Document doc, Instant timestamp) {
        getConnection().withCloseable { connection ->
            if (hasCard(doc.shortId, connection)) {
                updateCard(new CardEntry(doc, timestamp), connection)
            }
        }
    }

    private List<String[]> _calculateDependenciesSystemIDs(Document doc, Connection connection) {
        List<String[]> dependencies = []

        Map iriToRelation = doc.getExternalRefs()
            .findAll{ it.iri.startsWith("http") }
            .collectEntries {  [ it.iri, it.relation ] }

        getSystemIds(iriToRelation.keySet(), connection) { String iri, String systemId, boolean deleted ->
            if (deleted) // doc refers to a deleted document which is not ok.
                throw new LinkValidationException("Record supposedly depends on deleted record: ${systemId}, which is not allowed.")

            if (systemId != doc.getShortId()) // Exclude A -> A (self-references)
                dependencies.add([iriToRelation[iri], systemId] as String[])
        }

        return dependencies
    }

    Map<String, String> getSystemIdsByIris (Iterable iris) {
        Map<String, String> ids = new HashMap<>()
        getConnection().withCloseable { connection ->
            getSystemIds(iris, connection) { String iri, String systemId, deleted ->
                ids.put(iri, systemId)
            }
        }
        return ids
    }

    private void getSystemIds(Iterable iris, Connection connection, Closure c) {
        PreparedStatement getSystemIds = null
        ResultSet rs = null
        try {
            getSystemIds = connection.prepareStatement(GET_SYSTEMIDS_BY_IRIS)
            getSystemIds.setArray(1, connection.createArrayOf("TEXT", iris as String[]))

            rs = getSystemIds.executeQuery()
            while (rs.next()) {
                String iri = rs.getString(1)
                String systemId = rs.getString(2)
                boolean deleted = rs.getBoolean(3)

                c(iri, systemId, deleted)
            }
        } finally {
            close(rs, getSystemIds)
        }
    }

    Map getCard(String iri) {
        String systemId = getSystemIdByIri(iri)
        return loadCard(systemId) ?: makeCardData(systemId)
    }

    Iterable<Map> getCards(Iterable<String> iris) {
        return createAndAddMissingCards(bulkLoadCards(getSystemIdsByIris(iris).values())).values()
    }

    /**
     * Check if a card has changed
     *
     * @param systemId
     * @return true if the card changed after or at the same time as the document was modified
     */
    boolean isCardChanged(String systemId) {
        Connection connection = getConnection()
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(IS_CARD_CHANGED)
            preparedStatement.setString(1, systemId)

            rs = preparedStatement.executeQuery()

            return rs.next() && rs.getBoolean(1)
        } finally {
            close(rs, preparedStatement, connection)
        }
    }

    protected boolean storeCard(CardEntry cardEntry) {
        return getConnection().withCloseable { connection -> storeCard(cardEntry, connection)}
    }

    protected boolean storeCard(CardEntry cardEntry, Connection connection) {
        Document card = cardEntry.getCard()
        Timestamp timestamp = new Timestamp(cardEntry.getChangedTimestamp().toEpochMilli())

        PreparedStatement preparedStatement = null
        try {
            preparedStatement = connection.prepareStatement(UPSERT_CARD)
            preparedStatement.setString(1, card.getShortId())
            preparedStatement.setObject(2, card.dataAsString, OTHER)
            preparedStatement.setString(3, card.getChecksum())
            preparedStatement.setTimestamp(4, timestamp)

            boolean createdOrUpdated = preparedStatement.executeUpdate() > 0

            if (createdOrUpdated) {
                cardsUpdated.incrementAndGet()
            }

            return createdOrUpdated
        }
        finally {
            close(preparedStatement)
        }
    }

    protected boolean updateCard(CardEntry cardEntry, Connection connection) {
        Document card = cardEntry.getCard()
        Timestamp timestamp = new Timestamp(cardEntry.getChangedTimestamp().toEpochMilli())

        PreparedStatement preparedStatement = null
        try {
            String checksum = card.getChecksum()
            preparedStatement = connection.prepareStatement(UPDATE_CARD)
            preparedStatement.setObject(1, card.dataAsString, OTHER)
            preparedStatement.setString(2, checksum)
            preparedStatement.setTimestamp(3, timestamp)
            preparedStatement.setString(4, card.getShortId())
            preparedStatement.setString(5, checksum)

            boolean updated = preparedStatement.executeUpdate() > 0

            if (updated) {
                cardsUpdated.incrementAndGet()
            }

            return updated
        }
        finally {
            close(preparedStatement)
        }
    }

    protected void deleteCard(Document doc, Connection connection) {
        String systemId = getSystemIdByIri(doc.getThingIdentifiers().first())
        PreparedStatement preparedStatement = null
        try {
            preparedStatement = connection.prepareStatement(DELETE_CARD)
            preparedStatement.setString(1,systemId)

            preparedStatement.executeUpdate()
        }
        finally {
            close(preparedStatement)
        }
    }

    protected Map loadCard(String id) {
        Connection connection = getConnection()
        try {
            return loadCard(id, connection)
        } finally {
            close(connection)
        }
    }

    protected Map<String, Map> bulkLoadCards(Iterable<String> ids) {
        Connection connection = getConnection()
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(BULK_LOAD_CARDS)
            preparedStatement.setArray(1,  connection.createArrayOf("TEXT", ids as String[]))

            rs = preparedStatement.executeQuery()
            SortedMap<String, Map> result = new TreeMap<>()
            while(rs.next()) {
                String card = rs.getString("data")
                result[rs.getString("id")] = card != null ? mapper.readValue(card, Map) : null
            }
            return result
        } finally {
            close(rs, preparedStatement, connection)
        }
    }

    protected boolean hasCard(String id, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(CARD_EXISTS)
            preparedStatement.setString(1, id)

            rs = preparedStatement.executeQuery()
            rs.next()
            return rs.getBoolean(1)
        }
        finally {
            close(rs, preparedStatement)
        }
    }

    protected Map loadCard(String id, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(GET_CARD)
            preparedStatement.setString(1, id)

            rs = preparedStatement.executeQuery()
            if (rs.next()) {
                return mapper.readValue(rs.getString("data"), Map)
            }
            else {
                return null
            }
        }
        finally {
            close(rs, preparedStatement)
        }
    }

    protected Map<String, Map> createAndAddMissingCards(Map<String, Map> cards) {
        Map <String, Map> result = [:]
        cards.each { id, card ->
            result[id] = card ?: makeCardData(id)
        }
        return result
    }

    private Map makeCardData(String systemId) {
        Document doc = load(systemId)
        if (!doc) {
            throw new WhelkException("Could not find document with id " + systemId)
        }

        CardEntry cardEntry = new CardEntry(doc, Instant.now())
        storeCard(cardEntry)
        return cardEntry.getCard().data
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

    boolean bulkStore(final List<Document> docs, String changedIn, String changedBy, String collection) {
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
                doc.normalizeUnicode()
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
        try {
            connection = getConnection()
            return getThingMainIriBySystemId(id, connection)
        } finally {
            close(connection)
        }
    }

    String getThingMainIriBySystemId(String id, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(GET_THING_MAIN_IRI_BY_SYSTEMID)
            preparedStatement.setString(1, id)
            rs = preparedStatement.executeQuery()
            if (rs.next())
                return rs.getString(1)
            throw new MissingMainIriException("No IRI found for system id $id")
        } finally {
            close(rs, preparedStatement)
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
            String sql = "SELECT id FROM lddb WHERE data#>>'{@graph, 1, itemOf, @id}' = ? AND data#>>'{@graph, 1, heldBy, @id}' = ? AND deleted = false"
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

    List<Tuple2<String, String>> followDependencies(String id, List<String> excludeRelations = []) {
        Connection connection = getConnection()
        try {
            return followDependencies(id, connection, excludeRelations)
        } finally {
            close(connection)
        }
    }

    List<Tuple2<String, String>> followDependencies(String id, Connection connection, List<String> excludeRelations = []) {
        return followDependencyData(id, FOLLOW_DEPENDENCIES, connection, excludeRelations)
    }

    List<Tuple2<String, String>> followDependers(String id, List<String> excludeRelations = []) {
        Connection connection = getConnection()
        try {
            followDependers(id, connection, excludeRelations)
        } finally {
            close(connection)
        }
    }

    List<Tuple2<String, String>> followDependers(String id, Connection connection, List<String> excludeRelations = []) {
        return followDependencyData(id, FOLLOW_DEPENDERS, connection, excludeRelations)
    }

    private static List<Tuple2<String, String>> followDependencyData(String id, String query, Connection connection,
                                                                     List<String> excludeRelations) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {

            String replacement = "'" + excludeRelations.join("', '") + "'"
            query = query.replace("€", replacement)
            preparedStatement = connection.prepareStatement(query)
            preparedStatement.setString(1, id)
            rs = preparedStatement.executeQuery()
            List<Tuple2<String, String>> dependencies = []
            while (rs.next()) {
                if (rs.getString(2) != null) // The first tuple will be (root, null), which we dont need in the result.
                    dependencies.add( new Tuple2<String, String>(rs.getString(1), rs.getString(2)) )
            }
            dependencies.sort { it.getFirst() }
            return dependencies
        }
        finally {
            close(rs, preparedStatement)
        }
    }

    List<String> getDependenciesOfType(String id, String relation) {
        return getDependencyDataOfType(id, relation, GET_DEPENDENCIES_OF_TYPE)
    }

    List<String> getDependersOfType(String id, String relation) {
        return getDependencyDataOfType(id, relation, GET_DEPENDERS_OF_TYPE)
    }

    Set<String> getByRelation(String iri, String relation) {
        return dependencyCache.getDependenciesOfType(iri, relation)
    }

    Set<String> getByReverseRelation(String iri, String relation) {
        return dependencyCache.getDependersOfType(iri, relation)
    }

    long getIncomingLinkCount(String id) {
        Connection connection = getConnection()
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(GET_INCOMING_LINK_COUNT)
            preparedStatement.setString(1, id)
            rs = preparedStatement.executeQuery()
            rs.next()
            return rs.getInt(1)
        } finally {
            close(rs, preparedStatement, connection)
        }
    }

    private static SortedSet<String> getDependencyData(String id, String query, Connection connection) {
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            preparedStatement = connection.prepareStatement(query)
            preparedStatement.setString(1, id)
            rs = preparedStatement.executeQuery()
            SortedSet<String> dependencies = new TreeSet<>()
            while (rs.next()) {
                dependencies.add( rs.getString(1) )
            }
            return dependencies
        }
        finally {
            close(rs, preparedStatement)
        }
    }

    private List<String> getDependencyDataOfType(String id, String relation, String query) {
        Connection connection = null
        PreparedStatement preparedStatement = null
        ResultSet rs = null
        try {
            connection = getConnection()
            preparedStatement = connection.prepareStatement(query)
            preparedStatement.setString(1, id)
            preparedStatement.setString(2, relation)
            rs = preparedStatement.executeQuery()
            List<String> dependencies = []
            while (rs.next()) {
                dependencies.add( rs.getString(1) )
            }
            return dependencies
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
     * Returns a list of holdings document ids, for any of the passed thingIdentifiers
     */
    List<String> getAttachedHoldings(List<String> thingIdentifiers) {
        // Build the query
        StringBuilder selectSQL = new StringBuilder(
                "SELECT id FROM lddb WHERE collection = 'hold' AND deleted = false AND ("
        )
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
            List<String> holdings = []
            while (rs.next()) {
                holdings.add(rs.getString("id"))
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

    private void normalizeDocumentForStorage(Document doc, Connection connection) {
        // Synthetic property, should never be stored
        DocumentUtil.findKey(doc.data, JsonLd.REVERSE_KEY) { value, path ->
            new DocumentUtil.Remove()
        }

        doc.normalizeUnicode()

        if (linkFinder != null) {
            linkFinder.normalizeIdentifiers(doc, connection)
        }
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
            if(!followDependers(identifier).isEmpty())
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

    /**
     * Get a database connection.
     */
    Connection getConnection(){
        return connectionPool.getConnection()
    }

    /**
     * Get a database connection that is safe to keep open while calling into this class.
     */
    Connection getOuterConnection() {
        return getOuterPool().getConnection()
    }

    private DataSource getOuterPool() {
        if (!outerConnectionPool) {
            synchronized (this) {
                if (!outerConnectionPool) {
                    outerConnectionPool = (HikariDataSource) createAdditionalConnectionPool("OuterPool")
                }
            }
        }
        return outerConnectionPool
    }

    DataSource createAdditionalConnectionPool(String name, int size = this.getPoolSize()) {
        HikariConfig config = new HikariConfig()
        connectionPool.copyStateTo(config)
        config.setPoolName(name)
        config.setMaximumPoolSize(size)
        HikariDataSource pool = new HikariDataSource(config)
        log.info("Created additional connection pool: ${pool.getPoolName()}, max size:${pool.getMaximumPoolSize()}")
        return pool
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
            return LegacyIntegrationTools.legacySigelToUri(changedBy)
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

    private class CardEntry {
        Document card
        Instant changedTimestamp

        CardEntry(Document doc, Instant changedTimestamp = null) {
            if (!jsonld) {
                throw new WhelkRuntimeException("jsonld not set")
            }

            this.card = new Document(jsonld.toCard(doc.data, false))
            this.changedTimestamp = changedTimestamp ?: doc.getModifiedTimestamp()
        }
    }
}
