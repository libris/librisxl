package whelk.component

import groovy.util.logging.Slf4j as Log

import org.apache.commons.dbcp2.BasicDataSource
import org.codehaus.jackson.map.ObjectMapper
import org.postgresql.PGStatement
import whelk.Document
import whelk.JsonLd
import whelk.Location
import whelk.exception.WhelkException

import javax.print.Doc
import java.security.MessageDigest
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.PreparedStatement
import java.sql.Connection
import java.sql.Types

@Log
class PostgreSQLComponent implements Storage {

    private BasicDataSource connectionPool
    static String driverClass = "org.postgresql.Driver"

    public final static mapper = new ObjectMapper()

    private final static LOCATION_PRECURSOR = "/resource"

    boolean versioning = true

    // SQL statements
    protected String UPSERT_DOCUMENT, INSERT_DOCUMENT_VERSION, GET_DOCUMENT, GET_DOCUMENT_VERSION, GET_ALL_DOCUMENT_VERSIONS, GET_DOCUMENT_BY_ALTERNATE_ID, LOAD_ALL_DOCUMENTS, LOAD_ALL_DOCUMENTS_WITH_LINKS, LOAD_ALL_DOCUMENTS_WITH_LINKS_BY_DATASET, LOAD_ALL_DOCUMENTS_BY_DATASET, DELETE_DOCUMENT_STATEMENT, STATUS_OF_DOCUMENT, LOAD_ID_FROM_ALTERNATE
    protected String LOAD_SETTINGS, SAVE_SETTINGS
    protected String QUERY_LD_API

    // Query defaults
    static final int DEFAULT_PAGE_SIZE=50

    // Query idiomatic data


    static final Map<StorageType, String> SQL_PREFIXES = [
            (StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS): "data->'descriptions'",
            (StorageType.MARC21_JSON): "data->'fields'"
    ]

    PostgreSQLComponent(String sqlUrl, String sqlMaintable) {
        String mainTableName = sqlMaintable
        String versionsTableName = mainTableName + "__versions"
        String settingsTableName = mainTableName + "__settings"


        connectionPool = new BasicDataSource();

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
        UPSERT_DOCUMENT = "WITH upsert AS (UPDATE $mainTableName SET data = ?, quoted = ?, manifest = ?, deleted = ?, modified = ? WHERE id = ? " +
                "OR manifest @> ? RETURNING *) " +
            "INSERT INTO $mainTableName (id, data, quoted, manifest, deleted) SELECT ?,?,?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)"

        INSERT_DOCUMENT_VERSION = "INSERT INTO $versionsTableName (id, data, manifest, checksum, modified) SELECT ?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM $versionsTableName WHERE id = ? ORDER BY modified DESC LIMIT 1) AS last WHERE last.checksum = ?)"// (SELECT 1 FROM $versionsTableName WHERE id = ? AND checksum = ?)" +

        GET_DOCUMENT = "SELECT id,data,manifest,created,modified,deleted FROM $mainTableName WHERE id= ?"
        GET_DOCUMENT_VERSION = "SELECT id,data,manifest FROM $versionsTableName WHERE id = ? AND checksum = ?"
        GET_ALL_DOCUMENT_VERSIONS = "SELECT id,data,manifest,manifest->>'created' AS created,modified,manifest->>'deleted' AS deleted FROM $versionsTableName WHERE id = ? ORDER BY modified"
        GET_DOCUMENT_BY_ALTERNATE_ID = "SELECT id,data,manifest,created,modified,deleted FROM $mainTableName WHERE manifest @> '{ \"${Document.ALTERNATE_ID_KEY}\": [?] }'"
        LOAD_ID_FROM_ALTERNATE = "SELECT id FROM $mainTableName WHERE manifest->'${Document.ALTERNATE_ID_KEY}' @> ?"
        LOAD_ALL_DOCUMENTS = "SELECT id,data,manifest,created,modified,deleted FROM $mainTableName WHERE modified >= ? AND modified <= ?"
        LOAD_ALL_DOCUMENTS_BY_DATASET = "SELECT id,data,manifest,created,modified,deleted FROM $mainTableName WHERE modified >= ? AND modified <= ? AND manifest->>'dataset' = ?"
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

        // Queries
        QUERY_LD_API = "SELECT id,data,manifest,created,modified,deleted FROM $mainTableName WHERE deleted IS NOT TRUE AND "

        // SQL for settings management
        LOAD_SETTINGS = "SELECT key,settings FROM $settingsTableName where key = ?"
        SAVE_SETTINGS = "WITH upsertsettings AS (UPDATE $settingsTableName SET settings = ? WHERE key = ? RETURNING *) " +
                "INSERT INTO $settingsTableName (key, settings) SELECT ?,? WHERE NOT EXISTS (SELECT * FROM upsertsettings)"

    }



    public Map status(String identifier, Connection connection = null) {
        Map statusMap = [:]
        boolean newConnection = (connection == null)
        try {
            if (newConnection) {
                connection = getConnection()
            }
            PreparedStatement statusStmt = connection.prepareStatement(STATUS_OF_DOCUMENT)
            statusStmt.setString(1, identifier)
            def rs = statusStmt.executeQuery()
            if (rs.next()) {
                statusMap['exists'] = true
                statusMap['created'] = new Date(rs.getTimestamp("created").getTime())
                statusMap['modified'] = new Date(rs.getTimestamp("modified").getTime())
                statusMap['deleted'] = rs.getBoolean("deleted")
                log.trace("StatusMap: $statusMap")
            } else {
                log.debug("No results returned for $identifier")
                statusMap['exists'] = false
            }
        } finally {
            if (newConnection) {
                connection.close()
            }
        }
        log.debug("Loaded status for ${identifier}: $statusMap")
        return statusMap
    }

    @Override
    Document store(Document doc) {
        if (!doc.dataset) {
            log.error("Can't save document without dataset.")
            throw new WhelkException("Can't save document without dataset.")
        }
        log.debug("Saving ${doc.id}")
        Connection connection = getConnection()
        connection.setAutoCommit(false)
        try {
            calculateChecksum(doc)
            Date now = new Date()
            if (versioning) {
                log.debug("Document has checksum ${doc.checksum} before saving version.")
                if (!saveVersion(doc, connection, now)) {
                    return doc // Same document already in storage.
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
        insert.setObject(2, doc.quotedAsString, java.sql.Types.OTHER)
        insert.setObject(3, doc.manifestAsJson, java.sql.Types.OTHER)
        insert.setBoolean(4, doc.isDeleted())
        insert.setTimestamp(5, new Timestamp(modTime.getTime()))
        insert.setString(6, doc.identifier)
        insert.setObject(7, matchAlternateIdentifierJson(doc.id), java.sql.Types.OTHER)
        insert.setString(8, doc.identifier)
        insert.setObject(9, doc.dataAsString, java.sql.Types.OTHER)
        insert.setObject(10, doc.quotedAsString, java.sql.Types.OTHER)
        insert.setObject(11, doc.manifestAsJson, java.sql.Types.OTHER)
        insert.setBoolean(12, doc.isDeleted())

        return insert
    }

    private static String matchAlternateIdentifierJson(String id) {
        return mapper.writeValueAsString([(Document.ALTERNATE_ID_KEY): [id]])

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
    boolean bulkStore(final List<Document> docs) {
        if (!docs || docs.isEmpty()) {
            return
        }
        log.trace("Bulk storing ${docs.size()} documents.")
        Connection connection = getConnection()
        PreparedStatement batch = connection.prepareStatement(UPSERT_DOCUMENT)
        PreparedStatement ver_batch = connection.prepareStatement(INSERT_DOCUMENT_VERSION)
        try {
            docs.each { doc ->
                Date now = new Date()
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
            return true
        } catch (Exception e) {
            log.error("Failed to save batch: ${e.message}", e)
        } finally {
            connection.close()
            log.trace("[bulkStore] Closed connection.")
        }
        return false
    }

    @Override
    Map<String, Object> linkedDataApiQuery(Map queryParameters, String dataset, StorageType storageType) {
        log.debug("Performing query with type $storageType : $queryParameters")
        long startTime = System.currentTimeMillis()
        Connection connection = getConnection()
        // Extract LDAPI parameters
        String pageSize = queryParameters.remove("_pageSize")?.first() ?: ""+DEFAULT_PAGE_SIZE
        String page = queryParameters.remove("_page")?.first() ?: "1"
        String sort = queryParameters.remove("_sort")?.first()
        queryParameters.remove("_where") // Not supported
        queryParameters.remove("_orderBy") // Not supported
        queryParameters.remove("_select") // Not supported

        def (whereClause, values) = buildQueryString(queryParameters, dataset, storageType)

        int limit = pageSize as int
        int offset = (Integer.parseInt(page)-1) * limit

        StringBuilder finalQuery = new StringBuilder("${values ? QUERY_LD_API + whereClause : (dataset ? LOAD_ALL_DOCUMENTS_BY_DATASET : LOAD_ALL_DOCUMENTS)+ " AND deleted IS NOT true"} OFFSET $offset LIMIT $limit")

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
            if (dataset) {
                query.setString(3, dataset)
            }
        }

        ResultSet rs = query.executeQuery()
        Map results = new HashMap<String, Object>()
        List items= []
        while (rs.next()) {
            def manifest = mapper.readValue(rs.getString("manifest"), Map)
            Document doc = new Document(rs.getString("id"), mapper.readValue(rs.getString("data"), Map), manifest)
            doc.setCreated(rs.getTimestamp("created").getTime())
            doc.setModified(rs.getTimestamp("modified").getTime())
            log.trace("Created document with id ${doc.id}")
            items.add(doc.data)
        }
        results.put("startIndex", offset)
        results.put("itemsPerPage", (limit > items.size() ? items.size() : limit))
        results.put("duration", "PT"+(System.currentTimeMillis()-startTime)/1000+"S")
        results.put("items", items)
        return results
    }

    def buildQueryString(Map queryParameters, String dataset, StorageType storageType) {
        boolean firstKey = true
        List values = []

        StringBuilder whereClause = new StringBuilder("(")

        if (dataset) {
            whereClause.append("manifest->>'dataset' = ?")
            values.add(dataset)
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
                    jsonbPath.append(SQL_PREFIXES.get(storageType, "")+"->")
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
            jsonbPath.append(" "+direction)
        }
        println("orderstring: ${jsonbPath.toString()}")
        return jsonbPath.toString()

    }

    protected translateToSql(String key, String value, StorageType storageType) {
        println("key: $key")
        def keyElements = key.split("\\.")
        println("keyElements: $keyElements")
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
                for (int i = (keyElements[0] == "items" ? 1 : 0); i < keyElements.length-1; i++) {
                    nextMap = [:]
                    jsonbQueryStructure.put(keyElements[i], nextMap)
                }
                if (nextMap == null) {
                    nextMap = jsonbQueryStructure
                }
                nextMap.put(keyElements.last(), value)
                println("queryStructure: $jsonbQueryStructure")
                value = mapper.writeValueAsString([jsonbQueryStructure])
            }
        }
        if (storageType == StorageType.MARC21_JSON) {
            if (keyElements.length == 1) {
                // Probably search in control field
                value = mapper.writeValueAsString([[(keyElements[0]): value]])
            } else {
                value = mapper.writeValueAsString([[(keyElements[0]): ["subfields": [[(keyElements[1]):value]]] ]]);

            }
        }

        jsonbPath.append(" @> ?")

        return [jsonbPath.toString(), value]
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
        log.trace("calculated checksum: $hashtext")
        doc.manifest[Document.CHECKUM_KEY] = hashtext
        // Reinsert created and modified
        doc.setCreated(created)
        doc.setModified(modified)
        return hashtext
    }

    // TODO: Update to real locate
    @Override
    Location locate(String uri, boolean loadDoc) {
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
            //if (load(identifier, null, [], false)) {
            def docStatus = status(identifier)
            if (docStatus.exists && !docStatus.deleted) {
                if (loadDoc) {
                    return new Location(load(identifier)).withResponseCode(303)
                } else {
                    return new Location().withURI(new URI(identifier)).withResponseCode(303)
                }
            }
            log.debug("Check alternate identifiers.")
            doc = loadByAlternateIdentifier(uri)
            if (doc) {
                if (loadDoc) {
                    return new Location(doc).withResponseCode(301)
                } else {
                    return new Location().withURI(new URI(doc.identifier)).withResponseCode(301)
                }
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
        Connection connection = getConnection()
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
            log.trace("Executed query.")
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

    Iterable<Document> loadAll(String dataset) {
        return loadAllDocuments(dataset, false)
    }

    private Document assembleDocument(ResultSet rs) {
        Document doc = new Document(mapper.readValue(rs.getString("data"), Map), mapper.readValue(rs.getString("manifest"), Map))
        doc.setCreated(rs.getTimestamp("created")?.getTime())
        doc.setModified(rs.getTimestamp("modified").getTime())
        doc.setDeleted(rs.getBoolean("deleted") ?: false)
        return doc
    }

    private Iterable<Document> loadAllDocuments(String dataset, boolean withLinks, Date since = null, Date until = null) {
        log.debug("Load all called with dataset: $dataset")
        return new Iterable<Document>() {
            Iterator<Document> iterator() {
                Connection connection = getConnection()
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
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS_WITH_LINKS + " ORDER BY modified")
                    } else {
                        loadAllStatement = connection.prepareStatement(LOAD_ALL_DOCUMENTS + " ORDER BY modified")
                    }
                }
                loadAllStatement.setFetchSize(100)
                loadAllStatement.setTimestamp(1, new Timestamp(sinceTS))
                loadAllStatement.setTimestamp(2, new Timestamp(untilTS))
                if (dataset) {
                    loadAllStatement.setString(3, dataset)
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

    @Override
    boolean remove(String identifier, String dataset) {
        if (versioning) {
            log.debug("Creating tombstone record with id ${identifier}")
            return store(createTombstone(identifier, dataset))
        } else {
            Connection connection = getConnection()
            PreparedStatement delstmt = connection.prepareStatement(DELETE_DOCUMENT_STATEMENT)
            try {
                delstmt.setString(1, identifier)
                delstmt.executeUpdate()
                return true
            } finally {
                connection.close()
                log.debug("[remove] Closed connection.")
            }
        }
        return false
    }


    protected Document createTombstone(id, dataset) {
        def tombstone = new Document(id, ["@type":"Tombstone"]).withContentType("application/ld+json").withDataset(dataset)
        tombstone.setDeleted(true)
        return tombstone
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

}
