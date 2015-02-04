package whelk.component

import groovy.util.logging.Slf4j as Log

import java.sql.*
import org.apache.commons.dbcp2.*

import whelk.*

@Log
class PostgreSQLStorage extends BasicComponent implements Storage {

    boolean versioning

    // Starta postgres: postgres -D /usr/local/var/postgres

    String mainTableName, versionsTableName

    // Database connectors
    Connection conn = null
    PreparedStatement statement = null
    ResultSet resultSet = null

    // Connectionpool
    private BasicDataSource connectionPool

    PostgreSQLStorage(String componentId = null, Map settings) {
        this.contentTypes = settings.get('contentTypes', null)
        this.versioning = settings.get('versioning', false)
        id = componentId
    }

    void componentBootstrap(String str) {
        log.info("Bootstrapping ${this.id}")
        if (!this.mainTableName) {
            this.mainTableName = str+"_"+this.id
        }
        if (versioning) {
            this.versionsTableName = mainTableName+VERSION_STORAGE_SUFFIX
        }
    }

    @Override
    void onStart() {
        log.info("Connecting to postgres ...")
        URI dbUri = new URI();
        //String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + dbUri.getPath();
        String dbUrl = "jdbc:postgresql:whelk"
        connectionPool = new BasicDataSource();

        if (dbUri.getUserInfo() != null) {
            connectionPool.setUsername(dbUri.getUserInfo().split(":")[0]);
            connectionPool.setPassword(dbUri.getUserInfo().split(":")[1]);
        }
        connectionPool.setDriverClassName("org.postgresql.Driver");
        connectionPool.setUrl(dbUrl);
        connectionPool.setInitialSize(1);
        createTable(mainTableName)
        if (versioning) {
            createTable(versionsTableName)
        }
    }

    void createTable(String tbl) {
        log.info("Creating tables.")
        Connection connection = connectionPool.getConnection();

        Statement stmt = connection.createStatement();
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS $tbl ("
            +"id serial,"
            +"identifier varchar(200) not null unique,"
            +"data bytea,"
            +"entry jsonb,"
            +"meta jsonb"
            +")");
    }

    @Override
    boolean eligibleForStoring(Document doc) {
        return true
    }

    @Override
    boolean store(Document doc) {
        Connection connection = connectionPool.getConnection();
        PreparedStatement pstmt = connection.prepareStatement("INSERT INTO $mainTableName (identifier,data,entry,meta) VALUES (?,?,?,?)")

        //stmt.executeUpdate("INSERT INTO $mainTableName (identifier,data,entry,meta) VALUES (${doc.identifier},${doc.data},${doc.entryAsJson},${doc.metaAsJson})");
        pstmt.setString(1, doc.identifier)
        pstmt.setBytes(2, doc.data)
        pstmt.setObject(3, doc.entryAsJson, java.sql.Types.OTHER)
        pstmt.setObject(4, doc.metaAsJson, java.sql.Types.OTHER)
        pstmt.executeUpdate()
        return true
    }

    @Override
    void bulkStore(final List docs) {
    }

    Document get(String identifier) {
        return get(identifier, null)
    }

    @Override
    Document get(String identifier, String version) {
        return null
    }

    @Override
    Document getByAlternateIdentifier(String identifier) {
        return null
    }

    @Override
    Iterable<Document> getAll() {
        return getAll(null,null,null)
    }

    Iterable<Document> getAll(String dataset, Date since = null, Date until = null) {
        return null
    }

    @Override
    void remove(String identifier) {
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
