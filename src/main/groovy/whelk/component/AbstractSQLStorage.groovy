package whelk.component

import groovy.util.logging.Slf4j as Log

import java.sql.*
import org.apache.commons.dbcp2.*

import whelk.*

abstract class AbstractSQLStorage extends BasicComponent implements Storage {
    boolean versioning
    boolean readOnly = false

    String connectionUrl = null

    // Connectionpool
    protected BasicDataSource connectionPool


    @Override
    void onStart() {
        log.info("Connecting to sql database at $connectionUrl")
        connectionPool = new BasicDataSource();

        if (whelk.props.sqlUsername != null) {
            connectionPool.setUsername(whelk.props.sqlUsername)
            connectionPool.setPassword(whelk.props.sqlPassword)
        }
        connectionPool.setDriverClassName(getJdbcDriver());
        connectionPool.setUrl(connectionUrl);
        connectionPool.setInitialSize(10);
        createTables()
    }

    abstract String getJdbcDriver()
    abstract void createTables()
}
