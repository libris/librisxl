package whelk.component

import groovy.util.logging.Slf4j as Log

import java.sql.*
import org.apache.commons.dbcp2.*

import whelk.*

abstract class AbstractSQLStorage extends BasicComponent implements Storage {
    boolean versioning
    boolean readOnly = false

    String connectionUrl = null
    String username = null
    String password = null

    // Connectionpool
    protected BasicDataSource connectionPool


    @Override
    void onStart() {
        log.info("Connecting to sql database at $connectionUrl")
        connectionPool = new BasicDataSource();

        if (username != null) {
            connectionPool.setUsername(username)
            connectionPool.setPassword(password)
        }
        connectionPool.setDriverClassName(getJdbcDriver());
        connectionPool.setUrl(connectionUrl);
        connectionPool.setInitialSize(10);
        connectionPool.setMaxTotal(40);
        connectionPool.setDefaultAutoCommit(true)
        //createTables()
    }

    abstract String getJdbcDriver()
    //abstract void createTables()
}
