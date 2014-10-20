package se.kb.libris.whelks.importers

import groovy.util.logging.Slf4j as Log

import java.sql.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*

@Log
class MySQLImporter extends BasicPlugin implements Importer {

    Whelk whelk

    MarcFrameConverter marcFrameConverter
    JsonLDLinkCompleterFilter enhancer

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"

    Connection conn = null
    PreparedStatement statement = null
    ResultSet resultSet = null

    boolean cancelled = false

    int recordCount
    long startTime

    MySQLImporter(Map settings) {
    }

    void bootstrap(String whelkId) {
        marcFrameConverter = plugins.find { it instanceof MarcFrameConverter }
        enhancer = plugins.find { it instanceof JsonLDLinkCompleterFilter }
    }

    int doImport(String dataset, int nrOfDocs = -1, boolean silent = false, boolean picky = true, URI serviceUrl = null) {
        recordCount = 0
        startTime = System.currentTimeMillis()
        cancelled = false

        try {
            Class.forName("com.mysql.jdbc.Driver")

            log.debug("Connecting to database...")
            conn = connectToUri(serviceUrl)

            log.debug("Creating statement...")
            //statement = conn.prepareStatement("SELECT bib_id FROM bib_record WHERE bib_id > ? ORDER BY bib_id LIMIT 1000")
            statement = conn.prepareStatement("SELECT auth_id FROM auth_record WHERE auth_id >= ? ORDER BY auth_id LIMIT 1000")

            int bib_id = 0

            for (;;) {
                statement.setInt(1, bib_id)
                resultSet = statement.executeQuery()

                int lastBibId = bib_id
                while(resultSet.next()){
                    bib_id  = resultSet.getInt("auth_id")
                    log.info("id: $bib_id  count: $recordCount")
                    recordCount++
                }
                if (cancelled || lastBibId == bib_id) {
                    log.info("Same id. Breaking.")
                    break
                }
                if (nrOfDocs > 0 && recordCount > nrOfDocs) {
                    log.info("Max docs reached. Breaking.")
                    break
                }
            }

        } catch(SQLException se) {
            log.error("SQL Exception", se)
        } catch(Exception e) {
            log.error("Exception", e)
        } finally {
            log.info("Record count: ${recordCount}. Elapsed time: " + (System.currentTimeMillis() - startTime) + " milliseconds.")
            close()
        }
        return recordCount
    }

    Connection connectToUri(URI uri) {
        log.info("connect uri: $uri")
        DriverManager.getConnection(uri.toString())
    }


    void cancel() { cancelled = true}


    public void close() {
        log.info("Closing down everything.")
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

