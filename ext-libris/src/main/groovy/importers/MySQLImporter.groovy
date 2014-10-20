package se.kb.libris.whelks.importers

import groovy.util.logging.Slf4j as Log

import java.sql.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*

import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*

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
        assert marcFrameConverter
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
            statement = conn.prepareStatement("SELECT bib.bib_id, bib.data, auth.auth_id FROM bib_record bib LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id WHERE bib.bib_id > ? ORDER BY bib.bib_id LIMIT 1000")

            int bib_id = 0

            for (;;) {
                statement.setInt(1, bib_id)
                resultSet = statement.executeQuery()
                def docSet = [:]

                int lastBibId = bib_id
                while(resultSet.next()){
                    bib_id  = resultSet.getInt("bib_id")
                    MarcRecord record = Iso2709Deserializer.deserialize(resultSet.getBytes("data"))
                    int auth_id = resultSet.getInt("auth_id")
                    def doc = (docSet.containsKey(bib_id) ? docSet.get(bib_id) : 

                    if (auth_id) {
                        log.trace("Found auth_id $auth_id for $bib_id (Don't know what to do with it yet, though.)")
                    }

                    log.info("id: $bib_id  count: $recordCount doc_id: ${doc?.identifier}")

                    docSet.put(bib_id, doc)

                    recordCount++
                }

                if (cancelled || lastBibId == bib_id) {
                    recordCount--
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


    void addDocuments(String dataset, final List recordSet) {
        tickets.acquire()
        queue.execute({
            try {
                def docs = []
                recordSet.each { record, auths ->
                    def entry = ["identifier":"/"+dataset+"/"+record.getControlfields("001").get(0).getData(),"dataset":dataset]
                    def meta = [:]
                    docs << enhancer.filter(marcFrameConverter.doConvert(record, ["entry":entry,"meta":meta]))
                    }
                log.debug("Saving collected documents.")
                whelk.bulkAdd(docs, docs.first().contentType)
            } finally {
                tickets.release()
            }
        } as Runnable)
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

