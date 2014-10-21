package se.kb.libris.whelks.importers

import groovy.util.logging.Slf4j as Log

import java.sql.*
import java.util.concurrent.*

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

    ExecutorService queue
    Semaphore tickets

    int numberOfThreads

    int recordCount
    long startTime

    MySQLImporter(Map settings) {
        this.numberOfThreads = settings.get("numberOfThreads", 50000)
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

        tickets = new Semaphore(numberOfThreads)
        queue = Executors.newSingleThreadExecutor()

        try {
            Class.forName("com.mysql.jdbc.Driver")

            log.debug("Connecting to database...")
            conn = connectToUri(serviceUrl)

            if (dataset == "bib") {
                log.info("Creating bib load statement.")
                statement = conn.prepareStatement("SELECT bib.bib_id, bib.data, auth.auth_id FROM bib_record bib LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id WHERE bib.bib_id > ? AND bib.deleted = 0 ORDER BY bib.bib_id LIMIT 1000")
            }
            if (dataset == "hold") {
                log.info("Creating hold load statement.")
                statement = conn.prepareStatement("SELECT mfhd_id, data, bib_id FROM mfhd_record WHERE mfhd_id > ? AND deleted = 0 ORDER BY mfhd_id LIMIT 5000")
            }

            if (!statement) {
                throw new Exception("No valid dataset selected")
            }

            int recordId = 0

            for (;;) {
                statement.setInt(1, recordId)
                resultSet = statement.executeQuery()
                def recordMap = [:]

                int lastRecordId = recordId
                while(resultSet.next()){
                    recordId  = resultSet.getInt(1)
                    MarcRecord record = Iso2709Deserializer.deserialize(resultSet.getBytes("data"))

                    def recordMeta = recordMap.get(recordId, ["record":record, "meta": [:]]).get("meta")

                    if (dataset == "bib") {
                        int auth_id = resultSet.getInt("auth_id")
                        if (auth_id > 0) {
                            log.trace("Found auth_id $auth_id for $recordId Adding to oaipmhSetSpecs")
                            recordMeta.get("oaipmhSetSpecs", []).add("authority:" + auth_id)
                        }
                    } else if (dataset == "hold") {
                        int bib_id = resultSet.getInt("bib_id")
                        if (bib_id > 0) {
                            log.trace("Found bib_id $bib_id for $recordId Adding to oaipmhSetSpecs")
                            recordMeta.get("oaipmhSetSpecs", []).add("bibid:" + bib_id)
                        }
                    }

                    recordMap.put(recordId, ["record": record, "meta": recordMeta])

                    //log.info("id: $recordId  count: $recordCount")

                    recordCount++
                }

                addDocuments(dataset, recordMap)

                if (nrOfDocs > 0 && recordCount > nrOfDocs) {
                    log.info("Max docs reached. Breaking.")
                    break
                }

                if (cancelled || lastRecordId == recordId) {
                    recordCount--
                    log.info("Same id. Breaking.")
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
        queue.shutdown()
        log.debug("Shutting down queue")
        return recordCount
    }


    void addDocuments(String dataset, final Map recordMap) {
        if (tickets.availablePermits() < 10) {
            log.info("Trying to acquire semaphore for adding to queue. ${tickets.availablePermits()} available.")
        }
        tickets.acquire()
        queue.execute({
            try {
                def docs = []
                log.debug("Converting MARC21 into JSONLD")
                recordMap.each { id, data ->
                    def entry = ["identifier":"/"+dataset+"/"+data.record.getControlfields("001").get(0).getData(),"dataset":dataset]
                    docs << enhancer.filter(marcFrameConverter.doConvert(data.record, ["entry":entry,"meta":data.meta]))
                }
                log.debug("Saving ${docs.size()} collected documents.")
                whelk.bulkAdd(docs, docs.first().contentType)
                log.debug("Documents saved.")
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
        log.info("Closing down mysql connections.")
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

