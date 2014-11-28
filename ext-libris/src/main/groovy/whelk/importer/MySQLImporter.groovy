package whelk.importer

import groovy.util.logging.Slf4j as Log

import java.sql.*
import java.util.concurrent.*

import whelk.*
import whelk.plugin.*

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
    int startAt = 0

    int addBatchSize = 5000

    int recordCount
    long startTime

    MySQLImporter(Map settings) {
        this.numberOfThreads = settings.get("numberOfThreads", 1)
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

        tickets = new Semaphore(20)
        //queue = Executors.newSingleThreadExecutor()
        queue = Executors.newFixedThreadPool(numberOfThreads)

        //log.info("Suspending camel during import.")
        //whelk.camelContext.suspend()

        try {

            Class.forName("com.mysql.jdbc.Driver")

            log.debug("Connecting to database...")
            conn = connectToUri(serviceUrl)

            if (dataset == "bib") {
                log.info("Creating bib load statement.")
                statement = conn.prepareStatement("SELECT bib.bib_id, bib.data, auth.auth_id FROM bib_record bib LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id WHERE bib.bib_id > ? AND bib.deleted = 0 ORDER BY bib.bib_id LIMIT 6000")
            }
            if (dataset == "hold") {
                log.info("Creating hold load statement.")
                statement = conn.prepareStatement("SELECT mfhd_id, data, bib_id, shortname FROM mfhd_record WHERE mfhd_id > ? AND deleted = 0 ORDER BY mfhd_id LIMIT 6000")
            }

            if (!statement) {
                throw new Exception("No valid dataset selected")
            }

            int recordId = startAt
            log.info("Starting loading at ID $recordId")

            def recordMap

            for (;;) {
                statement.setInt(1, recordId)
                resultSet = statement.executeQuery()
                log.debug("Reset statement with $recordId")

                int lastRecordId = recordId
                while(resultSet.next()){
                    recordId  = resultSet.getInt(1)
                    MarcRecord record = Iso2709Deserializer.deserialize(resultSet.getBytes("data"))

                    buildDocument(recordId, record, dataset, null)

                    if (dataset == "bib") {
                        int auth_id = resultSet.getInt("auth_id")
                        if (auth_id > 0) {
                            log.trace("Found auth_id $auth_id for $recordId Adding to oaipmhSetSpecs")
                            buildDocument(recordId, record, dataset, "authority:"+auth_id)
                        }
                    } else if (dataset == "hold") {
                        int bib_id = resultSet.getInt("bib_id")
                        String sigel = resultSet.getString("shortname")
                        if (bib_id > 0) {
                            log.trace("Found bib_id $bib_id for $recordId Adding to oaipmhSetSpecs")
                            buildDocument(recordId, record, dataset, "bibid:" + bib_id)
                        }
                        if (sigel) {
                            log.trace("Found sigel $sigel for $recordId Adding to oaipmhSetSpecs")
                            buildDocument(recordId, record, dataset, "location:" + sigel)
                        }
                    }

                }

                if (nrOfDocs > 0 && recordCount > nrOfDocs) {
                    log.info("Max docs reached. Breaking.")
                    break
                }

                if (cancelled || lastRecordId == recordId) {
                    log.info("Same id. Breaking.")
                    break
                }

            }
            log.debug("Clearing out remaining docs ...")
            buildDocument(0, null, dataset, null)

        } catch(SQLException se) {
            log.error("SQL Exception", se)
        } catch(Exception e) {
            log.error("Exception", e)
        } finally {
            log.info("Record count: ${recordCount}. Elapsed time: " + (System.currentTimeMillis() - startTime) + " milliseconds for sql results.")
            close()
        }

        /*
        queue.execute({
            log.info("Starting camel context ...")
            whelk.camelContext.resume()
        } as Runnable)
        */

        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)
        log.info("Import has completed in " + (System.currentTimeMillis() - startTime) + " milliseconds.")
        return recordCount
    }

    List<Document> documentList = new ArrayList<Document>()
    Document documentBuild = null

    void buildDocument(int id, MarcRecord record, String dataset, String oaipmhSetSpecValue) {
        String identifier = "/"+dataset+"/"+id
        if (documentBuild && (documentBuild.identifier != identifier || !record)) {
            /*
            log.info("dbi: ${documentBuild.identifier}")
            log.info("id: ${identifier}")
            log.info("record: $record")
            */
            if (!record) {
                log.debug("Received flush list signal.")
            } else {
                log.trace("New document received. Adding last ($documentBuild.identifier}) to the queue")
            }
            documentList << documentBuild
            if (documentList.size() % addBatchSize == 0 || !record) {
                log.debug("documentList is full. Sending it to bulkAdd (open the trapdoor)")
                addDocuments(documentList)
                documentList = new ArrayList<Document>()
            }
            documentBuild = null
        }
        if (record && !documentBuild) {
            log.trace("Creating new Document")
            def entry = ["identifier":identifier,"dataset":dataset]
            documentBuild = marcFrameConverter.doConvert(record, ["entry":entry,"meta":[:]])
            if (enhancer) {
                documentBuild = enhancer.filter(documentBuild)
            }
            recordCount++
        }
        if (documentBuild && oaipmhSetSpecValue) {
            documentBuild.meta.get("oaipmhSetSpecs", []).add(oaipmhSetSpecValue)
        }
    }
    void addDocuments(final List<Document> docs) {
        if (tickets.availablePermits() < 1) {
            log.info("Queues are full at the moment. Waiting for some to finish.")
        }
        tickets.acquire()
        queue.execute({
            try {
                log.debug("Starting add of ${docs.size()} documents.")
                whelk.bulkAdd(docs, docs.first().contentType)
            } finally {
                tickets.release()
                log.debug("Add completed.")
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

