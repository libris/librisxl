package whelk.importer.libris

import groovy.util.logging.Slf4j as Log

import java.sql.*
import java.util.concurrent.*

import whelk.*
import whelk.importer.*
import whelk.plugin.*
import whelk.plugin.libris.*

import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*

@Log
class MySQLImporter extends BasicPlugin implements Importer {

    MarcFrameConverter marcFrameConverter
    JsonLDLinkCompleterFilter enhancer

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"

    boolean cancelled = false

    ExecutorService queue
    Semaphore tickets

    int startAt = 0

    int addBatchSize = 5000

    int recordCount
    long startTime

    List<Document> documentList = []
    ConcurrentHashMap buildingMetaRecord = new ConcurrentHashMap()
    String lastIdentifier = null


    MySQLImporter(Map settings) {
    }

    void bootstrap() {
        marcFrameConverter = plugins.find { it instanceof MarcFrameConverter }
        enhancer = plugins.find { it instanceof JsonLDLinkCompleterFilter }
        assert marcFrameConverter
    }

    int doImport(String dataset, int nrOfDocs = -1, boolean silent = false, boolean picky = true, URI serviceUrl = null) {
        recordCount = 0
        startTime = System.currentTimeMillis()
        cancelled = false
        Connection conn = null
        PreparedStatement statement = null
        ResultSet resultSet = null


        int sqlLimit = 6000
        if (nrOfDocs > 0 && nrOfDocs < sqlLimit) { sqlLimit = nrOfDocs }

        tickets = new Semaphore(0)
        //queue = Executors.newSingleThreadExecutor()
        queue = Executors.newWorkStealingPool()

        def versioningSettings = [:]

        //log.info("Suspending camel during import.")
        //whelk.camelContext.suspend()
        for (st in this.whelk.getStorages()) {
            log.debug("Turning off versioning in ${st.id}")
            // Preserve original setting
            versioningSettings.put(st.id, st.versioning)
            st.versioning = false
        }

        try {

            Class.forName("com.mysql.jdbc.Driver")

            log.debug("Connecting to database...")
            conn = connectToUri(serviceUrl)

            if (dataset == "bib") {
                log.info("Creating bib load statement.")
                statement = conn.prepareStatement("SELECT bib.bib_id, bib.data, auth.auth_id FROM bib_record bib LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id WHERE bib.bib_id > ? AND bib.deleted = 0 ORDER BY bib.bib_id LIMIT $sqlLimit")
            }
            if (dataset == "hold") {
                log.info("Creating hold load statement.")
                statement = conn.prepareStatement("SELECT mfhd_id, data, bib_id, shortname FROM mfhd_record WHERE mfhd_id > ? AND deleted = 0 ORDER BY mfhd_id LIMIT $sqlLimit")
            }

            if (!statement) {
                throw new Exception("No valid dataset selected")
            }

            int recordId = startAt
            log.info("Starting loading at ID $recordId")

            MarcRecord record = null

            for (;;) {
                statement.setInt(1, recordId)
                log.debug("Reset statement with $recordId")
                resultSet = statement.executeQuery()

                log.debug("Query executed. Starting processing ...")
                int lastRecordId = recordId
                while(resultSet.next()) {
                    recordId  = resultSet.getInt(1)
                    record = Iso2709Deserializer.deserialize(resultSet.getBytes("data"))

                    buildDocument(record, dataset, null)

                    if (dataset == "bib") {
                        int auth_id = resultSet.getInt("auth_id")
                        if (auth_id > 0) {
                            log.trace("Found auth_id $auth_id for $recordId Adding to oaipmhSetSpecs")
                            buildDocument(record, dataset, "authority:"+auth_id)
                        }
                    } else if (dataset == "hold") {
                        int bib_id = resultSet.getInt("bib_id")
                        String sigel = resultSet.getString("shortname")
                        if (bib_id > 0) {
                            log.trace("Found bib_id $bib_id for $recordId Adding to oaipmhSetSpecs")
                            buildDocument(record, dataset, "bibid:" + bib_id)
                        }
                        if (sigel) {
                            log.trace("Found sigel $sigel for $recordId Adding to oaipmhSetSpecs")
                            buildDocument(record, dataset, "location:" + sigel)
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
            buildDocument(null, dataset, null)

        } catch(SQLException se) {
            log.error("SQL Exception", se)
        } catch(Exception e) {
            log.error("Exception", e)
        } finally {
            log.info("Record count: ${recordCount}. Elapsed time: " + (System.currentTimeMillis() - startTime) + " milliseconds for sql results.")
            close(conn, statement, resultSet)
        }

        queue.execute({
            this.whelk.flush()
            log.debug("Resetting versioning setting for storages")
            for (st in this.whelk.getStorages()) {
                st.versioning = versioningSettings.get(st.id)
            }
            //log.info("Starting camel context ...")
            //whelk.camelContext.resume()
        } as Runnable)

        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)
        log.info("Import has completed in " + (System.currentTimeMillis() - startTime) + " milliseconds.")
        return recordCount
    }

    void buildDocument(MarcRecord record, String dataset, String oaipmhSetSpecValue) {
        String identifier = null
        if (documentList.size() >= addBatchSize || record == null) {
            /*
            if (tickets.availablePermits() < 1) {
                log.info("Queues are full at the moment. Waiting for some to finish.")
            }
            tickets.acquire()
            */
            log.debug("Doclist has reached batch size. Sending it to bulkAdd (open the trapdoor)")
            queue.execute(new ConvertAndStoreRunner(whelk, marcFrameConverter, enhancer, documentList, tickets))
            log.debug("     Current poolsize: ${queue.poolSize}")
            log.debug("------------------------------")
            log.debug("queuedSubmissionCount: ${queue.queuedSubmissionCount}")
            log.debug("      queuedTaskCount: ${queue.queuedTaskCount}")
            log.debug("   runningThreadCount: ${queue.runningThreadCount}")
            log.debug("    activeThreadCount: ${queue.activeThreadCount}")
            log.debug("------------------------------")
            //log.debug("       completed jobs: ${tickets.availablePermits()}")
            this.documentList = []
        }
        if (record) {
            def aList = record.getDatafields("599").collect { it.getSubfields("a").data }.flatten()
            if ("SUPPRESSRECORD" in aList) {
                log.debug("Record ${identifier} is suppressed. Next ...")
                return
            }
            log.trace("building document $identifier")
            identifier = "/"+dataset+"/"+record.getControlfields("001").get(0).getData()
            buildingMetaRecord.get(identifier, [:]).put("record", record)
            buildingMetaRecord.get(identifier, [:]).put("entry", ["identifier":identifier,"dataset":dataset])
        }
        if (oaipmhSetSpecValue) {
            buildingMetaRecord.get(identifier, [:]).get("meta", [:]).get("oaipmhSetSpecs", []).add(oaipmhSetSpecValue)
        }
        if (lastIdentifier && lastIdentifier != identifier) {
            log.trace("New document received. Adding last ($lastIdentifier}) to the doclist")
            recordCount++
            documentList << buildingMetaRecord.remove(lastIdentifier)
        }
        lastIdentifier = identifier
    }

    class ConvertAndStoreRunner implements Runnable {

        private Whelk whelk
        private MarcFrameConverter converter
        private Filter filter

        private List recordList

        private Semaphore tickets

        ConvertAndStoreRunner(Whelk w, FormatConverter c, Filter f, final List recList, Semaphore t) {
            this.whelk = w
            this.converter = c
            this.filter = f

            this.recordList = recList
            this.tickets = t
        }

        @Override
        void run() {
            try {
                List<Document> convertedDocs = []
                recordList.each {

                    Document doc = converter.doConvert(it.record, ["entry":it.entry,"meta":it.meta])
                    if (filter) {
                        doc = filter.filter(doc)
                    }
                    convertedDocs << doc
                }
                this.whelk.bulkAdd(convertedDocs, convertedDocs.first().dataset, convertedDocs.first().contentType, false)
            } finally {
                tickets.release()
            }
        }
    }

    /*
    void addDocuments(final List<Document> docs) {
        if (tickets.availablePermits() < 1) {
            log.info("Queues are full at the moment. Waiting for some to finish.")
        }
        tickets.acquire()
        queue.execute({
            try {
                log.debug("Starting add of ${docs.size()} documents.")
                whelk.bulkAdd(docs, docs.first().contentType, false)
                log.debug("Bulk add operation completed.")
            } finally {
                tickets.release()
                log.debug("Add completed.")
            }
        } as Runnable)
    }
    */

    Connection connectToUri(URI uri) {
        log.info("connect uri: $uri")
        DriverManager.getConnection(uri.toString())
    }


    void cancel() { cancelled = true}


    public void close(Connection conn, PreparedStatement statement, ResultSet resultSet) {
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

