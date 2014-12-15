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

        int sqlLimit = 6000
        if (nrOfDocs > 0 && nrOfDocs < sqlLimit) { sqlLimit = nrOfDocs }

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
                resultSet = statement.executeQuery()
                log.debug("Reset statement with $recordId")

                int lastRecordId = recordId
                while(resultSet.next()){
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
    ConcurrentHashMap buildingMetaRecord = new ConcurrentHashMap()
    String lastIdentifier = null
    Stack<Future> futures = new Stack<Future>()

    void buildDocument(MarcRecord record, String dataset, String oaipmhSetSpecValue) {
        String identifier = null
        if (record) {
            identifier = "/"+dataset+"/"+record.getControlfields("001").get(0).getData()
            buildingMetaRecord.get(identifier, [:]).put("record", record)
        }
        log.trace("building document $identifier")
        if (documentList.size() >= addBatchSize || record == null) {
            log.debug("documentList is full. Sending it to bulkAdd (open the trapdoor)")
            whelk.bulkAdd(documentList, documentList.first().contentType)
            log.debug("documents added.")
            documentList = new ArrayList<Document>()
        }

        if (lastIdentifier && lastIdentifier != identifier) {
            log.trace("New document received. Adding last ($lastIdentifier}) to the queue")
            recordCount++
            log.trace("pushing to queue: dataset: $dataset, meta: $buildingMetaRecord")
            // Add converter to queue
            futures.push(queue.submit(new MarcDocumentConverter(lastIdentifier, dataset, marcFrameConverter, enhancer, buildingMetaRecord.get(lastIdentifier).record, buildingMetaRecord.get(lastIdentifier).get("meta", [:]))))
            buildingMetaRecord.remove(lastIdentifier)
        }
        if (oaipmhSetSpecValue) {
            buildingMetaRecord.get(identifier, [:]).get("meta", [:]).get("oaipmhSetSpecs", []).add(oaipmhSetSpecValue)
        }
        lastIdentifier = identifier

        while (!futures.isEmpty()) {
            documentList << futures.pop().get()
        }
    }

    class MarcDocumentConverter implements Callable<Document> {

        private MarcRecord record
        private Map meta
        private MarcFrameConverter converter
        private Filter filter
        private String dataset
        private String identifier

        MarcDocumentConverter(String i, String d, FormatConverter c, Filter f, final MarcRecord mr, final Map m) {
            this.record = mr
            this.meta = m
            this.converter = c
            this.filter = f
            this.dataset = d
            this.identifier = i
        }

        @Override
        Document call() {
            def entry = ["identifier":identifier,"dataset":dataset]
            if (!identifier) {
                log.error("Bad sit: entry is $entry")
                throw new RuntimeException("No, i don't wanna.")
            }
            Document doc = converter.doConvert(this.record, ["entry":entry,"meta":meta])
            if (filter) {
                doc = filter.filter(doc)
            }
            return doc
        }
    }

    @Deprecated
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

