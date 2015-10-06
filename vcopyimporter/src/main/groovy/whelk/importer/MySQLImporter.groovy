package whelk.importer

import groovy.util.logging.Slf4j as Log
import org.picocontainer.Characteristics
import org.picocontainer.DefaultPicoContainer
import org.picocontainer.PicoContainer
import org.picocontainer.containers.PropertiesPicoContainer
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.converter.FormatConverter
import whelk.converter.JsonLDLinkCompleterFilter
import whelk.converter.marc.MarcFrameConverter
import whelk.converter.MarcJSONConverter

import java.sql.*
import java.util.concurrent.*
import java.text.*

import whelk.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*

@Log
class MySQLImporter {

    MarcFrameConverter marcFrameConverter
    JsonLDLinkCompleterFilter enhancer

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"
    long MAX_MEMORY_THRESHOLD = 70 // In percent

    boolean cancelled = false

    ExecutorService queue
    Semaphore tickets

    int startAt = 0

    int addBatchSize = 1000

    int recordCount
    long startTime

    List<Document> documentList = []
    ConcurrentHashMap buildingMetaRecord = new ConcurrentHashMap()
    String lastIdentifier = null


    PicoContainer pico

    MySQLImporter() {
        log.info("Setting up httpwhelk.")

        // If an environment parameter is set to point to a file, use that one. Otherwise load from classpath
        InputStream secretsConfig = ( System.getProperty("xl.secret.properties")
                ? new FileInputStream(System.getProperty("xl.secret.properties"))
                : this.getClass().getClassLoader().getResourceAsStream("secret.properties") )

        Properties props = new Properties()

        props.load(secretsConfig)

        pico = new DefaultPicoContainer(new PropertiesPicoContainer(props))
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(ElasticSearch.class)
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(PostgreSQLComponent.class)
        pico.addComponent(new MarcFrameConverter())
        pico.addComponent(Whelk.class)

        pico.start()

        log.info("Started ...")
    }

    void bootstrap() {
        marcFrameConverter = plugins.find { it instanceof MarcFrameConverter }
        enhancer = plugins.find { it instanceof JsonLDLinkCompleterFilter }
        assert marcFrameConverter
    }

    void doImport(String dataset, int nrOfDocs = -1, boolean silent = false, boolean picky = true, URI serviceUrl = null) {
        recordCount = 0
        startTime = System.currentTimeMillis()
        cancelled = false
        Connection conn = null
        PreparedStatement statement = null
        ResultSet resultSet = null


        int sqlLimit = 6000
        if (nrOfDocs > 0 && nrOfDocs < sqlLimit) { sqlLimit = nrOfDocs }

        tickets = new Semaphore(10)
        //queue = Executors.newSingleThreadExecutor()
        //queue = Executors.newWorkStealingPool()
        queue = Executors.newFixedThreadPool(10)

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
            conn.setAutoCommit(false)

            if (dataset == "auth") {
                log.info("Creating auth load statement.")
                statement = conn.prepareStatement("SELECT auth_id, data FROM auth_record WHERE auth_id > ? AND deleted = 0 ORDER BY auth_id", java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
            }
            if (dataset == "bib") {
                log.info("Creating bib load statement.")
                statement = conn.prepareStatement("SELECT bib.bib_id, bib.data, auth.auth_id FROM bib_record bib LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id WHERE bib.bib_id > ? AND bib.deleted = 0 ORDER BY bib.bib_id", java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
            }
            if (dataset == "hold") {
                log.info("Creating hold load statement.")
                statement = conn.prepareStatement("SELECT mfhd_id, data, bib_id, shortname FROM mfhd_record WHERE mfhd_id > ? AND deleted = 0 ORDER BY mfhd_id", java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
            }

            if (!statement) {
                throw new Exception("No valid dataset selected")
            }
            statement.setFetchSize(Integer.MIN_VALUE)

            int recordId = startAt
            log.info("Starting loading at ID $recordId")

            MarcRecord record = null

            statement.setInt(1, recordId)
            resultSet = statement.executeQuery()

            log.debug("Query executed. Starting processing ...")
            int lastRecordId = recordId
            while(resultSet.next()) {
                recordId = resultSet.getInt(1)
                record = Iso2709Deserializer.deserialize(normalizeString(new String(resultSet.getBytes("data"), "UTF-8")).getBytes("UTF-8"))

                buildDocument(recordId, record, dataset, null)

                if (dataset == "auth") {
                    int auth_id = resultSet.getInt("auth_id")
                    if (auth_id > 0) {
                        buildDocument(recordId, record, dataset, null)
                    }
                } else if (dataset == "bib") {
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
                if (nrOfDocs > 0 && recordCount > nrOfDocs) {
                    log.info("Max docs reached. Breaking.")
                    break
                }
                if (cancelled) {
                    break
                }
            }
            log.debug("Clearing out remaining docs ...")
            buildDocument(null, null, dataset, null)

        } catch(SQLException se) {
            log.error("SQL Exception", se)
        } catch(Exception e) {
            log.error("Exception", e)
        } finally {
            log.info("Record count: ${recordCount}. Elapsed time: " + (System.currentTimeMillis() - startTime) + " milliseconds for sql results.")
            close(conn, statement, resultSet)
        }


        /*
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
        */

        log.info("Import has completed in " + (System.currentTimeMillis() - startTime) + " milliseconds.")
        //return new ImportResult(numberOfDocuments: recordCount, lastRecordDatestamp: null) // TODO: Add correct last document datestamp
    }

    void buildDocument(Integer recordId, MarcRecord record, String type, String oaipmhSetSpecValue) {
        String identifier = null
        String dataset = type
        if (documentList.size() >= addBatchSize || record == null) {
            if (tickets.availablePermits() < 1) {
                log.info("Queues are full at the moment. Waiting for some to finish.")
            }
            tickets.acquire()
            log.debug("Doclist has reached batch size. Sending it to bulkAdd (open the trapdoor)")

            //def casr = new ConvertAndStoreRunner(whelk, marcFrameConverter, enhancer, documentList, tickets)
            //casr.run()
            queue.execute(new ConvertAndStoreRunner(whelk, marcFrameConverter, enhancer, documentList, tickets))
            /*
            log.debug("     Current poolsize: ${queue.poolSize}")
            log.debug("------------------------------")
            log.debug("queuedSubmissionCount: ${queue.queuedSubmissionCount}")
            log.debug("      queuedTaskCount: ${queue.queuedTaskCount}")
            log.debug("   runningThreadCount: ${queue.runningThreadCount}")
            log.debug("    activeThreadCount: ${queue.activeThreadCount}")
            log.debug("------------------------------")
            */
            //log.debug("       completed jobs: ${tickets.availablePermits()}")
            log.debug("Documents stored. Continuing to load rows")
            this.documentList = []
        }
        if (record) {
            def aList = record.getDatafields("599").collect { it.getSubfields("a").data }.flatten()
            if ("SUPPRESSRECORD" in aList) {
                log.debug("Record ${identifier} is suppressed. Setting dataset to $SUPPRESSRECORD_DATASET ...")
                dataset = SUPPRESSRECORD_DATASET
            }
            log.trace("building document $identifier")
            try {
                identifier = "/"+type+"/"+record.getControlfields("001").get(0).getData()
                buildingMetaRecord.get(identifier, [:]).put("record", record)
                buildingMetaRecord.get(identifier, [:]).put("manifest", ["identifier":identifier,"dataset":dataset])
            } catch (Exception e) {
                log.error("Problem getting field 001 from marc record $recordId. Skipping document.", e)
            }
        }
        try {
            if (oaipmhSetSpecValue) {
                buildingMetaRecord.get(identifier, [:]).get("meta", [:]).get("oaipmhSetSpecs", []).add(oaipmhSetSpecValue)
            }
            if (lastIdentifier && lastIdentifier != identifier) {
                log.trace("New document received. Adding last ($lastIdentifier}) to the doclist")
                recordCount++
                documentList << buildingMetaRecord.remove(lastIdentifier)
            }
            lastIdentifier = identifier
        } catch (Exception e) {
            log.error("Problem with marc record ${recordId}. Skipping ...", e)
        }
    }

    class ConvertAndStoreRunner implements Runnable {

        private Whelk whelk
        private MarcFrameConverter converter

        private List recordList

        private Semaphore tickets

        ConvertAndStoreRunner(Whelk w, FormatConverter c, final List recList, Semaphore t) {
            this.whelk = w
            this.converter = c

            this.recordList = recList
            this.tickets = t
        }

        @Override
        void run() {
            try {
                def convertedDocs = [:]
                recordList.each {
                    if (!convertedDocs.containsKey(it.manifest.dataset)) { // Create new list
                        convertedDocs.put(it.manifest.dataset, [])
                    }
                    if (it.manifest.dataset == SUPPRESSRECORD_DATASET) {
                        it.manifest['contentType'] = "application/x-marc-json"
                        convertedDocs[(SUPPRESSRECORD_DATASET)] << whelk.createDocument(MarcJSONConverter.toJSONMap(it.record), it.manifest, it.meta)
                    } else {
                        Document doc = converter.doConvert(it.record, ["manifest":it.manifest,"meta":it.meta])
                        convertedDocs[(doc.dataset)] << doc
                    }
                }
                convertedDocs.each { ds, docList ->
                    this.whelk.bulkAdd(docList, ds, docList.first().contentType, false)
                }
            } finally {
                tickets.release()
            }
        }
    }

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

    String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            log.trace("Normalizing ...")
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }
}

