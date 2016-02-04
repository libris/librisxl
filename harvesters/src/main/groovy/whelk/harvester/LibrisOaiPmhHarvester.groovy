package whelk.harvester

import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult
import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.converter.JsonLDLinkCompleterFilter
import whelk.converter.marc.MarcFrameConverter
import whelk.exception.WhelkAddException
import whelk.util.LegacyIntegrationTools

import java.text.*
import java.util.concurrent.*

import whelk.*
import se.kb.libris.util.marc.*

import se.kb.libris.util.marc.io.*
import whelk.converter.MarcJSONConverter

@Log
class LibrisOaiPmhHarvester extends OaiPmhHarvester {

    static SERVICE_BASE_URL = "http://data.libris.kb.se/{dataset}/oaipmh"

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"

    String collection
    String serviceUrl
    int recordCount = 0
    int nrDeleted = 0
    int skippedRecordCount = 0
    long startTime = 0

    boolean preserveTimestamps = true

    long runningTime = 0

    ExecutorService queue
    Semaphore tickets
    Whelk whelk
    MarcFrameConverter marcFrameConverter
    JsonLDLinkCompleterFilter enhancer
    static final ObjectMapper mapper = new ObjectMapper()

    String username, password

    boolean cancelled = false

    LibrisOaiPmhHarvester(Whelk w, MarcFrameConverter mfc, JsonLDLinkCompleterFilter jlcf, String oaipmhServiceUrl, String oaipmhUsername, String oaipmhPassword) {
        whelk = w
        serviceUrl = oaipmhServiceUrl
        marcFrameConverter = mfc
        username = oaipmhUsername
        password = oaipmhPassword
        enhancer = jlcf
    }

    LibrisOaiPmhHarvester(Whelk w, MarcFrameConverter mfc, String oaipmhServiceUrl, String oaipmhUsername, String oaipmhPassword) {
        whelk = w
        serviceUrl = oaipmhServiceUrl
        marcFrameConverter = mfc
        username = oaipmhUsername
        password = oaipmhPassword
    }

    LibrisOaiPmhHarvester(){}

    HarvestResult doImport(String collection, int nrOfDocs) {
        return doImport(collection, null, nrOfDocs)
    }

    @groovy.transform.Synchronized
    HarvestResult doImport(String collection, String startResumptionToken = null, int nrOfDocs = -1, boolean silent = false, boolean picky = true, Date from = null) {
        getAuthentication()
        this.cancelled = false
        this.collection = collection
        this.recordCount = 0
        this.skippedRecordCount = 0
        this.nrDeleted = 0
        if (!serviceUrl) {
            serviceUrl = SERVICE_BASE_URL
        }
        String baseUrl = serviceUrl.replace("{dataset}", collection)

        String urlString = baseUrl + "?verb=ListRecords&metadataPrefix=marcxml"

        def versioningSettings = [:]

        tickets = new Semaphore(100)

        if (from) {
            urlString = urlString + "&from=" + from.format(DATE_FORMAT, TimeZone.getTimeZone('UTC'))
        }
        log.debug("urlString: $urlString")
        queue = Executors.newSingleThreadExecutor()
        //queue = Executors.newFixedThreadPool(numberOfThreads)
        //queue = Executors.newWorkStealingPool()
        startTime = System.currentTimeMillis()
        URL url
        if (startResumptionToken) {
            url = new URL(baseUrl + "?verb=ListRecords&resumptionToken=" + startResumptionToken)
        } else {
            url = new URL(urlString)
        }
        log.debug("Harvesting OAIPMH data from ${url.toString()}. Pickymode: $picky")

        def harvestResult = null
        Date lastRecordDatestamp = from
        def resumptionToken = null
        long loadUrlTime = startTime
        long elapsed = 0

        while (elapsed == 0 ||
            (!cancelled && resumptionToken && (nrOfDocs == -1 || recordCount <  nrOfDocs))) {
            loadUrlTime = System.currentTimeMillis()
            if (resumptionToken) {
                url = new URL(baseUrl + "?verb=ListRecords&resumptionToken=" + resumptionToken)
            }
            log.trace("Harvesting $url")
            try {
                harvestResult = harvest(url, harvestResult?.lastRecordDatestamp ?: from ?: new Date(0))
                //harvestResult = harvest(url, new HarvestResult())
            } catch (XmlParsingFailedException xpfe) {
                log.warn("[$collection / $recordCount] Harvesting failed. Retrying ...")
            } catch (Exception e) {
                log.error("Caught exception in doImport loop", e)
                break
            }
            lastRecordDatestamp = harvestResult.lastRecordDatestamp ?: from
            resumptionToken = harvestResult.resumptionToken
            elapsed = System.currentTimeMillis() - loadUrlTime
            if (elapsed > 6000) {
                log.warn("[$collection / $recordCount] Harvest took more than 6 seconds ($elapsed)")
            }
            log.debug("resumptionToken: $resumptionToken")
        }
        log.debug("Shutting down queue")
        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)
        log.debug("Import has completed in " + (System.currentTimeMillis() - startTime) + " milliseconds.")

        return new HarvestResult(numberOfDocuments: recordCount, numberOfDeleted: nrDeleted, numberOfDocumentsSkipped: skippedRecordCount, lastRecordDatestamp: lastRecordDatestamp)
    }

    String washXmlOfBadCharacters(String xmlString) {
        log.warn("Trying to recuperate by washing XML ...")
        StringBuilder sb = new StringBuilder(xmlString)
        for (int i=0;i<sb.length();i++)
            if (sb.charAt(i) < 0x09 || (sb.charAt(i) > 0x0D && sb.charAt(i) < 0x1F)) {
                log.warn("Found illegal character: ${sb.charAt(i)}")
                sb.setCharAt(i, '?' as char);
            }

        return sb.toString()
    }

    def harvest(URL url, Date startDate) {
        long elapsed = System.currentTimeMillis()
        Date recordDate
        Date now = new Date()
        def xmlString
        try {
            xmlString = normalizeString(url.text)
        } catch (ConnectException ce) {
            log.warn("Failed to connect to $url")
            return new HarvestResult(resumptionToken: null, lastRecordDatestamp: recordDate)
        }
        if ((System.currentTimeMillis() - elapsed) > 5000) {
            log.warn("[$collection / $recordCount] Load from URL ${url.toString()} took more than 5 seconds (${System.currentTimeMillis() - elapsed})")
        }
        def OAIPMH
        elapsed = System.currentTimeMillis()
        try {
            OAIPMH = new XmlSlurper(false,false).parseText(xmlString)
        } catch (org.xml.sax.SAXParseException spe) {
            if (xmlString != null && xmlString.length() > 0) {
                xmlString = washXmlOfBadCharacters(xmlString)
                try {
                    OAIPMH = new XmlSlurper(false,false).parseText(xmlString)
                } catch (org.xml.sax.SAXParseException sp) {
                    log.error("Failed to parse XML despite efforts to clean: $xmlString", sp)
                    throw new XmlParsingFailedException("Failing XML: ($xmlString)", sp)
                }
            } else {
                log.error("Failed to parse XML: $xmlString", spe)
                throw new XmlParsingFailedException("Failing XML: ($xmlString)", spe)
            }
        }
        if ((System.currentTimeMillis() - elapsed) > 1000) {
            log.warn("[$collection / $recordCount] XML slurping took more than 1 second (${System.currentTimeMillis() - elapsed})")
        }
        def documents = []
        elapsed = System.currentTimeMillis()
        def resumptionToken = OAIPMH.ListRecords.resumptionToken.text()
        for (it in OAIPMH.ListRecords.record) {
            String ds = this.collection
            String mdrecord = createString(it.metadata.record)
            if (mdrecord) {
                try {
                    recordDate = Date.parse(DATE_FORMAT, it.header.datestamp.toString())
                    log.debug("Date recordDate: ${recordDate}. String datestamp: ${it.header.datestamp.toString()}. Startdate: $startDate")
                    if (recordDate.before(startDate)) {
                        log.error("Encountered datestamp older (${recordDate}) than starttime (${startDate}) for record ${it.header.identifier}. Breaking.")
                        recordDate = startDate
                        resumptionToken = null
                        break
                    } else if (recordDate.after(now)) {
                        log.error("Encountered datestamp in the future (${recordDate}) for record ${it.header.identifier}. Breaking.")
                        recordDate = startDate
                        resumptionToken = null
                        break
                    }
                    MarcRecord record = MarcXmlRecordReader.fromXml(mdrecord)
                    Map document
                    log.trace("Marc record instantiated from XML.")
                    def aList = record.getDatafields("599").collect { it.getSubfields("a").data }.flatten()
                    if (!aList.contains("SUPPRESSRECORD")) {
                        document = createDocumentMap(record, recordDate, it.header, ds)
                    }
                    if (document) {
                        documents << document
                        recordCount++
                    } else {
                        skippedRecordCount++
                    }
                    runningTime = System.currentTimeMillis() - startTime
                } catch (Exception e) {
                    log.error("Failed! (${e.message}) for :\n$mdrecord", e)
                    break
                }
            } else if (it.header?.@status == 'deleted' || it.header?.@deleted == 'true') {
                recordDate = Date.parse(DATE_FORMAT, it.header.datestamp.toString())
                if (recordDate.before(startDate)) {
                    log.error("Encountered datestamp older (${recordDate}) than starttime (${startDate}) for record ${it.header.identifier}. Breaking.")
                    recordDate = startDate
                    resumptionToken = null
                    break
                } else if (recordDate.after(now)) {
                    log.error("Encountered datestamp in the future (${recordDate}) for record ${it.header.identifier}. Breaking.")
                    recordDate = startDate
                    resumptionToken = null
                    break
                }
                String legacyIdentifier = "/" + new URI(it.header.identifier.text()).getPath().split("/")[2 .. -1].join("/")
                String deleteIdentifier = LegacyIntegrationTools.generateId(legacyIdentifier)
                try {
                    log.info("Deleting record $legacyIdentifier ($deleteIdentifier)")
                    whelk.remove(deleteIdentifier)
                } catch (Exception e2) {
                    log.error("Whelk remove of $deleteIdentifier triggered exception.", e2)
                }
                nrDeleted++
            } else {
                log.error("Failed to handle record: " + createString(it))
                resumptionToken = null
                break
            }
        }
        if ((System.currentTimeMillis() - elapsed) > 5000) {
            log.warn("[$collection / $recordCount] Conversion of documents took more than 5 seconds (${System.currentTimeMillis() - elapsed})")
        }
        if (documents?.size() > 0) {
            addDocuments(documents)
        }

        return new HarvestResult(resumptionToken: resumptionToken, lastRecordDatestamp: recordDate)
    }

    Map createDocumentMap(MarcRecord record, Date recordDate, def header, String ds = this.collection) {
        def documentMap = [:]
        log.trace("Record preparation starts.")
        String recordId = "/"+this.collection+"/"+record.getControlfields("001").get(0).getData()
        String xlId = LegacyIntegrationTools.generateId(recordId)

        def manifest = ["identifier":xlId,"collection":ds]
        if (preserveTimestamps) {
            log.trace("Setting date: $recordDate")
            manifest.put(Document.MODIFIED_KEY, recordDate.getTime())
        }

        String originalIdentifier = null
        long originalModified = 0

        log.trace("Start check for 887")
        try {
            for (field in record.getDatafields("887")) {
                if (!field.getSubfields("2").isEmpty() && field.getSubfields("2").first().data == "librisxl") {
                    try {
                        def xlData = mapper.readValue(field.getSubfields("a").first().data, Map)
                        originalIdentifier = xlData.get("@id")
                        originalModified = xlData.get("modified") as long
                    } catch (Exception e) {
                        log.error("Failed to parse 887 as json for $xlId (${recordId})")
                    }
                }
            }
            if (originalIdentifier) {
                log.info("Detected an original Libris XL identifier in Marc data: ${originalIdentifier}, updating manifest.")
                manifest['identifier'] = originalIdentifier
                long marcRecordModified = getMarcRecordModificationTime(record)?.getTime()
                log.info("record timestamp: $marcRecordModified")
                log.info("    xl timestamp: $originalModified")
                long diff = (marcRecordModified - originalModified) / 1000
                log.info("/update time difference: $diff secs.")
                if (diff < 30) {
                    log.info("Record probably not edited in Voyager. Skipping ...")
                    return null
                }
            }
        } catch (NoSuchElementException nsee) {
            log.trace("Record doesn't have a 887 field.")
        }

        log.trace("887 check complete.")
        def meta = [:]

        if (header.setSpec) {
            for (spec in header.setSpec) {
                meta.get("oaipmhSetSpecs", []).add(spec.toString())
            }
        }
        log.trace("Record prepared.")

        documentMap['record'] = record
        if (meta) {
            manifest['extraData'] = meta
        }
        documentMap['manifest'] = manifest
        documentMap['originalIdentifier'] = originalIdentifier

        return documentMap
    }

    Date getMarcRecordModificationTime(MarcRecord record) {
        String datetime = record.getControlfields("005")?.get(0).getData()
        if (datetime) {
            return Date.parse("yyyyMMddHHmmss.S",datetime)
        }
        return null
    }

    void addDocuments(final List documents) {
        if (tickets.availablePermits() < 10) {
            log.debug("Trying to acquire semaphore for adding to queue. ${tickets.availablePermits()} available.")
        }
        tickets.acquire()
        queue.execute({
            try {
                def convertedDocs = [:]
                documents.each {
                    try {
                        if (marcFrameConverter) {
                            log.trace("Conversion starts.")
                            it.manifest['contentType'] = "application/x-marc-json"
                            Document doc = new Document(MarcJSONConverter.toJSONMap(it.record), it.manifest)

                            doc = marcFrameConverter.convert(doc)
                            //Files.write(Paths.get("/tmp/${doc.id}_flat.jsonld"), doc.dataAsString.getBytes("utf-8"), StandardOpenOption.CREATE)


                            log.trace("Convestion finished.")
                            if (it.originalIdentifier) {
                                def dataMap = doc.data
                                dataMap['controlNumber'] = it.record.getControlfields("001").get(0).getData()
                                doc = doc.withData(dataMap)
                                doc.addIdentifier("/"+this.collection+"/"+it.record.getControlfields("001").get(0).getData())
                            }
                            if (enhancer) {
                                log.trace("Enhancing starts.")
                                doc = enhancer.filter(doc)
                                log.trace("Enhancing finished.")
                            }
                            if (doc) {
                                if (!convertedDocs.containsKey(doc.collection)) {
                                    convertedDocs.put(doc.collection, [])
                                }
                                convertedDocs[(doc.collection)] << doc
                            }
                        } else {
                            if (!convertedDocs.containsKey(it.manifest.collection)) {
                                convertedDocs.put(it.manifest.collection, [])
                            }
                            it.manifest['contentType'] = "application/x-marc-json"
                            convertedDocs[(it.manifest.collection)] << new Document(MarcJSONConverter.toJSONMap(it.record), it.manifest)// whelk.createDocument(MarcJSONConverter.toJSONMap(it.record), it.manifest)
                        }
                    } catch (Exception e) {
                        log.error("Exception in conversion", e)
                    }
                }

                if (convertedDocs.size() > 0) {
                    try {
                        log.debug("Adding ${convertedDocs.size()} documents to whelk.")
                        long elapsed = System.currentTimeMillis()
                        convertedDocs.each { ds, docList ->
                            log.debug("Bulk saving ${docList.size()} documents to collection $ds ...")
                            this.whelk.bulkStore(docList)
                        }
                        if ((System.currentTimeMillis() - elapsed) > 10000) {
                            log.warn("[$collection / $recordCount] Bulk add took more than 10 seconds (${System.currentTimeMillis() - elapsed})")
                        }
                    } catch (WhelkAddException wae) {
                        log.warn("Failed adding: ${wae.message} (${wae.failedIdentifiers})")
                            throw wae
                    } catch (Exception e) {
                        log.error("Exception on bulkAdd: ${e.message}", e)
                        throw e
                    }
                }
            } finally {
                tickets.release()
            }
        } as Runnable)
    }

    private void getAuthentication() {
        if (username && password) {
            log.debug("Setting username (${username}) and password (${password})")
        } else {
            log.debug("Not setting username/password")
        }
        try {
            Authenticator.setDefault(new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray())
                    }
                });
        } catch (Exception ex) {
            log.error("Exception getting authentication credentials: $ex")
        }
    }

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind{
            out << root
        }
    }

    void cancel() {
        this.cancelled = true
    }
}
