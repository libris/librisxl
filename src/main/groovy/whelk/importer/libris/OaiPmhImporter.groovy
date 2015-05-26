package whelk.importer.libris

import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult
import groovy.util.logging.Slf4j as Log

import java.text.*
import java.util.concurrent.*

import whelk.*
import whelk.result.*
import whelk.exception.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*
import whelk.plugin.*
import whelk.plugin.libris.*
import whelk.converter.MarcJSONConverter
import whelk.util.Tools

@Log
class OaiPmhImporter extends BasicPlugin implements Importer {

    static SERVICE_BASE_URL = "http://data.libris.kb.se/{dataset}/oaipmh"

    Whelk whelk
    String dataset

    String serviceUrl
    int recordCount = 0
    int nrDeleted = 0
    long startTime = 0

    boolean preserveTimestamps = true

    long runningTime = 0

    boolean prepareDocuments = true

    ExecutorService queue
    Semaphore tickets
    MarcFrameConverter marcFrameConverter
    JsonLDLinkCompleterFilter enhancer

    boolean cancelled = false

    OaiPmhImporter(Map settings) {
        this.serviceUrl = settings.get('serviceUrl',SERVICE_BASE_URL)
        this.preserveTimestamps = settings.get("preserveTimestamps", true)
    }

    void bootstrap() {
        marcFrameConverter = plugins.find { it instanceof MarcFrameConverter }
        enhancer = plugins.find { it instanceof JsonLDLinkCompleterFilter }
    }

    ImportResult doImport(String dataset, int nrOfDocs) {
        return doImport(dataset, null, nrOfDocs)
    }

    @groovy.transform.Synchronized
    ImportResult doImport(String dataset, String startResumptionToken = null, int nrOfDocs = -1, boolean silent = false, boolean picky = true, Date from = null) {
        getAuthentication()
        this.cancelled = false
        this.dataset = dataset
        this.recordCount = 0
        this.nrDeleted = 0
        if (!serviceUrl) {
            serviceUrl = SERVICE_BASE_URL
        }
        String baseUrl = serviceUrl.replace("{dataset}", dataset)

        String urlString = baseUrl + "?verb=ListRecords&metadataPrefix=marcxml"

        def versioningSettings = [:]

        tickets = new Semaphore(100)

        /*
        if (from == null) {
            for (st in this.whelk.getStorages()) {
                log.debug("Turning off versioning in ${st.id}")
                // Preserve original setting
                versioningSettings.put(st.id, st.versioning)
                st.versioning = false
            }
            from = new Date(0L)
            prepareDocuments = false
        }
        */

        if (from) {
            urlString = urlString + "&from=" + from.format("yyyy-MM-dd'T'HH:mm:ss'Z'")
        }
        log.debug("urlString: $urlString")
        //queue = Executors.newSingleThreadExecutor()
        //queue = Executors.newFixedThreadPool(numberOfThreads)
        queue = Executors.newWorkStealingPool()
        startTime = System.currentTimeMillis()
        URL url
        if (startResumptionToken) {
            url = new URL(baseUrl + "?verb=ListRecords&resumptionToken=" + startResumptionToken)
            log.debug("Harvesting OAIPMH data from ${url.toString()}. Pickymode: $picky")
        } else {
            url = new URL(urlString)
            log.debug("Harvesting OAIPMH data from $urlString. Pickymode: $picky")
        }

        def harvestResult = null
        Date lastRecordDatestamp = from
        def resumptionToken = null
        long loadUrlTime = startTime
        long elapsed = 0

        while (elapsed == 0 ||
            (!cancelled && resumptionToken && (nrOfDocs == -1 || recordCount <  nrOfDocs))) {
            loadUrlTime = System.currentTimeMillis()
            url = new URL(baseUrl + "?verb=ListRecords&resumptionToken=" + resumptionToken)
            log.trace("Harvesting $url")
            try {
                harvestResult = harvest(url)
            } catch (XmlParsingFailedException xpfe) {
                log.warn("[$dataset / $recordCount] Harvesting failed. Retrying ...")
            } catch (Exception e) {
                log.error("Caught exception in doImport loop", e)
                break
            }
            lastRecordDatestamp = harvestResult.lastRecordDatestamp ?: from
            resumptionToken = harvestResult.resumptionToken
            elapsed = System.currentTimeMillis() - loadUrlTime
            if (elapsed > 6000) {
                log.warn("[$dataset / $recordCount] Harvest took more than 6 seconds ($elapsed)")
            }
            log.debug("resumptionToken: $resumptionToken")
        }
        log.debug("Flushing data ...")
        /*
        queue.execute({
            this.whelk.flush()
            log.debug("Resetting versioning setting for storages")
            if (!from) {
                for (st in this.whelk.getStorages()) {
                    st.versioning = versioningSettings.get(st.id)
                }
            }
        } as Runnable)
        */
        log.debug("Shutting down queue")
        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)
        log.debug("Import has completed in " + (System.currentTimeMillis() - startTime) + " milliseconds.")

        return new ImportResult(numberOfDocuments: recordCount, numberOfDeleted: nrDeleted, lastRecordDatestamp: lastRecordDatestamp)
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

    def harvest(URL url) {
        long elapsed = System.currentTimeMillis()
        Date recordDate
        def xmlString = normalizeString(url.text)
        if ((System.currentTimeMillis() - elapsed) > 5000) {
            log.warn("[$dataset / $recordCount] Load from URL ${url.toString()} took more than 5 seconds (${System.currentTimeMillis() - elapsed})")
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
            log.warn("[$dataset / $recordCount] XML slurping took more than 1 second (${System.currentTimeMillis() - elapsed})")
        }
        def documents = []
        elapsed = System.currentTimeMillis()
        def resumptionToken = OAIPMH.ListRecords.resumptionToken.text()
        for (it in OAIPMH.ListRecords.record) {
            String mdrecord = createString(it.metadata.record)
            if (mdrecord) {
                try {
                    MarcRecord record = MarcXmlRecordReader.fromXml(mdrecord)
                    def aList = record.getDatafields("599").collect { it.getSubfields("a").data }.flatten()
                    if ("SUPPRESSRECORD" in aList) {
                        log.debug("Record ${entry.identifier} is suppressed. Next ...")
                        continue
                    }
                    log.trace("Marc record instantiated from XML.")
                    recordDate = Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'", it.header.datestamp.toString())
                    def document = createDocumentMap(record, recordDate, it.header)
                    documents << document
                    recordCount++
                    runningTime = System.currentTimeMillis() - startTime
                } catch (Exception e) {
                    log.error("Failed! (${e.message}) for :\n$mdrecord", e)
                    break
                }
            } else if (it.header?.@status == 'deleted' || it.header?.@deleted == 'true') {
                recordDate = Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'", it.header.datestamp.toString())
                String deleteIdentifier = "/" + new URI(it.header.identifier.text()).getPath().split("/")[2 .. -1].join("/")
                    try {
                        whelk.remove(deleteIdentifier, this.dataset)
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
            log.warn("[$dataset / $recordCount] Conversion of documents took more than 5 seconds (${System.currentTimeMillis() - elapsed})")
        }
        if (documents?.size() > 0) {
            addDocuments(documents)
        }

        return new HarvestResult(resumptionToken: resumptionToken, lastRecordDatestamp: recordDate)
    }

    Map createDocumentMap(MarcRecord record, Date recordDate, def header) {
        def documentMap = [:]
        log.trace("Record preparation starts.")
        String recordId = "/"+this.dataset+"/"+record.getControlfields("001").get(0).getData()

        def entry = ["identifier":recordId,"dataset":this.dataset]
        if (preserveTimestamps) {
            log.trace("Setting date: $recordDate")
            entry.put(Document.MODIFIED_KEY, recordDate.getTime())
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
                        log.error("Failed to parse 887 as json for $recordId")
                    }
                }
            }
            if (originalIdentifier) {
                log.info("Detected an original Libris XL identifier in Marc data: ${originalIdentifier}, updating entry.")
                entry['identifier'] = originalIdentifier
                long marcRecordModified = getMarcRecordModificationTime(record)?.getTime()
                log.info("record timestamp: $marcRecordModified")
                log.info("    xl timestamp: $originalModified")
                long diff = (marcRecordModified - originalModified) / 1000
                log.info("/update time difference: $diff secs.")
                if (diff < 60) {
                    log.info("Record probably not edited in Voyager. Skipping ...")
                    return null
                }
            }
        } catch (NoSuchElementException nsee) {
            log.trace("Record doesn't have a 877 field.")
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
        documentMap['entry'] = entry
        documentMap['meta'] = meta
        documentMap['originalIdentifier'] = originalIdentifier

        return documentMap
    }

    class HarvestResult {
        String resumptionToken
        Date lastRecordDatestamp
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
            def convertedDocs = []
            documents.each {
                try {
                    if (marcFrameConverter) {
                        log.trace("Conversion starts.")
                        def doc = marcFrameConverter.doConvert(it.record, ["entry":it.entry,"meta":it.meta])
                        log.trace("Convestion finished.")
                        if (it.originalIdentifier) {
                            def dataMap = doc.dataAsMap
                            dataMap['controlNumber'] = it.record.getControlfields("001").get(0).getData()
                            doc = doc.withData(dataMap)
                            doc.addIdentifier("/"+this.dataset+"/"+it.record.getControlfields("001").get(0).getData())
                        }
                        if (enhancer) {
                            log.trace("Enhancing starts.")
                            doc = enhancer.filter(doc)
                            log.trace("Enhancing finished.")
                        }
                        if (doc) {
                            convertedDocs << doc
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception in conversion", e)
                }
            }

            try {
                log.debug("Adding ${convertedDocs.size()} documents to whelk.")
                long elapsed = System.currentTimeMillis()
                this.whelk.bulkAdd(convertedDocs, convertedDocs.get(0).dataset, convertedDocs.get(0).contentType, prepareDocuments)
                if ((System.currentTimeMillis() - elapsed) > 10000) {
                    log.warn("[$dataset / $recordCount] Bulk add took more than 10 seconds (${System.currentTimeMillis() - elapsed})")
                }
            } catch (WhelkAddException wae) {
                log.warn("Failed adding: ${wae.message} (${wae.failedIdentifiers})")
                throw wae
            } catch (Exception e) {
                log.error("Exception on bulkAdd: ${e.message}", e)
                throw e
            } finally {
                tickets.release()
            }
        } as Runnable)
    }

    private void getAuthentication() {
        try {
            final String username = whelk.props.get("oaipmhUsername")
            final String password = whelk.props.get("oaipmhPassword")
            Authenticator.setDefault(new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray())
                    }
                });
        } catch (Exception ex) {
            log.error("Exception getting authentication credentials: $ex")
        }
    }

    String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            log.trace("Normalizing ...")
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
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

class XmlParsingFailedException extends Exception {
    XmlParsingFailedException() {
        super("Parse failed. Most likely, the received document was empty. Or null.")
    }
    XmlParsingFailedException(String msg) {
        super(msg)
    }
    XmlParsingFailedException(Throwable t) {
        super("Parse failed. Most likely, the received document was empty. Or null.", t)
    }
    XmlParsingFailedException(String msg, Throwable t) {
        super(msg, t)
    }

}
