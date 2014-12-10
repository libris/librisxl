package whelk.importer

import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult
import groovy.util.logging.Slf4j as Log

import java.text.*
import java.util.concurrent.*

import whelk.*
import whelk.exception.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*
import whelk.plugin.*
import whelk.converter.MarcJSONConverter
import whelk.util.Tools

@Log
class OldOAIPMHImporter extends BasicPlugin implements Importer {

    static SERVICE_BASE_URL = "http://data.libris.kb.se/{dataset}/oaipmh"

    Whelk whelk
    String dataset

    String serviceUrl
    int recordCount = 0
    int nrDeleted = 0
    long startTime = 0

    boolean picky = true
    boolean silent = false
    boolean preserveTimestamps = true

    long runningTime = 0


    ExecutorService queue
    Semaphore tickets
    int numberOfThreads = 1
    MarcFrameConverter marcFrameConverter
    JsonLDLinkCompleterFilter enhancer

    boolean cancelled = false

    List errorMessages = []

    OldOAIPMHImporter(Map settings) {
        this.serviceUrl = settings.get('serviceUrl',SERVICE_BASE_URL)
        this.preserveTimestamps = settings.get("preserveTimestamps", true)
        this.numberOfThreads = settings.get("numberOfThreads", numberOfThreads)
    }

    void bootstrap(String whelkId) {
        marcFrameConverter = plugins.find { it instanceof MarcFrameConverter }
        enhancer = plugins.find { it instanceof JsonLDLinkCompleterFilter }
    }

    int doImport(String dataset, int nrOfDocs) {
        return doImport(dataset, null, nrOfDocs)
    }

    int doImport(String dataset, String startResumptionToken = null, int nrOfDocs = -1, boolean silent = false, boolean picky = true, Date from = null) {
        getAuthentication()
        this.cancelled = false
        this.dataset = dataset
        this.picky = picky
        this.silent = silent
        this.recordCount = 0
        this.nrDeleted = 0
        if (!serviceUrl) {
            serviceUrl = SERVICE_BASE_URL
        }
        String baseUrl = serviceUrl.replace("{dataset}", dataset)

        String urlString = baseUrl + "?verb=ListRecords&metadataPrefix=marcxml"

        def versioningSettings = [:]

        tickets = new Semaphore(100)

        if (from) {
            urlString = urlString + "&from=" + from.format("yyyy-MM-dd'T'HH:mm:ss'Z'")
        } else {
            for (st in this.whelk.getStorages()) {
                log.debug("Turning off versioning in ${st.id}")
                // Preserve original setting
                versioningSettings.put(st.id, st.versioning)
                //st.versioning = false
            }
        }
        //queue = Executors.newSingleThreadExecutor()
        queue = Executors.newFixedThreadPool(numberOfThreads)
        startTime = System.currentTimeMillis()
        URL url
        if (startResumptionToken) {
            url = new URL(baseUrl + "?verb=ListRecords&resumptionToken=" + startResumptionToken)
            log.debug("Harvesting OAIPMH data from ${url.toString()}. Pickymode: $picky")
        } else {
            url = new URL(urlString)
            log.debug("Harvesting OAIPMH data from $urlString. Pickymode: $picky")
        }
        String resumptionToken = harvest(url)
        log.debug("resumptionToken: $resumptionToken")
        long loadUrlTime = startTime
        long elapsed = 0
        while (!cancelled && resumptionToken && (nrOfDocs == -1 || recordCount <  nrOfDocs)) {
            loadUrlTime = System.currentTimeMillis()
            url = new URL(baseUrl + "?verb=ListRecords&resumptionToken=" + resumptionToken)
            log.trace("Harvesting $url")
            try {
                String rtok = harvest(url)
                resumptionToken = rtok
            } catch (XmlParsingFailedException xpfe) {
                log.warn("[$dataset / $recordCount] Harvesting failed. Retrying ...")
            }
            elapsed = System.currentTimeMillis() - loadUrlTime
            if (elapsed > 6000) {
                log.warn("[$dataset / $recordCount] Harvest took more than 6 seconds ($elapsed)")
            }
            log.debug("resumptionToken: $resumptionToken")
        }
        log.debug("Flushing data ...")
        queue.execute({
            this.whelk.flush()
            log.debug("Resetting versioning setting for storages")
            if (!from) {
                for (st in this.whelk.getStorages()) {
                    st.versioning = versioningSettings.get(st.id)
                }
            }
        } as Runnable)
        log.debug("Shutting down queue")
        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)
        log.info("Import has completed in " + (System.currentTimeMillis() - startTime) + " milliseconds.")

        return recordCount
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

    String harvest(URL url) {
        long elapsed = System.currentTimeMillis()
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
        def marcdocuments = []
        elapsed = System.currentTimeMillis()
        OAIPMH.ListRecords.record.each {
            String mdrecord = createString(it.metadata.record)
            if (mdrecord) {
                try {
                    MarcRecord record = MarcXmlRecordReader.fromXml(mdrecord)
                    String recordId = "/"+this.dataset+"/"+record.getControlfields("001").get(0).getData()

                    def entry = ["identifier":recordId,"dataset":this.dataset]
                    def aList = record.getDatafields("599").collect { it.getSubfields("a").data }.flatten()
                    if ("SUPPRESSRECORD" in aList) {
                        log.debug("Record ${entry.identifier} is suppressed. Next ...")
                        return
                    }
                    String originalIdentifier = null
                    try {
                        originalIdentifier = record.getDatafields("901").collect { it.getSubfields("i").data}.flatten().first()
                        if (originalIdentifier) {
                            log.info("Detected an original Libris XL identifier in Marc data: ${originalIdentifier}, updating entry.")
                            entry['identifier'] = originalIdentifier
                        }
                    } catch (NoSuchElementException nsee) {
                        log.trace("Record doesn't have a 901i field.")
                    }

                    if (preserveTimestamps && it.header.datestamp) {
                        def date = Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'", it.header.datestamp.toString())
                        log.trace("Setting date: $date")
                        entry.put(Document.MODIFIED_KEY, date.getTime())
                    }

                    def meta = [:]

                    if (it.header.setSpec) {
                        for (spec in it.header.setSpec) {
                            meta.get("oaipmhSetSpecs", []).add(spec.toString())
                        }
                    }

                    try {
                        if (marcFrameConverter) {
                            def doc = marcFrameConverter.doConvert(record, ["entry":entry,"meta":meta])
                            if (originalIdentifier) {
                                def dataMap = doc.dataAsMap
                                dataMap['sameAs'] = ['@id':recordId]
                                doc = doc.withData(dataMap)
                            }
                            if (enhancer) {
                                doc = enhancer.filter(doc)
                            }
                            documents << doc
                        }
                        def marcmeta = meta
                        marcmeta.put("oaipmhHeader", createString(it.header))
                        marcdocuments << whelk.createDocument("application/marcxml+xml").withMetaEntry(["entry":entry, "meta":marcmeta]).withData(mdrecord)
                    } catch (Exception e) {
                        log.error("Conversion failed for id ${entry.identifier}", e)
                    }
                    recordCount++
                    def velocityMsg = ""
                    runningTime = System.currentTimeMillis() - startTime
                    if (!silent) {
                        velocityMsg = "Current velocity: ${recordCount/(runningTime/1000)}."
                        Tools.printSpinner("Running OAIPMH ${this.dataset} import. ${recordCount} documents imported sofar. $velocityMsg", recordCount)
                    }
                } catch (Exception e) {

                    log.error("Failed! (${e.message}) for :\n$mdrecord", e)
                    if (picky) {
                        log.error("Picky mode enabled. Throwing exception", e)
                        throw e
                    }
                }
            } else if (it.header.@deleted == 'true') {
                String deleteIdentifier = "/" + new URI(it.header.identifier.text()).getPath().split("/")[2 .. -1].join("/")
                    try {
                        whelk.remove(deleteIdentifier)
                    } catch (Exception e2) {
                        log.error("Whelk remove of $deleteIdentifier triggered exception.", e2)
                    }
                nrDeleted++
            } else {
                throw new WhelkRuntimeException("Failed to handle record: " + createString(it))
            }
        }
        if ((System.currentTimeMillis() - elapsed) > 5000) {
            log.warn("[$dataset / $recordCount] Conversion of documents took more than 5 seconds (${System.currentTimeMillis() - elapsed})")
        }
        if (documents?.size() > 0) {
            addDocuments(documents)
        }
        if (marcdocuments?.size() > 0) {
            addDocuments(marcdocuments)
        }

        if (!OAIPMH.ListRecords.resumptionToken.text()) {
            log.trace("Last page is $xmlString")
        }
        return OAIPMH.ListRecords.resumptionToken

    }

    void addDocuments(final List documents) {
        if (tickets.availablePermits() < 10) {
            log.info("Trying to acquire semaphore for adding to queue. ${tickets.availablePermits()} available.")
        }
        tickets.acquire()
        queue.execute({
            try {
                log.debug("Adding ${documents.size()} documents to whelk.")
                long elapsed = System.currentTimeMillis()
                //def storage = whelk.getStorage(document.get(0).contentType)
                //storage.bulkStore(documents)
                this.whelk.bulkAdd(documents, documents.get(0).contentType)
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
            Properties properties = new Properties()
            properties.load(this.getClass().getClassLoader().getResourceAsStream("oaipmh.properties"))
            final String username = properties.getProperty("username")
            final String password = properties.getProperty("password")
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
