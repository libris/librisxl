package se.kb.libris.whelks.importers

import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult
import groovy.util.logging.Slf4j as Log

import java.text.*
import java.util.concurrent.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.util.marc.*
import se.kb.libris.util.marc.io.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.conch.converter.MarcJSONConverter
import se.kb.libris.conch.Tools

@Log
class OAIPMHImporter extends BasicPlugin implements Importer {

    static SERVICE_BASE_URL = "http://data.libris.kb.se/"
    static final String OUT_CONTENT_TYPE = "text/oaipmh+xml"

    Whelk whelk
    String dataset

    String serviceUrl
    int nrImported = 0
    int nrDeleted = 0
    long startTime = 0
    boolean picky = true
    boolean silent = false

    // Stat tools
    long meanTime
    int sizeOfBatch

    ExecutorService queue
    StreamingMarkupBuilder markupBuilder = new StreamingMarkupBuilder()
    XmlSlurper slurper = new XmlSlurper(false, false)

    List<String> errorMessages

    boolean cancelled = false

    OAIPMHImporter() {
        this.serviceUrl = null
    }

    OAIPMHImporter(Map settings) {
        this.serviceUrl = settings.get('serviceUrl',null)
    }

    int doImport(String dataset, int nrOfDocs = -1, boolean silent = false, boolean picky = true, Date from = null) {
        getAuthentication()
        this.cancelled = false
        this.dataset = dataset
        this.picky = picky
        this.silent = silent
        this.serviceUrl = (serviceUrl ?: SERVICE_BASE_URL + dataset+"/oaipmh/")

        String urlString = serviceUrl + "?verb=ListRecords&metadataPrefix=marcxml"

        this.errorMessages = []

        if (from) {
            urlString = urlString + "&from=" + from.format("yyyy-MM-dd'T'HH:mm:ss'Z'")
        }
        queue = Executors.newSingleThreadExecutor()
        startTime = System.currentTimeMillis()
        log.info("Harvesting OAIPMH data from $urlString. Pickymode: $picky")
        URL url = new URL(urlString)
        String resumptionToken = harvest(url)
        log.debug("resumptionToken: $resumptionToken")
        long loadUrlTime = startTime
        long elapsed = 0
        while (!cancelled && resumptionToken && (nrOfDocs == -1 || nrImported <  nrOfDocs)) {
            loadUrlTime = System.currentTimeMillis()
            url = new URL(serviceUrl + "?verb=ListRecords&resumptionToken=" + resumptionToken)
            log.trace("Harvesting $url")
            try {
                String rtok = harvest(url)
                resumptionToken = rtok
            } catch (XmlParsingFailedException xpfe) {
                log.warn("Harvesting failed. Retrying ...")
            }
            elapsed = System.currentTimeMillis() - loadUrlTime
            if (elapsed > 6000) {
                log.warn("Harvest took more than 3 seconds ($elapsed)")
            }
            log.debug("resumptionToken: $resumptionToken")
        }
        log.info("Flushing data ...")
        queue.execute({
            this.whelk.flush()
        } as Runnable)
        log.debug("Shutting down queue")
        queue.shutdown()
        return nrImported
    }


    String harvest(URL url) {
        long elapsed = System.currentTimeMillis()
        def xmlString = normalizeString(url.text)
        if ((System.currentTimeMillis() - elapsed) > 5000) {
            log.warn("Load from URL ${url.toString()} took more than 5 seconds (${System.currentTimeMillis() - elapsed})")
        }
        def OAIPMH
        try {
            elapsed = System.currentTimeMillis()
            OAIPMH = slurper.parseText(xmlString)
            if ((System.currentTimeMillis() - elapsed) > 1000) {
                log.warn("XML slurping took more than 1 second (${System.currentTimeMillis() - elapsed})")
            }
        } catch (org.xml.sax.SAXParseException spe) {
            //errorMessages << new String("Failed to parse XML: $xmlString\nReason: ${spe.message}")
            log.error("Failed to parse XML: $xmlString", spe)
            throw new XmlParsingFailedException("Failing XML: ($xmlString)", spe)
        }
        def documents = []
        elapsed = System.currentTimeMillis()
        OAIPMH.ListRecords.record.each {
            def rawrecord = createString(it)
            def mdrecord = createString(it.metadata.record)
            if (mdrecord) {
                MarcRecord record = MarcXmlRecordReader.fromXml(mdrecord)

                String id = record.getControlfields("001").get(0).getData()

                def doc
                try {
                    doc = new Document()
                        .withData(rawrecord.getBytes("UTF-8"))
                        .withEntry(["identifier":"/"+this.dataset+"/"+id,"dataset":this.dataset,"contentType":OUT_CONTENT_TYPE])
                    documents << doc
                    nrImported++
                    sizeOfBatch++
                    def velocityMsg = ""
                    if (sizeOfBatch && meanTime) {
                        velocityMsg = "Current velocity: " + (1000*(sizeOfBatch / (System.currentTimeMillis() - meanTime))) + " docs/second."
                    }
                    if (!silent) {
                        Tools.printSpinner("Running OAIPMH ${this.dataset} import. ${nrImported} documents imported sofar. $velocityMsg", nrImported)
                    }
                } catch (Exception e) {
                    //errorMessages << new String("Failed! (${e.message}) for:\n$mdrecord")

                    log.error("Failed! (${e.message}) for :\n$mdrecord", e)
                    if (picky) {
                        log.error("Picky mode enabled. Throwing exception", e)
                        throw e
                    }
                }
            } else if (it.header.@deleted == 'true') {
                String deleteIdentifier = "/" + new URI(it.header.identifier.text()).getPath().split("/")[2 .. -1].join("/")
                try {
                    whelk.remove(new URI(deleteIdentifier))
                } catch (Exception e2) {
                    //errorMessages << new String("Whelk remove of $deleteIdentifier triggered exception: ${e2.message}")
                    log.error("Whelk remove of $deleteIdentifier triggered exception.", e2)
                }
                nrDeleted++
            } else {
                throw new WhelkRuntimeException("Failed to handle record: " + createString(it))
            }
        }
        if ((System.currentTimeMillis() - elapsed) > 3000) {
            log.warn("Deserializing of documents took more than 3 seconds (${System.currentTimeMillis() - elapsed})")
        }
        meanTime = System.currentTimeMillis()
        addDocuments(documents)
        sizeOfBatch = 0

        if (!OAIPMH.ListRecords.resumptionToken.text()) {
            log.trace("Last page is $xmlString")
        }
        return OAIPMH.ListRecords.resumptionToken
    }

    void addDocuments(final List documents) {
        queue.execute({
            try {
                log.debug("Adding ${documents.size()} documents to whelk.")
                long elapsed = System.currentTimeMillis()
                this.whelk.bulkAdd(documents)
                if ((System.currentTimeMillis() - elapsed) > 3000) {
                    log.warn("Bulk add took more than 3 seconds (${System.currentTimeMillis() - elapsed})")
                }
            } catch (WhelkAddException wae) {
                //errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed adding: ${wae.message} (${wae.failedIdentifiers})")
            } catch (Exception e) {
                log.error("Exception on bulkAdd: ${e.message}", e)
                /*
                StringWriter sw = new StringWriter()
                e.printStackTrace(new PrintWriter(sw))
                errorMessages << new String("Exception on add: ${sw.toString()}")
                */
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
        return markupBuilder.bind{
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
