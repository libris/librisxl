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
import se.kb.libris.conch.converter.MarcJSONConverter
import se.kb.libris.conch.Tools

@Log
class OAIPMHImporter {

    Whelk whelk
    String resource
    int nrImported = 0
    int nrDeleted = 0
    long startTime = 0
    boolean picky = true

    ExecutorService queue
    File failedLog
    File exceptionLog

    OAIPMHImporter(Whelk toWhelk, String fromResource) {
        failedLog = new File("failed_ids.log")
        exceptionLog = new File("exceptions.log")
        this.whelk = toWhelk
        this.resource = fromResource
    }

    int doImport(Date from = null, int nrOfDocs = -1, boolean picky = true) {
        getAuthentication()
        this.picky = picky
        String urlString = "http://data.libris.kb.se/"+this.resource+"/oaipmh/?verb=ListRecords&metadataPrefix=marcxml"
        if (from) {
            urlString = urlString + "&from=" + from.format("yyyy-MM-dd'T'HH:mm:ss'Z'")
        }
        queue = Executors.newSingleThreadExecutor()
        startTime = System.currentTimeMillis()
        log.info("Harvesting OAIPMH data from $urlString. Pickymode: $picky")
        URL url = new URL(urlString)
        String resumptionToken = harvest(url)
        log.debug("resumptionToken: $resumptionToken")
        while (resumptionToken && (nrOfDocs == -1 || nrImported <  nrOfDocs)) {
            url = new URL("http://data.libris.kb.se/" + this.resource + "/oaipmh/?verb=ListRecords&resumptionToken=" + resumptionToken)
            resumptionToken = harvest(url)
            log.debug("resumptionToken: $resumptionToken")
        }
        queue.shutdown()
        return nrImported
    }


    String harvest(URL url) {
        def xmlString = normalizeString(url.text)
        def OAIPMH
        try {
            OAIPMH = new XmlSlurper(false,false).parseText(xmlString)
        } catch (org.xml.sax.SAXParseException spe) {
            log.error("Failed to parse XML: $xmlString")
            throw spe
        }
        def documents = []
        OAIPMH.ListRecords.record.each {
            def mdrecord = createString(it.metadata.record)
            if (mdrecord) {
                MarcRecord record = MarcXmlRecordReader.fromXml(mdrecord)

                String id = record.getControlfields("001").get(0).getData()
                String jsonRec = MarcJSONConverter.toJSONString(record)

                def links = []
                //def tags = new HashSet<Map<String,Object>>()
                if (it.header.setSpec) {
                    for (sS in it.header.setSpec) {
                        if (sS.toString().startsWith("authority:")) {
                            links.add(["identifier":new String("/auth/" + sS.toString().substring(10)), "type":"auth"])
                        }
                        /* TODO: This should maybe be in holdings?
                        if (sS.toString().startsWith("location:")) {
                            tags.add(["location", sS.toString().substring(9)])
                        }
                        */
                        if (sS.toString().startsWith("bibid:")) {
                            links.add(["identifier":new String("/bib/" + sS.toString().substring(6)), "type":"bib"])
                        }
                    }
                }
                def doc
                try {
                    doc = whelk.createDocument(jsonRec.getBytes("UTF-8"), ["identifier":"/"+this.resource+"/"+id,"contentType":"application/x-marc-json"], ["links": links])
                    documents << doc
                    nrImported++
                    Tools.printSpinner("Running OAIPMH ${this.resource} import. ${nrImported} documents imported sofar.", nrImported)
                } catch (Exception e) {
                    log.error("Failed! (${e.message}) for :\n$mdrecord")
                    if (picky) {
                        throw e
                    }
                }
            } else if (it.header.@deleted == 'true') {
                String deleteIdentifier = "/" + new URI(it.header.identifier.text()).getPath().split("/")[2 .. -1].join("/")
                whelk.remove(new URI(deleteIdentifier))
                nrDeleted++
            } else {
                throw new WhelkRuntimeException("Failed to handle record: " + createString(it))
            }
        }
        //whelk.bulkAdd(documents)
        addDocuments(documents)
        int sizeOfBatch = documents.size()


        if (!OAIPMH.ListRecords.resumptionToken.text()) {
            log.trace("Last page is $xmlString")
        }
        return OAIPMH.ListRecords.resumptionToken
    }

    void addDocuments(final List documents) {
        queue.execute({
            try {
                this.whelk.bulkAdd(documents)
            } catch (WhelkAddException wae) {
                for (fi in wae.failedIdentifiers) {
                    failedLog << "$fi\n"
                }
            } catch (Exception e) {
                e.printStackTrace(new FileWriter(exceptionLog, true))
            }
        } as Runnable)
    }

    private void getAuthentication() {
        try {
            Properties properties = new Properties()
            properties.load(this.getClass().getClassLoader().getResourceAsStream("whelks-core.properties"))
            final String username = properties.getProperty("username")
            final String password = properties.getProperty("password")
            Authenticator.setDefault(new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray())
                    }
                });
        } catch (Exception ex) {
            log.error("Exception: $ex")
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
}
