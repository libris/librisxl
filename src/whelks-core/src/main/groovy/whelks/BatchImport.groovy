package se.kb.libris.whelks.imports;

import groovy.util.logging.Slf4j as Log
import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult

import groovy.transform.Synchronized

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.text.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Controlfield;
import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.Subfield;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.Iso2709Serializer;
import se.kb.libris.conch.converter.MarcJSONConverter;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.basic.*;
import se.kb.libris.whelks.plugin.*;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.util.concurrent.ThreadPoolExecutor

@Log
class BatchImport {

    private String resource;

    private long starttime = 0;
    private int NUMBER_OF_IMPORTERS = 20
    def pool

    List docList = Collections.synchronizedList(new LinkedList())

    public BatchImport() {}


    public BatchImport(String resource) {
        this.resource = resource;
    }

     URL getBaseUrl(Date from, Date until) {
        //return "http://data.libris.kb.se/"+this.resource+"/oaipmh/?verb=ListRecords&metadataPrefix=marcxml&from=2012-05-23T15:21:27Z";
        String url = "http://data.libris.kb.se/"+this.resource+"/oaipmh/?verb=ListRecords&metadataPrefix=marcxml";
        if (from != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            url = url + "&from=" + sdf.format(from);
            if (until) {
                url = url + "&until=" + sdf.format(until)
                log.debug("" + sdf.format(from) + "-" + sdf.format(until))
            }
        }
        log.debug("URL: $url");
        return new URL(url)
    }

    public void setResource(String r) { this.resource = r; }

    // END possible authentication alternative
    public int doImport(ImportWhelk whelk, Date from) {
        try {
            pool = Executors.newCachedThreadPool()

            this.starttime = System.currentTimeMillis();
            List<Future> futures = []
            if (from) {
                futures << pool.submit(new Harvester(whelk, this.resource, getBaseUrl(from, null), 0))
            } else {
                for (int i = 1970; i < 2013; i++) {
                    final Date fromDate = getYearDate(i)
                    final Date untilDate = getYearDate(i+1)
                    futures << pool.submit(new Harvester(whelk, this.resource, getBaseUrl(fromDate, untilDate), i))
                }
            }
            log.info("Collecting results ...")
            def results = futures.collect{it.get()}
            log.info("Results: $results, after " + (System.currentTimeMillis() - starttime)/1000 + " seconds.")
        } finally {
            pool.shutdown()
        }
        return 0
    }

    Date getYearDate(int year) {
        def sdf = new SimpleDateFormat("yyyy")
        return sdf.parse("" + year)
    }
}

@Log
class Harvester implements Runnable {
    URL url
    Whelk whelk
    String resource
    private int imported = 0;
    int year
    def storepool

    Harvester(Whelk w, String r, URL u, int y) {
        this.url = u
        this.resource = r
        this.whelk = w
        this.year = y
        this.storepool = Executors.newCachedThreadPool()
    }

    private void getAuthentication() {
        try {
            Properties properties = new Properties();
            properties.load(this.getClass().getClassLoader().getResourceAsStream("whelks-core.properties"));
            final String username = properties.getProperty("username");
            final String password = properties.getProperty("password");
                Authenticator.setDefault(new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray());
                    }
                });
        } catch (Exception ex) {
            Logger.getLogger(BatchImport.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    // END possible authentication alternative
    public int doImport(ImportWhelk whelk, Date from) {
        pool = java.util.concurrent.Executors.newCachedThreadPool()
        //pool = java.util.concurrent.Executors.newFixedThreadPool(20)
    }

    public void run() {
        try {
            getAuthentication();
            log.info("Starting harvester with url: $url")
            String resumptionToken = harvest(url);
            if (!resumptionToken) {
                log.warn("Curious results ($resumptionToken) for $url")
            }
            while (resumptionToken != null) {
                url = new URL("http://data.libris.kb.se/" + this.resource + "/oaipmh/?verb=ListRecords&resumptionToken=" + resumptionToken);
                resumptionToken = harvest(url)
                log.debug("Received resumptionToken $resumptionToken")
            }
        //} catch(Exception e){}
        } finally {
            log.info("Harvester for ${this.year} has ended its run.")
            this.storepool.shutdown()
        }
    }

    String harvest(URL url) {
        log.debug("Call for harvest on $url")
        String mdrecord = null
        try {
            log.debug("URL.text: ${url.text}")
            def OAIPMH = new XmlSlurper(false,false).parseText(normalizeString(url.text))
            log.debug("OAIPMH: $OAIPMH")
            def documents = []
            OAIPMH.ListRecords.record.each {
                mdrecord = createString(it.metadata.record)
                MarcRecord record = MarcXmlRecordReader.fromXml(mdrecord)
                String id = record.getControlfields("001").get(0).getData();
                String jsonRec = MarcJSONConverter.toJSONString(record);
                documents << new BasicDocument().withData(jsonRec.getBytes("UTF-8")).withIdentifier("/" + whelk.prefix + "/" + id).withContentType("application/json");
            }
            if (documents.size() > 0) {
                imported = imported + documents.size()
                storepool.submit(new Runnable() {
                    public void run() {
                        log.debug("Current pool size: " + ((ThreadPoolExecutor)pool).getPoolSize() + " current active count " + ((ThreadPoolExecutor)pool).getActiveCount())
                        log.debug("Pushing ${document.identifier} to $whelk")
                        //whelk.store(document)
                        whelk.store(documents)
                        log.trace("Thread has now imported $imported documents.")
                    }
                })
            }
            /*
            log.info("rt type: " + OAIPMH.ListRecords.resumptionToken.class.name)
            log.error "object: (${OAIPMH.ListRecords.resumptionToken})"
            if (OAIPMH.ListRecords.resumptionToken == "") {
                log.error("NO RESUMPTION TOKEN FOR $url!")
                log.error("Raw: " + url.text)
                log.error("Normalized: " + normalizeString(url.text))
                throw new se.kb.libris.whelks.exception.WhelkRuntimeException("Bad")
            }
            */
            return OAIPMH.ListRecords.resumptionToken
        } catch (java.io.IOException ioe) {
            log.warn("Failed to parse record \"$mdrecord\": ${ioe.message}. Trying to extract resumptionToken and continue.")
            return findResumptionToken(url.text)
        } 
        catch (Exception e) {
            log.warn("Failed to parse XML document \"${url.text}\": ${e.message}. Trying to extract resumptionToken and continue.")
            return findResumptionToken(url.text)
        }
    }

    String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            log.debug("Normalizing ...")
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }

    String findResumptionToken(xmlString) {
        log.info("findResumption ...")
        try {
            return xmlString.split("(<)/?(resumptionToken>)")[1]
        } catch (ArrayIndexOutOfBoundsException a) {
            log.warn("Failed to extract resumptionToken from xml:\n$xmlString")
        }
        return null
    }

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind{
            out << root
        }
    }
}