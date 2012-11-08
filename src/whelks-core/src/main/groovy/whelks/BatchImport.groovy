package se.kb.libris.whelks.imports;

import groovy.util.logging.Slf4j as Log
import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult

import groovy.transform.Synchronized

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
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

    private int imported = 0;
    private long starttime = 0;
    private int NUMBER_OF_IMPORTERS = 20
    def pool

    List docList = Collections.synchronizedList(new LinkedList())

    public BatchImport() {}


    public BatchImport(String resource) {
        this.resource = resource;
    }

    private String getBaseUrl(Date from) {
        //return "http://data.libris.kb.se/"+this.resource+"/oaipmh/?verb=ListRecords&metadataPrefix=marcxml&from=2012-05-23T15:21:27Z";
        String url = "http://data.libris.kb.se/"+this.resource+"/oaipmh/?verb=ListRecords&metadataPrefix=marcxml";
        if (from != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            url = url + "&" + sdf.format(from);
        }
        log.info("URL: $url");
        return url;
    }

    public void setResource(String r) { this.resource = r; }

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
        //pool = java.util.concurrent.Executors.newCachedThreadPool()
        pool = java.util.concurrent.Executors.newFixedThreadPool(20)

        getAuthentication(); // Testar detta istället för urlconn-grejen i harvest()
        try {
            // While resumptionToken is something
            URL url = new URL(getBaseUrl(from));
            this.starttime = System.currentTimeMillis();
            String resumptionToken = harvest(url, whelk);
            while (resumptionToken != null) {
                //redefine url
                url = new URL("http://data.libris.kb.se/" + this.resource + "/oaipmh/?verb=ListRecords&resumptionToken=" + resumptionToken);
                resumptionToken = harvest(url, whelk);
            }

            // Loop through harvest
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return imported;
    }

    public static String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind{
            out << root
        }
    }

    String findResumptionToken(xmlString) {
        try {
            return xmlString.split("(<)/?(resumptionToken>)")[1]
        } catch (ArrayIndexOutOfBoundsException a) {
            log.error("Failed to extract resumptionToken from xml:\n$xmlString")
        }
    }

    String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }

    String harvest(url, whelk) {
        try {
            def OAIPMH = new XmlSlurper(false,false).parseText(normalizeString(url.text))
            //def documents = []
            OAIPMH.ListRecords.record.each {
                MarcRecord record = MarcXmlRecordReader.fromXml(createString(it.metadata.record))
                String id = record.getControlfields("001").get(0).getData();
                String jsonRec = MarcJSONConverter.toJSONString(record);
                Document document = new BasicDocument().withData(jsonRec.getBytes("UTF-8")).withIdentifier("/" + whelk.prefix + "/" + id).withContentType("application/json");
                log.debug("Submitting document to importer.")
                pool.submit(new Runnable() {
                    public void run() {
                        log.debug("Current pool size: " + ((ThreadPoolExecutor)pool).getPoolSize() + " current active count " + ((ThreadPoolExecutor)pool).getActiveCount())
                        log.debug("Pushing ${document.identifier} to $whelk")
                        whelk.store(document)
                    }
                })
                imported++
            }
            //addBatch(documents)
            return OAIPMH.ListRecords.resumptionToken
        } catch (Exception e) {
            log.warn("Failed to parse XML document: ${e.message}. Trying to extract resumptionToken and continue.", e)
            return findResumptionToken(url.text)
        }
    }

    /*
    void addBatch(List<Document> docs) {
        docList << docs
    }

    @Synchronized
    List<List<Document>> nextBatch() {
        try {
            List<Document> docs = docList.pop()
            return docs
        } catch (NoSuchElementException nse) {
            return null
        }
    }

    class Importer implements Runnable {

        Whelk whelk
        List<Document> batch

        Importer(Whelk whelk, List<Document> b) { this.whelk = whelk; this.batch = b;}

        void run() {
            whelk.store(batch)
        }
    }
    */
}
