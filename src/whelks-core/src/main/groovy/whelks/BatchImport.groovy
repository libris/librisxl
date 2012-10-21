package se.kb.libris.whelks.imports;

import groovy.util.logging.Slf4j as Log
import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult

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
import se.kb.libris.whelks.plugin.*;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@Log
class BatchImport {

    private String resource;

    private int imported = 0;
    private long starttime = 0;

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

    /*
    String getFilteredString(String xmlString) {
        byte[] outbytes
        try {
            // Decode the file just to get a CharBuffer for the encoder - perhaps not the simpliest way?
            Charset charset = Charset.forName("UTF-8");
            CharsetDecoder utf8Decoder = charset.newDecoder();
            utf8Decoder.onMalformedInput(CodingErrorAction.IGNORE);
            utf8Decoder.onUnmappableCharacter(CodingErrorAction.IGNORE);
            CharsetEncoder utf8Encoder = charset.newEncoder();
            CharBuffer buffer = utf8Decoder.decode(ByteBuffer.wrap(xmlString.getBytes()));
            ByteBuffer bb = utf8Encoder.encode(buffer);
            outbytes = bb.array();
        } catch (CharacterCodingException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        } 
        println "converted doc: " + new String(outbytes)
        return new String(outbytes)
    }
    */

    String findResumptionToken(xmlString) {
        try {
            return xmlString.split("(<)/?(resumptionToken>)")[1]
        } catch (ArrayIndexOutOfBoundsException a) {
            log.error("Failed to extract resumptionToken from xml:\n$xmlString")
        }
    }

    String harvest(url, whelk) {
        try {
            def OAIPMH = new XmlSlurper(false,false).parseText(url.text)
            def documents = []
            OAIPMH.ListRecords.record.each {
                MarcRecord record = MarcXmlRecordReader.fromXml(createString(it.metadata.record))
                String id = record.getControlfields("001").get(0).getData();
                String jsonRec = MarcJSONConverter.toJSONString(record);
                if (whelk) {
                    documents << whelk.createDocument().withData(jsonRec.getBytes("UTF-8")).withIdentifier("/" + this.resource + "/" + id).withContentType("application/json");
                }
                imported++
            }
            if (whelk) {
                whelk.store(documents)
            }
            return OAIPMH.ListRecords.resumptionToken
        } catch (Exception e) {
            log.warn("Failed to parse XML document: ${e.message}. Trying to extract resumptionToken and continue.")
            return findResumptionToken(url.text)
        }
    }

    /*
    public String oldharvest(URL url, ImportWhelk whelk) {
        String restok = null;
        InputStream is = null;
        HttpURLConnection urlConnection = null;
        try {
            urlConnection = (HttpURLConnection) url.openConnection(); 
            String xmlString = getFilteredStreamFromURLConnection(urlConnection);
            println "Length of $xmlString is " + xmlString.length()
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            is = IOUtils.toInputStream(xmlString);
            Document document = documentBuilder.parse(is);
            NodeList nodeList = document.getElementsByTagName("resumptionToken");
            Element element = (Element) nodeList.item(0);
            if (element != null && element.getTextContent().length() > 0) {
                restok = element.getTextContent();
            }
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Source xmlSource = new DOMSource(document);
            Result outputTarget = new StreamResult(outputStream);
            TransformerFactory.newInstance().newTransformer().transform(xmlSource, outputTarget);
            is = new ByteArrayInputStream(outputStream.toByteArray());

            MarcXmlRecordReader marcXmlRecordReader = new MarcXmlRecordReader(is, "/OAI-PMH/ListRecords/record/metadata/record");
            MarcRecord record;

            ArrayList<se.kb.libris.whelks.Document> documents = new ArrayList<se.kb.libris.whelks.Document>();

            while ((record = marcXmlRecordReader.readRecord()) != null) {
                String id = record.getControlfields("001").get(0).getData();

                String jsonRec = MarcJSONConverter.toJSONString(record);

                se.kb.libris.whelks.Document doc = whelk.createDocument().withData(jsonRec.getBytes()).withIdentifier("/" + this.resource + "/" + id).withContentType("application/json");
                //
                //System.out.println("Storing document " + doc.getIdentifier());
                //System.out.println("Created document with id " + doc.getIdentifier());
                documents.add(doc);
                imported++;
                //whelk.store(doc);
            }
            whelk.store(documents);

        } catch (ParserConfigurationException e) {
            System.err.println("URL " + url + " failed:");
            e.printStackTrace();
        } catch (SAXException e) {
            System.err.println("URL " + url + " failed:");
            e.printStackTrace();
        } catch (TransformerConfigurationException e) {
            System.err.println("URL " + url + " failed:");
            e.printStackTrace();
        } catch (TransformerException e) {
            System.err.println("URL " + url + " failed:");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("URL " + url + " failed:");
            e.printStackTrace();
        } finally {
            if (null != is) {
                try {
                    is.close();
                    urlConnection.disconnect();
                } catch (IOException e) {
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
        return restok;
    }*/
}
