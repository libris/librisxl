package se.kb.libris.whelks.imports;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
import java.text.*;
import org.apache.commons.codec.binary.Base64;
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

public class BatchImport {

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
            url = url + sdf.format(from);
        }
        System.out.println("URL: " + url);
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
    // START possible authentication alternative
    /*public void getAuthentication() throws IOException {
        Properties properties = new Properties();
            properties.load(new FileInputStream("resources/whelks-core.properties"));
            final String username = properties.getProperty("username");
            final String password = properties.getProperty("password");
            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password.toCharArray());
                }
            });
    }*/
    // END possible authentication alternative
    public int doImport(ImportWhelk whelk, Date from) {
        getAuthentication(); // Testar detta istället för urlconn-grejen i harvest()
        for (Plugin p : whelk.getPlugins()) {
            if (p instanceof Notifier) {
                p.disable();
            }
        }
        try {
            /*Properties properties = new Properties(); properties.load(new FileInputStream("resources/whelks-core.properties")); String
            authString = properties.getProperty("authString");
            byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
            String authStringEnc = new String(authEncBytes);
            urlConnection = url.openConnection();
            urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);*/
            // While resumptionToken is something
            URL url = new URL(getBaseUrl(from));
            this.starttime = System.currentTimeMillis();
            String resumptionToken = harvest(url, whelk);
            while (resumptionToken != null) {
                //redefine url
                url = new URL("http://data.libris.kb.se/" + this.resource + "/oaipmh/?verb=ListRecords&resumptionToken=" + resumptionToken);
                resumptionToken = harvest(url, whelk);
            }
            
            //System.out.println("RESTOK: " + resumptionToken);
            // Loop through harvest
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        /*} catch (FileNotFoundException e) {
            e.printStackTrace();*/
        } finally {
            for (Plugin p : whelk.getPlugins()) {
                if (p instanceof Notifier) {
                    p.enable();
                }
            }
        }
        return imported;
    }

    public String harvest(URL url, ImportWhelk whelk) {
        String restok = null;
        InputStream is = null;
        HttpURLConnection urlConnection = null;
        try {
            urlConnection = (HttpURLConnection) url.openConnection();
            is = urlConnection.getInputStream();
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
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
                /*
                if (imported % 1000 == 0) {
                    System.out.println("" + imported + " documents imported in " + ((System.currentTimeMillis() - this.starttime)/1000) + " seconds.");
                }
                */
            }
            whelk.store(documents);

        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
                urlConnection.disconnect();
            } catch (IOException e) {
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return restok;
    }
}
