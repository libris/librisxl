package se.kb.libris.whelks.imports;

import java.io.*;
import java.net.*;
import java.util.LinkedList;
import java.util.Properties;
import org.apache.commons.codec.binary.Base64;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Controlfield;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.Iso2709Serializer;
import se.kb.libris.conch.converter.MarcJSONConverter;
import se.kb.libris.whelks.*;
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

    private String baseUrl;
    private WhelkManager manager;

    public BatchImport(String baseUrl) {
        this.baseUrl = baseUrl;
        try {
            manager = new WhelkManager(new URL("file:///tmp/whelkconfig.json"));
        } catch (MalformedURLException mue) {
            mue.printStackTrace();
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
    public void doImport() {
        try {
            /*Properties properties = new Properties(); properties.load(new FileInputStream("resources/whelks-core.properties")); String
            authString = properties.getProperty("authString");
            byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
            String authStringEnc = new String(authEncBytes);
            urlConnection = url.openConnection();
            urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);*/
            // While resumptionToken is something
            URL url = new URL(baseUrl);
            String resumptionToken = harvest(url);
            while (resumptionToken != null) {
                //redefine url
                url = new URL("http://data.libris.kb.se/auth/oaipmh/?verb=ListRecords&resumptionToken=" + resumptionToken);
                resumptionToken = harvest(url);
            }
            
            //System.out.println("RESTOK: " + resumptionToken);
            // Loop through harvest
            

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        /*} catch (FileNotFoundException e) {
            e.printStackTrace();*/
        }

    }

    public String harvest(URL url) {
        String restok = null;
        InputStream is = null;
        HttpURLConnection urlConnection = null;
        try {
            Properties properties = new Properties(); // Authenticeringen ska göras annorstädes 
            properties.load(new FileInputStream("meta/whelks-core.properties"));
            String authStringRaw = properties.getProperty("authString");
            byte[] authEncBytes = Base64.encodeBase64(authStringRaw.getBytes());
            String authString = new String(authEncBytes);
            urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestProperty("Authorization", "Basic " + authString);
            is = urlConnection.getInputStream();
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document document = documentBuilder.parse(is);
            NodeList nodeList = document.getElementsByTagName("resumptionToken");
            Element element = (Element) nodeList.item(0);
            System.out.println("resumptionToken : " + nodeList.getLength() + ", " + element.getTextContent());
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

            while ((record = marcXmlRecordReader.readRecord()) != null) {
                for (Controlfield cf : record.getControlfields("001")) {
                    System.out.println("CF: " + cf.getData());
                }
                //LinkedList<ControlField> cf001 = record.getControfields("001");

                //System.out.write(Iso2709Serializer.serialize(record));
                //System.out.println(record);
                String hylla = MarcJSONConverter.toJSONString(record);
                System.out.println(hylla);
            }
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
            }
        }
        return restok;
    }

    public Document createDocument(InputStream is) {
        return null;
    }

    public static void main(String[] args) {
        BatchImport bi = new BatchImport("http://data.libris.kb.se/auth/oaipmh/?verb=ListRecords&metadataPrefix=marcxml&from=2012-05-23T15:21:27Z");
        bi.doImport();

    }
}
