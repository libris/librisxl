/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kb.libris.whelks.imports;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.io.MarcXmlRecordReader;
import se.kb.libris.util.marc.io.Iso2709Serializer;
import java.net.URL;
import java.net.URLConnection;
import org.apache.commons.codec.binary.Base64;
import se.kb.libris.conch.converter.MarcJSONConverter;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import javax.xml.parsers.*;
import org.xml.sax.SAXException;
import org.xml.sax.Attributes;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author katarina
 */
//public class BatchImportTest extends org.xml.sax.helpers.DefaultHandler {
public class BatchImportTest extends DefaultHandler {

    private boolean idStart = false, resumptionTokenStart = false;
    private StringBuffer idContent = null, resumptionTokenContent = null;
    private String resumptionToken = "";
    private MarcXmlRecordReader marcXmlRecordReader = null;
    private SAXParser saxParser = null;
    private ArrayList<String> identifiers = new ArrayList<String>();

    public static void main(String[] args) {
        BatchImportTest bit = new BatchImportTest();
        bit.batchImport();
    }

    public void batchImport() {
        boolean doIt = true;
        String getUrl = "http://data.libris.kb.se/auth/oaipmh/?verb=ListIdentifiers&metadataPrefix=marcxml&from=2012-05-20T15:21:27Z";
        String resumptionToken = getIdentifiers(getUrl, null, null, null, null);
        /*while (doIt) {
            getIdentifiers(getUrl, null, null, null, null);
            System.out.println("L: " + resumptionToken.length());
            if (resumptionToken.length() > 0){
                System.out.println("RES: " + resumptionToken);
                getUrl = "http://data.libris.kb.se/auth/oaipmh/?verb=ListIdentifiers&resumptionToken=" + resumptionToken;
                
            }
            else {
                System.out.println("No MORE");
                doIt = false;
            }
        }*/
        System.out.println("SI: " + identifiers.size());
        for (String s : identifiers) {
            System.out.println(s);
        }
            
    }

    public String getIdentifiers(String baseUrl, String from, String until, String metadataPrefix, String set) {
        String u = "";
        try {
            URL url = new URL("http://data.libris.kb.se/auth/oaipmh/?verb=ListRecords&metadataPrefix=marcxml");
            saxParser = SAXParserFactory.newInstance().newSAXParser();
            /*Properties properties = new Properties();
            properties.load(new FileInputStream("resources/whelks-core.properties"));
            String authString = properties.getProperty("authString");

            byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
            String authStringEnc = new String(authEncBytes);*/
            URLConnection urlConnection = url.openConnection();
            //urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);
            InputStream is = urlConnection.getInputStream();
            //DefaultHandler ih = new DefaultHandler();
            idContent = new StringBuffer();
            resumptionTokenContent = new StringBuffer();
            saxParser.parse(is, this);
            //marcXmlRecordReader = new MarcXmlRecordReader(is, "/OAI-PMH/ListRecords/record/metadata/record");
            /*
             * marcXmlRecordReader = new MarcXmlRecordReader(is,
             * "/OAI-PMH/ListRecords/record/metadata/record");
             *
             * MarcRecord record; while ((record =
             * marcXmlRecordReader.readRecord()) != null) {
             * System.out.write(Iso2709Serializer.serialize(record));
             * System.out.println(record); String hylla =
             * MarcJSONConverter.toJSONString(record);
             * System.out.println(hylla);
            }
             */

        } catch (MalformedURLException mue) {
            mue.printStackTrace();
        } catch (ParserConfigurationException ex) {
            Logger.getLogger(BatchImportTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SAXException ex) {
            Logger.getLogger(BatchImportTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(BatchImportTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(BatchImportTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        return u;
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        idContent.setLength(0);
        resumptionTokenContent.setLength(0);
        if (qName.equals("identifier")) {
            idStart = true;
            //System.out.println("ID: " + identifier);
        } else if (qName.equals("resumptionToken")) {
            resumptionTokenStart = true;
        }
    }

    public void characters(char[] values, int start, int length) throws SAXException {
        if (idStart) {
            idContent.append(values, start, length);
        } else if (resumptionTokenStart) {
            // System.out.println("FOUND RES");
            resumptionTokenContent.append(values, start, length);
        }
    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (qName.equals("identifier")) {
            identifiers.add(idContent.toString());
            //System.out.println("ID: " + idContent.toString());
            idStart = false;
        } else if (qName.equals("resumptionToken")) {
            resumptionToken = resumptionTokenContent.toString();
            resumptionTokenStart = false;
        }
    }
}
