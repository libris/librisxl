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
import javax.xml.parsers.*;
import org.xml.sax.SAXException;
import org.xml.sax.Attributes;
import java.util.ArrayList;
/**
 *
 * @author katarina
 */
public class BatchImportTest extends org.xml.sax.helpers.DefaultHandler {
   private boolean idStart = false; 
   private StringBuffer idContent = null;
   private MarcXmlRecordReader marcXmlRecordReader = null;
   private SAXParser saxParser = null;
   private ArrayList<String> identifiers = new ArrayList<String>();

   public static void main(String[] args) throws Exception {
       BatchImportTest bit = new BatchImportTest();
       bit.batchImport();
   }

    public void batchImport() throws Exception {

        try {
            URL url = new URL("http://data.libris.kb.se/auth/oaipmh/?verb=ListIdentifiers&metadataPrefix=marcxml");
            saxParser = SAXParserFactory.newInstance().newSAXParser();
            Properties properties = new Properties();
            properties.load(new FileInputStream("whelks-core.properties")); //Kanske ska göra en ClassLoader.getResource-habrovink istället
            String authString = properties.getProperty("authString");

            byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
            String authStringEnc = new String(authEncBytes);
            URLConnection urlConnection = url.openConnection();
            urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);
            InputStream is = urlConnection.getInputStream();
            //ImportHandler ih = new ImportHandler();
            idContent = new StringBuffer();
            saxParser.parse(is, this); 
            for (String s: identifiers) {
               System.out.println(s);
            }
            //marcXmlRecordReader = new MarcXmlRecordReader(is, "/OAI-PMH/ListRecords/record/metadata/record");
            /*marcXmlRecordReader = new MarcXmlRecordReader(is, "/OAI-PMH/ListRecords/record/metadata/record");

            MarcRecord record;
            while ((record = marcXmlRecordReader.readRecord()) != null) {
                System.out.write(Iso2709Serializer.serialize(record));
                System.out.println(record);
                String hylla = MarcJSONConverter.toJSONString(record);
                System.out.println(hylla);
            }*/

        } catch (MalformedURLException mue) {
            mue.printStackTrace();
        }
    }
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
           idContent.setLength(0);
        if (qName.equals("identifier")) {
           idStart = true;
           //System.out.println("ID: " + identifier);
        }
    }
    public void characters (char[] values, int start, int length) throws SAXException {
        if (idStart) {
            idContent.append(values, start, length);
        }
    }
    public void endElement (String uri, String localName, String qName) throws SAXException {
        if (qName.equals("identifier")) {
           identifiers.add(idContent.toString());
           //System.out.println("ID: " + idContent.toString());
        }
    }
}
