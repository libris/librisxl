package se.kb.libris.imports;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
//import org.dom4j.Document;
//import org.dom4j.io.SAXReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import org.apache.commons.codec.binary.Base64;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.XMLEventAllocator;

public class BatchImport {
    public static void main(String[] args) {
        try {
            URL url = new URL("http://data.libris.kb.se/auth/oaipmh/?verb=ListIdentifiers&metadataPrefix=marcxml&set=type:P");
            String authString = "apibeta:beta";
            byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
            String authStringEnc = new String(authEncBytes);
            URLConnection urlConnection = url.openConnection();
            urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);
            InputStream is = urlConnection.getInputStream();
            XMLInputFactory xif = XMLInputFactory.newInstance();
            System.out.println("XIF: " + xif);
            XMLStreamReader streamReader = xif.createXMLStreamReader(is);
            XMLEventReader eventReader = xif.createXMLEventReader(streamReader);
            boolean startRecord = false;
            while (streamReader.hasNext()) {
                int event = streamReader.next();
                
               if (streamReader.getEventType() == streamReader.START_ELEMENT && streamReader.getNamespaceURI().trim().equals("http://www.openarchives.org/OAI/2.0/") && streamReader.getName().getLocalPart().trim().equals("header")) {
                    startRecord = true;

                    System.out.println("NAME :" + streamReader.getName().toString());
                }
            }

            /*while (eventReader.hasNext()) {
                XMLEvent xe = eventReader.nextEvent();
                if (xe.isStartElement()) {
                   System.out.println(streamReader.getName()); 
                }
                   //System.out.println(xe.getEventType());
            }*/
            //XMLStreamReader reader = new SAXReader();
            
            /*
            InputStreamReader isr = new InputStreamReader(is);
            int numCharsRead;
            char[] charArray = new char[1024];
            StringBuffer sb = new StringBuffer();
            while ((numCharsRead = isr.read(charArray)) > 0) {
                sb.append(charArray, 0, numCharsRead);
            }
            String result = sb.toString();
            System.out.println("*** START ***");
            System.out.println(result);
            System.out.println("*** STOP ***");
            */
        }
        catch (MalformedURLException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (XMLStreamException e) {
            e.printStackTrace();
        }
            
        //System.out.println("Testing");
    }
}

