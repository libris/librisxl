package whelk.export.servlet;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.Charset;

public class TestCommon
{
    public final static int port = 8012;
    public final static String baseUrl = "http://localhost:" + port;

    public static String httpGet(String pathWithoutBaseUrl)
            throws ProtocolException, IOException
    {
        final int BUF_SIZE = 4096;
        byte[] buf = new byte[BUF_SIZE];
        int bytesInBuffer;
        StringBuilder result = new StringBuilder();

        URL url = new URL(baseUrl + pathWithoutBaseUrl);
        HttpURLConnection connection = (HttpURLConnection)
                url.openConnection();
        connection.setRequestMethod("GET");

        DataInputStream inStream = new
                DataInputStream(connection.getInputStream());

        while( (bytesInBuffer = inStream.read(buf, 0, BUF_SIZE)) != -1)
        {
            String decodedUtf8 = new String(buf, 0, bytesInBuffer, Charset.forName("UTF-8"));
            result.append(decodedUtf8);
        }

        inStream.close();

        return result.toString();
    }

    public static String extractFirstOccurrenceElementContents(String xml, String elementName) throws Exception
    {
        String elementBegin = "<"+elementName+">";
        String elementEnd = "</"+elementName+">";
        int indexBegin = xml.indexOf(elementBegin) + elementBegin.length();
        int indexEnd = xml.indexOf(elementEnd);
        return xml.substring(indexBegin, indexEnd);
    }
}
