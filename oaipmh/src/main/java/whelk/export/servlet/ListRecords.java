package whelk.export.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.DocumentBuilder;

public class ListRecords {
    public static String createResponse(HttpServletRequest req, DocumentBuilder xmlBuilder)
    {
        org.w3c.dom.Document xmlResponse = xmlBuilder.newDocument();

        return null;
    }
}
