package whelk.export.servlet;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.converter.marc.JsonLD2MarcXMLConverter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class ListRecords {
    public static void streamResponse(HttpServletRequest req, HttpServletResponse res)
            throws IOException, XMLStreamException, SQLException
    {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();

        try (Connection dbconn = DataBase.getConnection())
        {
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(res.getOutputStream());

            String tableName = OaiPmh.configuration.getProperty("sqlMaintable");
            String selectSQL = "SELECT data, manifest FROM " + tableName + " LIMIT 5";
            PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
            //preparedStatement.setString(1, OaiPmh.configuration.getProperty("sqlMaintable"));
            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next())
            {
                String data = rs.getString("data");
                String manifest = rs.getString("manifest");
                HashMap<String, Object> datamap = new ObjectMapper().readValue(data, HashMap.class);
                HashMap<String, Object> manifestmap = new ObjectMapper().readValue(manifest, HashMap.class);
                Document jsonLDdoc = new Document(datamap, manifestmap);
                System.out.println("DB item: " + jsonLDdoc.getId());
                /*JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter();
                Document marcXMLDoc = converter.convert(jsonLDdoc);
                System.out.println(marcXMLDoc.getData());
                PrintWriter out = res.getWriter();
                out.print(datat);
                out.flush();*/
            }

            writer.close();
        }
    }
}
