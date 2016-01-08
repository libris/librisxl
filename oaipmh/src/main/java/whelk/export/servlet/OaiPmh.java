package whelk.export.servlet;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.converter.marc.JsonLD2MarcXMLConverter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class OaiPmh extends HttpServlet {

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        sendResponse(req, res);
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        sendResponse(req, res);
    }

    private void sendResponse(HttpServletRequest req, HttpServletResponse res) throws IOException
    {
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder xmlBuilder = null;
        try {
            xmlBuilder = builderFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e)
        {
            res.sendError(500, e.getMessage());
            return;
        }

        String verb = req.getParameter("verb");

        switch (verb)
        {
            case "GetRecord":
                break;
            case "Identify":
                break;
            case "ListIdentifiers":
                break;
            case "ListMetadataFormats":
                break;
            case "ListRecords":
                ListRecords.createResponse(req, xmlBuilder);
                break;
            case "ListSets":
                break;
            default:
                res.sendError(400, "Correct OAI-PMH verb required.");
        }
    }

    /*
    public void doGet(HttpServletRequest req, HttpServletResponse res)
    {
        System.out.println("Get is correctly wired!\n\n");

        try
        {
            System.out.println(req.getPathInfo());
            String dataset = req.getPathInfo().split("/")[1];
            System.out.println(dataset);
            String verb = req.getParameter("verb");
            String id = req.getParameter("identifier");
            if (verb.equals("GetRecord") ){

                String selectSQL = "SELECT data FROM lddb WHERE id = ?";
                PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
                preparedStatement.setString(1, id);
                ResultSet rs = preparedStatement.executeQuery();
                if (rs.next()){
                    String datat = rs.getString("data");
                    HashMap<String, Object> datamap = new ObjectMapper().readValue(datat, HashMap.class);
                    Document jsonLDdoc = new Document(id, datamap);
                    JsonLD2MarcXMLConverter converter = new JsonLD2MarcXMLConverter();
                    Document marcXMLDoc = converter.convert(jsonLDdoc);
                    System.out.println(marcXMLDoc.getData());
                    PrintWriter out = res.getWriter();
                    out.print(datat);
                    out.flush();
                }

            }
            /*
            else if (verb.equals("ListRecords")) {
                String from = req.getParameter("from");
                String until = req.getParameter("until");
                //String until = req.getParameter("resumptionToken"); //TODO

                String selectSQL = "SELECT data FROM lddb WHERE date >= ? AND date <= ?";
                PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
                PreparedStatement.setString(1, from);
                PreparedStatement.setString(2, until);
                ResultSet rs = preparedStatement.executeQuery();
                if (rs.next()){
                    String datat = rs.getString("data");
                    PrintWriter out = res.getWriter();
                    out.print(datat);
                    out.flush();
                }


            }*

            else {
                PrintWriter out = res.getWriter();
                out.print("Far åt pipsvängen!" + verb);
                out.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }



    }

    @Override
    public void init()
    {
        System.out.println("initing");
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/whelk");
            //connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("outiting");


    }
    */
}
