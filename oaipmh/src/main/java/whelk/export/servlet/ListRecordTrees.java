package whelk.export.servlet;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;
import java.time.ZonedDateTime;
import java.util.*;

public class ListRecordTrees
{
    public static void respond(HttpServletRequest request, HttpServletResponse response,
                                ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec,
                                String requestedFormat)
            throws IOException, XMLStreamException, SQLException
    {
        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        // First connection, used for iterating over the requested root (holding) nodes. ID only
        try (Connection firstConn = DataBase.getConnection()) {

            // Construct the query
            String selectSQL = "SELECT id FROM " + tableName + " WHERE TRUE ";
            if (setSpec.getRootSet() != null)
                selectSQL += " AND manifest->>'collection' = ?";
            if (setSpec.getSubset() != null)
                selectSQL += " AND data @> '{\"@graph\":[{\"heldBy\": {\"@type\": \"Organization\", \"notation\": \"" +
                        Helpers.scrubSQL(setSpec.getSubset()) + "\"}}]}' ";

            PreparedStatement preparedStatement = firstConn.prepareStatement(selectSQL);

            // Assign parameters
            if (setSpec.getRootSet() != null)
                preparedStatement.setString(1, setSpec.getRootSet());

            ResultSet resultSet = preparedStatement.executeQuery();

            // Build the xml response feed
            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
            xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
            XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());
            ResponseCommon.writeOaiPmhHeader(writer, request, true);

            writer.writeStartElement("ListRecordTrees");
            writer.writeStartElement("records");

            while (resultSet.next())
            {
                List<String> nodeDatas = new LinkedList<String>();
                HashSet<String> visitedIDs = new HashSet<String>();
                String id = resultSet.getString("id");

                // Use a second db connection for the embedding process
                try (Connection secondConn = DataBase.getConnection()) {
                    addNodeAndSubnodesToTree(id, visitedIDs, secondConn, nodeDatas);
                }

                Document mergedDocument = mergeDocument(id, nodeDatas);

                writer.writeStartElement("record");
                writer.writeStartElement("metadata");
                ResponseCommon.writeConvertedDocument(writer, requestedFormat, mergedDocument);
                writer.writeEndElement(); // metadata
                writer.writeEndElement(); // record
            }

            writer.writeEndElement(); // records
            writer.writeEndElement(); // ListRecordTrees
            ResponseCommon.writeOaiPmhClose(writer, request);
        }
    }

    private static void addNodeAndSubnodesToTree(String id, Set<String> visitedIDs, Connection connection, List<String> nodeDatas)
            throws SQLException, IOException
    {
        if (visitedIDs.contains(id))
            return;

        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");
        String selectSQL = "SELECT id, data FROM " + tableName + " WHERE id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
        preparedStatement.setString(1, id);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (!resultSet.next())
            return;

        ObjectMapper mapper = new ObjectMapper();
        String jsonBlob = resultSet.getString("data");
        nodeDatas.add(jsonBlob);
        visitedIDs.add(id);

        Map map = mapper.readValue(jsonBlob, HashMap.class);
        parseMap(map, visitedIDs, connection, nodeDatas);
    }

    private static void parseMap(Map map, Set<String> visitedIDs, Connection connection, List<String> nodeDatas)
            throws SQLException, IOException
    {
        for (Object key : map.keySet())
        {
            Object value = map.get(key);

            if (value instanceof Map)
                parseMap( (Map) value, visitedIDs, connection, nodeDatas );
            else if (value instanceof List)
                parseList( (List) value, visitedIDs, connection, nodeDatas );
            else
                parsePotentialId( key, value, visitedIDs, connection, nodeDatas );
        }
    }

    private static void parseList(List list, Set<String> visitedIDs, Connection connection, List<String> nodeDatas)
            throws SQLException, IOException
    {
        for (Object item : list)
        {
            if (item instanceof Map)
                parseMap( (Map) item, visitedIDs, connection, nodeDatas );
            else if (item instanceof List)
                parseList( (List) item, visitedIDs, connection, nodeDatas );
        }
    }

    private static void parsePotentialId(Object key, Object value, Set<String> visitedIDs, Connection connection, List<String> nodeDatas)
            throws SQLException, IOException
    {
        if ( !(key instanceof String) || !(value instanceof String))
            return;

        if ( ! "@id".equals(key) )
            return;

        String potentialID = (String) value;
        if ( !potentialID.startsWith("http") )
            return;

        potentialID = potentialID.replace("resource/", "");

        String sql = "SELECT id FROM lddb__identifiers WHERE identifier = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, potentialID);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next())
        {
            String id = resultSet.getString("id");
            addNodeAndSubnodesToTree( id, visitedIDs, connection, nodeDatas );
        }
    }

    private static Document mergeDocument(String id, List<String> nodeDatas)
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        // One element in the list is guaranteed.
        String rootData = nodeDatas.get(0);
        Map rootMap = mapper.readValue(rootData, HashMap.class);
        List mergedGraph = (List) rootMap.get("@graph");

        for (int i = 1; i < nodeDatas.size(); ++i)
        {
            String nodeData = nodeDatas.get(i);
            Map nodeRootMap = mapper.readValue(nodeData, HashMap.class);
            List nodeGraph = (List) nodeRootMap.get("@graph");
            mergedGraph.addAll(nodeGraph);
        }

        rootMap.replace("@graph", mergedGraph);

        return new Document(id, rootMap);
    }
}
