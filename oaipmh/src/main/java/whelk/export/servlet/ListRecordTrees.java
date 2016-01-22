package whelk.export.servlet;

import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.*;
import java.time.ZonedDateTime;
import java.util.*;

public class ListRecordTrees
{
    private final static String FROM_PARAM = "from";
    private final static String UNTIL_PARAM = "until";
    private final static String SET_PARAM = "set";
    private final static String FORMAT_PARAM = "metadataPrefix";

    public static void handleListRecordTreesRequest(HttpServletRequest request, HttpServletResponse response)
            throws IOException, XMLStreamException, SQLException {
        // Parse and verify the parameters allowed for this request
        String from = request.getParameter(FROM_PARAM); // optional
        String until = request.getParameter(UNTIL_PARAM); // optional
        String set = request.getParameter(SET_PARAM); // optional
        String metadataPrefix = request.getParameter(FORMAT_PARAM); // required

        if (ResponseCommon.errorOnExtraParameters(request, response,
                FROM_PARAM, UNTIL_PARAM, SET_PARAM, FORMAT_PARAM))
            return;

        if (metadataPrefix == null) {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "metadataPrefix argument required.", request, response);
            return;
        }

        // Was the set selection valid?
        SetSpec setSpec = new SetSpec(set);
        if (!setSpec.isValid()) {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "Not a supported set spec: " + set, request, response);
            return;
        }

        // Record trees must have a 'hold' root node. Meaning, the root set of the request must be hold.
        if (!setSpec.getRootSet().equals(SetSpec.SET_HOLD)) {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_BAD_ARGUMENT,
                    "ListRecordTrees requires that the 'hold' (with optional subsets) be specified", request, response);
            return;
        }

        // Was the data ordered in a format we know?
        if (!OaiPmh.supportedFormats.keySet().contains(metadataPrefix)) {
            ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_CANNOT_DISSEMINATE_FORMAT, "Unsupported format: " + metadataPrefix,
                    request, response);
            return;
        }

        // "No start date" is replaced with a _very_ early start date.
        if (from == null)
            from = "0000-01-01";

        ZonedDateTime fromDateTime = Helpers.parseISO8601(from);
        ZonedDateTime untilDateTime = Helpers.parseISO8601(until);

        respond(request, response, fromDateTime, untilDateTime, setSpec, metadataPrefix);
    }

    private static void respond(HttpServletRequest request, HttpServletResponse response,
                                ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec,
                                String requestedFormat)
            throws IOException, XMLStreamException, SQLException
    {

        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        // First connection, used for iterating over the requested root (holding) nodes. ID only
        try (Connection firstConn = DataBase.getConnection()) {

            // Construct the query
            String selectSQL = "SELECT id FROM " + tableName +
                    " WHERE manifest->>'collection' = 'hold'";
            if (setSpec.getSubset() != null)
                selectSQL += " AND data @> '{\"@graph\":[{\"heldBy\": {\"@type\": \"Organization\", \"notation\": \"" +
                        Helpers.scrubSQL(setSpec.getSubset()) + "\"}}]}' ";
            //selectSQL += " LIMIT 1 ";
            PreparedStatement preparedStatement = firstConn.prepareStatement(selectSQL);
            ResultSet resultSet = preparedStatement.executeQuery();

            List<String> nodeDatas = new LinkedList<String>();
            HashSet<String> visitedIDs = new HashSet<String>();
            while (resultSet.next())
            {
                //Map data = mapper.readValue(resultSet.getString("data"), HashMap.class);
                //Map manifest = mapper.readValue(resultSet.getString("manifest"), HashMap.class);
                //Document doc = new Document(data, manifest);
                String id = resultSet.getString("id");

                System.out.println("* Building new tree, root: " + id);

                // Use a second db connection for the embedding process
                try (Connection secondConn = DataBase.getConnection()) {
                    addNodeToTree(id, visitedIDs, secondConn, nodeDatas);
                }
            }
        }
    }

    private static void addNodeToTree(String id, Set<String> visitedIDs, Connection connection, List<String> nodeDatas)
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

        System.out.println("Added node: " + id + " to tree.");

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

        // KB IDs can take many shapes. Examples:
        // /some?type=Organization&notation=KVIN // ignore, fixed by linkfinder ?
        // https://libris.kb.se/nszp6vst12dg8t5 // lookup
        // https://libris.kb.se/resource/bib/1234 // transform!
        // https://libris.kb.se/bib/1234 // ignore, fixed by linkfinder ?
        // https://id.kb.se/term/kao/Jansson%2C%20Tove // lookup

        //if ( !potentialID.startsWith("https://libris.kb.se/") && !potentialID.startsWith("https://id.kb.se/") )
            //return;

        if ( !potentialID.startsWith("http") )
            return;

        potentialID = potentialID.replace("resource/", "");

        System.out.println("  potential id: " + potentialID);

        String sql = "SELECT id FROM lddb__identifiers WHERE identifier = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, potentialID);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next())
        {
            String id = resultSet.getString("id");
            System.out.println("  Passing real id: " + id);
            addNodeToTree( id, visitedIDs, connection, nodeDatas );
        }


        /*
        if (visitedIDs.contains(potentialID))
            return;*/


    }
}
