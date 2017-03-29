package whelk.export.servlet;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.util.LegacyIntegrationTools;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;


public class ListRecordTrees
{
    private static ObjectMapper s_mapper = new ObjectMapper();

    // The ModificationTimes class is used as a crutch to simulate "pass by reference"-mechanics. The point of this is that (a pointer to)
    // an instance of ModificationTimes is passed around in the tree building process, being _updated_ (which a ZonedDateTime cannot be)
    // with each documents created-timestamp.
    public static class ModificationTimes
    {
        public ZonedDateTime earliestModification;
        public ZonedDateTime latestModification;
    }

    /**
     * Sends a response to a ListRecords (or ListIdentifiers) request, with a metadataPrefix tagged with _expanded.
     * A tree is built for each record, containing all other nodes linked to by that record. The exception is auth
     * records, which are only included in one level (else we would build trees of half of libris for every post).
     */
    public static void respond(HttpServletRequest request, HttpServletResponse response,
                                ZonedDateTime fromDateTime, ZonedDateTime untilDateTime, SetSpec setSpec,
                                String requestedFormat, boolean onlyIdentifiers)
            throws IOException, XMLStreamException, SQLException
    {
        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        // First connection, used for iterating over the requested root nodes. ID only.
        try (Connection dbconn = OaiPmh.s_postgreSqlComponent.getConnection();
             PreparedStatement preparedStatement = prepareRootStatement(dbconn, setSpec);
             ResultSet resultSet = preparedStatement.executeQuery())
        {
            // Build the xml response feed
            XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
            xmlOutputFactory.setProperty("escapeCharacters", false); // Inline xml must be left untouched.
            XMLStreamWriter writer = null;

            boolean xmlIntroWritten = false;

            while (resultSet.next())
            {
                Vector<String> nodeDatas = new Vector<String>();
                HashSet<String> visitedIDs = new HashSet<String>();

                String id = resultSet.getString("id");

                ZonedDateTime modified = ZonedDateTime.ofInstant(resultSet.getTimestamp("modified").toInstant(), ZoneOffset.UTC);
                ModificationTimes modificationTimes = new ModificationTimes();
                modificationTimes.earliestModification = modified;
                modificationTimes.latestModification = modified;

                addNodeAndSubnodesToTree(id, visitedIDs, nodeDatas, modificationTimes);

                if (fromDateTime != null && fromDateTime.compareTo(modificationTimes.latestModification) > 0)
                    continue;
                if (untilDateTime != null && untilDateTime.compareTo(modificationTimes.earliestModification) < 0)
                    continue;

                // Do not begin writing to the response until at least one record has passed all checks. We might still need to
                // send a "noRecordsMatch".
                if (!xmlIntroWritten)
                {
                    xmlIntroWritten = true;
                    writer = xmlOutputFactory.createXMLStreamWriter(response.getOutputStream());
                    ResponseCommon.writeOaiPmhHeader(writer, request, true);
                    writer.writeStartElement("ListRecords");
                }

                Document mergedDocument = mergeDocument(id, nodeDatas);

                emitRecord(resultSet, mergedDocument, modificationTimes, writer, requestedFormat, onlyIdentifiers);
            }

            if (xmlIntroWritten)
            {
                writer.writeEndElement(); // ListRecords
                ResponseCommon.writeOaiPmhClose(writer, request);
            }
            else
            {
                ResponseCommon.sendOaiPmhError(OaiPmh.OAIPMH_ERROR_NO_RECORDS_MATCH, "", request, response);
            }
        }
    }

    /**
     * Called recursively to gather the nodes that will make up a tree. The 'data' portion of every concerned record
     * will be added to the nodeDatas list.
     */
    public static void addNodeAndSubnodesToTree(String id, Set<String> visitedIDs, List<String> nodeDatas,
                                                ModificationTimes modificationTimes)
            throws SQLException, IOException
    {
        if (visitedIDs.contains(id))
            return;
        visitedIDs.add(id);

        Map map = null;

        try (Connection dbconn = OaiPmh.s_postgreSqlComponent.getConnection();
             PreparedStatement preparedStatement = prepareNodeStatement(dbconn, id);
             ResultSet resultSet = preparedStatement.executeQuery())
        {
            if (!resultSet.next())
                return;

            String jsonBlob = resultSet.getString("data");
            nodeDatas.add(jsonBlob);

            // Only allow one level of recursive auth posts into the tree, or we'll end up adding half the database
            // into each tree.
            String collection = resultSet.getString("collection");
            if (collection.equals("auth"))
                return;

            ZonedDateTime modified = ZonedDateTime.ofInstant(resultSet.getTimestamp("modified").toInstant(), ZoneOffset.UTC);

            if (modified.compareTo(modificationTimes.earliestModification) < 0)
                modificationTimes.earliestModification = modified;
            if (modified.compareTo(modificationTimes.latestModification) > 0)
                modificationTimes.latestModification = modified;

            map = s_mapper.readValue(jsonBlob, HashMap.class);
        }

        if (map != null)
            parseMap(map, visitedIDs, nodeDatas, modificationTimes);
    }

    @SuppressWarnings("unchecked")
    public static Document mergeDocument(String id, List<String> nodeDatas)
            throws IOException
    {
        // One element in the list is guaranteed.
        String rootData = nodeDatas.get(0);
        Map rootMap = s_mapper.readValue(rootData, HashMap.class);
        List mergedGraph = (List) rootMap.get("@graph");

        for (int i = 1; i < nodeDatas.size(); ++i)
        {
            String nodeData = nodeDatas.get(i);
            Map nodeRootMap = s_mapper.readValue(nodeData, HashMap.class);
            List nodeGraph = (List) nodeRootMap.get("@graph");
            mergedGraph.addAll(nodeGraph);
        }

        rootMap.replace("@graph", mergedGraph);

        Document document = new Document(rootMap);
        document.setId(id);
        return document;
    }

    private static PreparedStatement prepareRootStatement(Connection dbconn, SetSpec setSpec)
            throws IOException, SQLException
    {
        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        // Construct the query
        String selectSQL = "SELECT id, collection, deleted, modified, data#>>'{@graph,1,heldBy,@id}' AS sigel FROM "
                + tableName + " WHERE collection <> 'definitions' ";
        if (setSpec.getRootSet() != null)
            selectSQL += " AND collection = ?";
        if (setSpec.getSubset() != null)
            selectSQL += " AND data @> ?";

        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);

        // Assign parameters
        if (setSpec.getRootSet() != null)
            preparedStatement.setString(1, setSpec.getRootSet());

        if (setSpec.getSubset() != null)
        {
            String strMap = "{\"@graph\":[{\"heldBy\":{\"@id\": \""+
                    LegacyIntegrationTools.legacySigelToUri(setSpec.getSubset())+
                    "\"}}]}";

            preparedStatement.setObject(2, strMap, java.sql.Types.OTHER);
        }

        preparedStatement.setFetchSize(512);

        return preparedStatement;
    }

    private static PreparedStatement prepareNodeStatement(Connection dbconn, String id)
            throws SQLException
    {
        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");
        String selectSQL = "SELECT id, data, modified, collection FROM " + tableName + " WHERE id = ?";
        PreparedStatement preparedStatement = dbconn.prepareStatement(selectSQL);
        preparedStatement.setString(1, id);

        return preparedStatement;
    }

    private static PreparedStatement prepareIdentifiersStatement(Connection dbconn, String id)
            throws SQLException
    {
        String tableName = OaiPmh.configuration.getProperty("sqlMaintable");

        String sql = "SELECT id FROM " + tableName + "__identifiers WHERE iri = ?";
        PreparedStatement preparedStatement = dbconn.prepareStatement(sql);
        preparedStatement.setString(1, id);

        return preparedStatement;
    }

    private static void parseMap(Map map, Set<String> visitedIDs, List<String> nodeDatas,
                                 ModificationTimes modificationTimes)
            throws SQLException, IOException
    {
        for (Object key : map.keySet())
        {
            Object value = map.get(key);

            if (value instanceof Map)
                parseMap( (Map) value, visitedIDs, nodeDatas, modificationTimes );
            else if (value instanceof List)
                parseList( (List) value, visitedIDs, nodeDatas, modificationTimes );
            else
                parsePotentialId( key, value, visitedIDs, nodeDatas, modificationTimes );
        }
    }

    private static void parseList(List list, Set<String> visitedIDs, List<String> nodeDatas, ModificationTimes modificationTimes)
            throws SQLException, IOException
    {
        for (Object item : list)
        {
            if (item instanceof Map)
                parseMap( (Map) item, visitedIDs, nodeDatas, modificationTimes );
            else if (item instanceof List)
                parseList( (List) item, visitedIDs, nodeDatas, modificationTimes );
        }
    }

    private static void parsePotentialId(Object key, Object value, Set<String> visitedIDs, List<String> nodeDatas, ModificationTimes modificationTimes)
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

        LinkedList<String> linkedIDs = new LinkedList<String>();

        try (Connection dbconn = OaiPmh.s_postgreSqlComponent.getConnection();
             PreparedStatement preparedStatement = prepareIdentifiersStatement(dbconn, potentialID);
             ResultSet resultSet = preparedStatement.executeQuery())
        {
            if (resultSet.next())
            {
                String id = resultSet.getString("id");
                linkedIDs.add(id);
            }
        }

        for (String id : linkedIDs)
            addNodeAndSubnodesToTree( id, visitedIDs, nodeDatas, modificationTimes );
    }

    private static void emitRecord(ResultSet resultSet, Document mergedDocument, ModificationTimes modificationTimes,
                                   XMLStreamWriter writer, String requestedFormat, boolean onlyIdentifiers)
            throws SQLException, XMLStreamException, IOException
    {
        // The ResultSet refers only to the root document. mergedDocument represents the entire tree.

        ObjectMapper mapper = new ObjectMapper();
        boolean deleted = resultSet.getBoolean("deleted");
        String sigel = resultSet.getString("sigel");
        if (sigel != null)
            sigel = LegacyIntegrationTools.uriToLegacySigel( resultSet.getString("sigel").replace("\"", "") );

        writer.writeStartElement("record");

        writer.writeStartElement("header");

        if (deleted)
            writer.writeAttribute("status", "deleted");

        writer.writeStartElement("identifier");
        writer.writeCharacters(mergedDocument.getURI().toString());
        writer.writeEndElement(); // identifier

        writer.writeStartElement("datestamp");
        //ZonedDateTime modified = ZonedDateTime.ofInstant(resultSet.getTimestamp("modified").toInstant(), ZoneOffset.UTC);
        writer.writeCharacters(modificationTimes.latestModification.toString());
        writer.writeEndElement(); // datestamp

        String dataset = resultSet.getString("collection");
        if (dataset != null)
        {
            writer.writeStartElement("setSpec");
            writer.writeCharacters(dataset);
            writer.writeEndElement(); // setSpec
        }

        if (sigel != null)
        {
            writer.writeStartElement("setSpec");
            // Output sigel without quotation marks (").
            writer.writeCharacters(dataset + ":" + sigel);
            writer.writeEndElement(); // setSpec
        }

        writer.writeEndElement(); // header

        if (!onlyIdentifiers && !deleted)
        {
            writer.writeStartElement("metadata");
            ResponseCommon.writeConvertedDocument(writer, requestedFormat, mergedDocument);
            writer.writeEndElement(); // metadata
        }

        writer.writeEndElement(); // record
    }
}
