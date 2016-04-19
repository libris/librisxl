package whelk.importer;

import se.kb.libris.util.marc.MarcRecord;
import whelk.Document;
import whelk.IdGenerator;
import whelk.component.ElasticSearch;
import whelk.component.PostgreSQLComponent;
import whelk.converter.MarcJSONConverter;
import whelk.converter.marc.MarcFrameConverter;
import whelk.util.PropertyLoader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

class XL
{
    private PostgreSQLComponent m_postgreSQLComponent;
    private ElasticSearch m_elasticSearchComponent;
    private Parameters m_parameters;
    private Properties m_properties;
    private MarcFrameConverter m_marcFrameConverter;

    XL(Parameters parameters)
    {
        m_parameters = parameters;
        m_properties = PropertyLoader.loadProperties("secret");
        m_postgreSQLComponent = new PostgreSQLComponent(m_properties.getProperty("sqlUrl"), m_properties.getProperty("sqlMaintable"));
        m_elasticSearchComponent = new ElasticSearch(m_properties.getProperty("elasticHost"), m_properties.getProperty("elasticCluster"), m_properties.getProperty("elasticIndex"));
        m_marcFrameConverter = new MarcFrameConverter();
    }

    /**
     * Write a ISO2709 MarcRecord to LibrisXL
     */
    void importISO2709(MarcRecord marcRecord)
            throws Exception
    {
        String collection = "bib"; // assumption
        if (marcRecord.getLeader(6) == 'u' || marcRecord.getLeader(6) == 'v' ||
                marcRecord.getLeader(6) == 'x' || marcRecord.getLeader(6) == 'y')
            collection = "hold";

        List<String> duplicateIDs = new ArrayList<>();

        for (Parameters.DUPLICATION_TYPE dupType : m_parameters.getDuplicationTypes())
        {
            switch (dupType)
            {
                case DUPTYPE_ISBN:
                case DUPTYPE_ISSN:
                case DUPTYPE_ISNA:
                case DUPTYPE_ISNZ:
                case DUPTYPE_URN:
                case DUPTYPE_OAI:
                case DUPTYPE_035A:
                    break;
                case DUPTYPE_LIBRISID:
                    duplicateIDs.addAll(getDuplicatesOnLibrisID(marcRecord, collection));
                    break;
            }
        }

        if (duplicateIDs.size() == 0)
        {
            // No coinciding documents, simple import
            Document rdfDoc = convertToRDF(marcRecord, collection, IdGenerator.generate(), null);
            m_postgreSQLComponent.store(rdfDoc, false);
            m_elasticSearchComponent.index(rdfDoc);
        }
        else if (duplicateIDs.size() == 1)
        {
            // Coinciding with exactly one document. Merge or replace?
            System.out.println("Would now MERGE OR REPLACE iso2709 record: " + marcRecord.toString());
        }
        else
        {
            // Multiple coinciding documents. Exit with error?
            throw new Exception("Multiple duplicates for this record.");
        }
    }

    private Document convertToRDF(MarcRecord marcRecord, String collection, String id, List<String> altIDs)
    {
        Map<String, Object> manifest = new HashMap<>();
        manifest.put(Document.getID_KEY(), id);
        manifest.put(Document.getCOLLECTION_KEY(), collection);
        manifest.put(Document.getCHANGED_IN_KEY(), "FTP-import");
        if (altIDs != null)
            manifest.put(Document.getALTERNATE_ID_KEY(), altIDs);

        Document doc = new Document(MarcJSONConverter.toJSONMap(marcRecord), manifest);
        return m_marcFrameConverter.convert(doc);
    }

    private List<String> getDuplicatesOnLibrisID(MarcRecord marcRecord, String collection)
            throws SQLException
    {
        String field001data = marcRecord.getControlfields("001").get(0).getData();

        String id = null;

        if (field001data.startsWith(Document.getBASE_URI().toString())) // xl id
        {
            id = field001data;
        }
        else if (field001data.matches("[0-9]+")) // completely numeric? = classic voyager id
        {
            String oldStyleIdentifier = "/"+collection+"/"+field001data;
            id = oldStyleIdentifier;
        }

        try(Connection connection = m_postgreSQLComponent.getConnection();
            PreparedStatement statement = getOnId_ps(connection, id);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private PreparedStatement getOnId_ps(Connection connection, String id)
            throws SQLException
    {
        String query = "SELECT id FROM lddb__identifiers WHERE identifier = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, id);
        return statement;
    }

    private List<String> collectIDs(ResultSet resultSet)
            throws SQLException
    {
        List<String> ids = new ArrayList<>();
        while (resultSet.next())
        {
            ids.add(resultSet.getString("id"));
        }
        return ids;
    }
}
