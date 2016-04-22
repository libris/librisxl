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
                case DUPTYPE_ISBN: // International Standard Book Number
                    break;
                case DUPTYPE_ISSN: // International Standard Serial Number (marc 022_A or 022_Z)
                case DUPTYPE_ISNA: // International Standard Serial Number (only from marc 022_A)
                case DUPTYPE_ISNZ: // International Standard Serial Number (only from marc 022_Z)
                    break;
                case DUPTYPE_URN: // Unknown and not in use by any provider, to be removed!
                    break;
                case DUPTYPE_OAI: // ?
                case DUPTYPE_035A: // Unique id number in another system
                    break;
                case DUPTYPE_LIBRISID:
                    duplicateIDs.addAll(getDuplicatesOnLibrisID(marcRecord, collection));
                    break;
            }
        }

        if (duplicateIDs.size() == 0)
        {
            // No coinciding documents, simple import

            // Delete any existing 001 fields
            String generatedId = IdGenerator.generate();
            if (marcRecord.getControlfields("001").size() != 0)
            {
                marcRecord.getFields().remove(marcRecord.getControlfields("001").get(0));
            }

            // Always write a new 001. If one existed in the imported post it was moved to 035a.
            // If it was not (because it was a valid libris id) then it was checked as a duplicate and
            // duplicateIDs.size would be > 0, and we would not be here.
            marcRecord.addField(marcRecord.createControlfield("001", generatedId));

            Document rdfDoc = convertToRDF(marcRecord, collection, generatedId, null);
            if (!m_parameters.getReadOnly())
            {
                m_postgreSQLComponent.store(rdfDoc, false);
                m_elasticSearchComponent.index(rdfDoc);
            }
            else
                System.out.println("Would now (if --live had been specified) have written the following json-ld to whelk:"
                        + rdfDoc.getDataAsString());
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
        String librisId = DigId.grepLibrisId(marcRecord);

        if (librisId == null)
            return new ArrayList<>();

        // completely numeric? = classic voyager id.
        // In theory an xl id could (though insanely unlikely) also be numeric :(
        if (librisId.matches("[0-9]+"))
        {
            librisId = "http://libris.kb.se/"+collection+"/"+librisId;
        }
        else if ( ! librisId.startsWith(Document.getBASE_URI().toString()))
        {
            librisId = Document.getBASE_URI().toString() + librisId;
        }

        try(Connection connection = m_postgreSQLComponent.getConnection();
            PreparedStatement statement = getOnId_ps(connection, librisId);
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
