package whelk.importer;

import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.Field;
import se.kb.libris.util.marc.MarcRecord;
import whelk.Document;
import whelk.IdGenerator;
import whelk.Whelk;
import whelk.component.ElasticSearch;
import whelk.component.PostgreSQLComponent;
import whelk.converter.MarcJSONConverter;
import whelk.converter.marc.MarcFrameConverter;
import whelk.filter.LinkFinder;
import whelk.util.LegacyIntegrationTools;
import whelk.util.PropertyLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.*;
import java.util.*;

class XL
{
    private Whelk m_whelk;
    private Parameters m_parameters;
    private Properties m_properties;
    private MarcFrameConverter m_marcFrameConverter;

    private final static String IMPORT_SYSTEM_CODE = "batch import";

    XL(Parameters parameters)
    {
        m_parameters = parameters;
        m_properties = PropertyLoader.loadProperties("secret");
        PostgreSQLComponent storage = new PostgreSQLComponent(m_properties.getProperty("sqlUrl"), m_properties.getProperty("sqlMaintable"));
        ElasticSearch elastic = new ElasticSearch(m_properties.getProperty("elasticHost"), m_properties.getProperty("elasticCluster"), m_properties.getProperty("elasticIndex"));
        m_whelk = new Whelk(storage, elastic);
        m_marcFrameConverter = new MarcFrameConverter();
    }

    /**
     * Write a ISO2709 MarcRecord to LibrisXL. returns a resource ID if the resulting document (merged or new) was in "bib".
     * This ID should then be passed (as 'relatedWithBibResourceId') when importing any subsequent related holdings post.
     * Returns null when supplied a hold post
     */
    String importISO2709(MarcRecord incomingMarcRecord, String relatedWithBibResourceId)
            throws Exception
    {
        String collection = "bib"; // assumption
        if (incomingMarcRecord.getLeader(6) == 'u' || incomingMarcRecord.getLeader(6) == 'v' ||
                incomingMarcRecord.getLeader(6) == 'x' || incomingMarcRecord.getLeader(6) == 'y')
            collection = "hold";

        Set<String> duplicateIDs = getDuplicates(incomingMarcRecord, collection, relatedWithBibResourceId);

        String resultingResourceId = null;

        if (duplicateIDs.size() == 0) // No coinciding documents, simple import
        {
            resultingResourceId = importNewRecord(incomingMarcRecord, collection, relatedWithBibResourceId);
        }
        else if (duplicateIDs.size() == 1)
        {
            // Coinciding with exactly one document. Merge or replace.

            // The only safe way to do a REPLACE (while Voyager is still around) is to properly delete the old record
            // and create a new one as a replacement, with a new ID. If we allowed replacing of the data in a record,
            // we would loose our "controlNumber" reference into voyager/vcopy and create a new duplicate there with
            // every REPLACE here. REPLACE is not currently in use for any source, do not enable this.

            // Enrich (or "merge")
            resultingResourceId = enrichRecord( (String) duplicateIDs.toArray()[0], incomingMarcRecord, collection, relatedWithBibResourceId );
        }
        else
        {
            // Multiple coinciding documents.

            if (m_parameters.getEnrichMulDup())
            {
                for (String id : duplicateIDs)
                {
                    enrichRecord( id, incomingMarcRecord, collection, relatedWithBibResourceId );
                }
            }

            if (collection.equals("bib"))
            {
                // In order to keep the program deterministic, the bib post to which subsequent holdings should attach
                // when there are multiple duplicates is defined as the one with the "lowest" alpha numeric id.
                List<String> duplicateList = new ArrayList<>(duplicateIDs);
                Collections.sort(duplicateList);
                resultingResourceId = duplicateList.get(0);
            }
            else
                resultingResourceId = null;
        }

        return resultingResourceId;
    }

    private String importNewRecord(MarcRecord marcRecord, String collection, String relatedWithBibResourceId)
    {
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

        Document rdfDoc = convertToRDF(marcRecord, generatedId);
        if (collection.equals("hold"))
            rdfDoc.setHoldingFor(relatedWithBibResourceId);

        if (!m_parameters.getReadOnly())
        {
            m_whelk.store(rdfDoc, IMPORT_SYSTEM_CODE, null, collection, false);
        }
        else
            System.out.println("Would now (if --live had been specified) have written the following json-ld to whelk:"
                    + rdfDoc.getDataAsString());

        if (collection.equals("bib"))
            return rdfDoc.getThingIdentifiers().get(0);
        return null;
    }

    private String enrichRecord(String ourId, MarcRecord incomingMarcRecord, String collection, String relatedWithBibResourceId)
            throws IOException
    {
        Document rdfDoc = convertToRDF(incomingMarcRecord, ourId);
        if (collection.equals("hold"))
            rdfDoc.setHoldingFor(relatedWithBibResourceId);

        if (!m_parameters.getReadOnly())
        {
            try
            {
                m_whelk.storeAtomicUpdate(ourId, false, IMPORT_SYSTEM_CODE, null, collection, false,
                        (Document doc) ->
                        {
                            if (collection.equals("bib"))
                            {
                                String encodingLevel = doc.getEncodingLevel();
                                if (encodingLevel == null || !encodingLevel.equals("marc:PartialPreliminaryLevel"))
                                    throw new TooHighEncodingLevelException();
                            }

                            try
                            {
                                enrich( doc, rdfDoc );
                            } catch (IOException e)
                            {
                                throw new UncheckedIOException(e);
                            }
                        });
            }
            catch (TooHighEncodingLevelException e)
            {
                System.out.println("Not enriching id: " + ourId + ", because it no longer has encoding level marc:PartialPreliminaryLevel");
            }
        }
        else
        {
            Document doc = m_whelk.getStorage().load( ourId );
            enrich( doc, rdfDoc );
            System.out.println("Would now (if --live had been specified) have written the following (merged) json-ld to whelk:");
            System.out.println("id:\n" + doc.getShortId());
            System.out.println("data:\n" + doc.getDataAsString());
        }

        if (collection.equals("bib"))
            return rdfDoc.getThingIdentifiers().get(0);
        return null;
    }

    private void enrich(Document mutableDocument, Document withDocument)
            throws IOException
    {
        JsonldSerializer serializer = new JsonldSerializer();
        List<String[]> withTriples = serializer.deserialize(withDocument.data);
        List<String[]> originalTriples = serializer.deserialize(mutableDocument.data);

        Graph originalGraph = new Graph(originalTriples);
        Graph withGraph = new Graph(withTriples);

        // This is temporary, these special rules should not be hardcoded here, but rather obtained from (presumably)
        // whelk-core's marcframe.json.
        Map<String, Graph.PREDICATE_RULES> specialRules = new HashMap<>();
        specialRules.put("created", Graph.PREDICATE_RULES.RULE_PREFER_ORIGINAL);
        specialRules.put("controlNumber", Graph.PREDICATE_RULES.RULE_PREFER_ORIGINAL);
        specialRules.put("modified", Graph.PREDICATE_RULES.RULE_PREFER_INCOMING);
        specialRules.put("marc:encLevel", Graph.PREDICATE_RULES.RULE_PREFER_ORIGINAL);

        // These should also be retrieved from whelk-core's marcframe.json.
        // The predicates listed here are those that must always be represented as lists in jsonld, even if the list
        // has only a single member.
        Set<String> alwaysSets = new HashSet<>();
        alwaysSets.add("sameAs");
        alwaysSets.add("genre");
        alwaysSets.add("comment");
        alwaysSets.add("hasComponent");
        alwaysSets.add("heldBy");

        originalGraph.enrichWith(withGraph, specialRules);

        Map enrichedData = JsonldSerializer.serialize(originalGraph.getTriples(), alwaysSets);
        JsonldSerializer.normalize(enrichedData, mutableDocument.getShortId());
        mutableDocument.data = enrichedData;
    }

    private Document convertToRDF(MarcRecord marcRecord, String id)
    {
        // The conversion process needs a 001 field to work correctly.
        if (marcRecord.getControlfields("001").size() == 0)
            marcRecord.addField(marcRecord.createControlfield("001", id));

        Map convertedData = m_marcFrameConverter.convert(MarcJSONConverter.toJSONMap(marcRecord), id);
        Document convertedDocument = new Document(convertedData);
        convertedDocument.setId(id);
        return convertedDocument;
    }

    private Set<String> getDuplicates(MarcRecord marcRecord, String collection, String relatedWithBibResourceId)
            throws SQLException
    {
        switch (collection)
        {
            case "bib":
                return getBibDuplicates(marcRecord);
            case "hold":
                return getHoldDuplicates(marcRecord, relatedWithBibResourceId);
            default:
                return new HashSet<>();
        }
    }

    private Set<String> getHoldDuplicates(MarcRecord marcRecord, String relatedWithBibResourceId)
            throws SQLException
    {
        Set<String> duplicateIDs = new HashSet<>();

        // Assumes the post being imported carries a valid libris id in 001, and "SE-LIBR" or "LIBRIS" in 003
        duplicateIDs.addAll(getDuplicatesOnLibrisID(marcRecord, "hold"));
        duplicateIDs.addAll(getDuplicatesOnHeldByHoldingFor(marcRecord, relatedWithBibResourceId));

        return duplicateIDs;
    }

    private Set<String> getBibDuplicates(MarcRecord marcRecord)
            throws SQLException
    {
        Set<String> duplicateIDs = new HashSet<>();

        for (Parameters.DUPLICATION_TYPE dupType : m_parameters.getDuplicationTypes())
        {
            switch (dupType)
            {
                case DUPTYPE_ISBNA: // International Standard Book Number (only from subfield A)
                    for (Field field : marcRecord.getFields("020"))
                    {
                        String isbn = DigId.grepIsbna( (Datafield) field );
                        if (isbn != null)
                        {
                            duplicateIDs.addAll(getDuplicatesOnISBN( isbn.toLowerCase() ));
                            duplicateIDs.addAll(getDuplicatesOnISBN( isbn.toUpperCase() ));
                        }
                    }
                    break;
                case DUPTYPE_ISBNZ: // International Standard Book Number (only from subfield Z)
                    for (Field field : marcRecord.getFields("020"))
                    {
                        String isbn = DigId.grepIsbnz( (Datafield) field );
                        if (isbn != null)
                        {
                            duplicateIDs.addAll(getDuplicatesOnISBN( isbn.toLowerCase() ));
                            duplicateIDs.addAll(getDuplicatesOnISBN( isbn.toUpperCase() ));
                        }
                    }
                    break;
                case DUPTYPE_ISSNA: // International Standard Serial Number (only from marc 022_A)
                    for (Field field : marcRecord.getFields("022"))
                    {
                        String issn = DigId.grepIssn( (Datafield) field, 'a' );
                        if (issn != null)
                        {
                            duplicateIDs.addAll(getDuplicatesOnISSN( issn.toLowerCase() ));
                            duplicateIDs.addAll(getDuplicatesOnISSN( issn.toUpperCase() ));
                        }
                    }
                    break;
                case DUPTYPE_ISSNZ: // International Standard Serial Number (only from marc 022_Z)
                    for (Field field : marcRecord.getFields("022"))
                    {
                        String issn = DigId.grepIssn( (Datafield) field, 'z' );
                        if (issn != null)
                        {
                            duplicateIDs.addAll(getDuplicatesOnISSN( issn.toLowerCase() ));
                            duplicateIDs.addAll(getDuplicatesOnISSN( issn.toUpperCase() ));
                        }
                    }
                    break;
                case DUPTYPE_035A:
                    // Unique id number in another system. The 035a of the post being imported will be checked against
                    // the @graph,0,systemNumber array of existing posts
                    duplicateIDs.addAll(getDuplicatesOn035a(marcRecord));
                    break;
                case DUPTYPE_LIBRISID:
                    // Assumes the post being imported carries a valid libris id in 001, and "SE-LIBR" or "LIBRIS" in 003
                    duplicateIDs.addAll(getDuplicatesOnLibrisID(marcRecord, "bib"));
                    break;
            }
        }

        return duplicateIDs;
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

        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement statement = getOnId_ps(connection, librisId);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private List<String> getDuplicatesOn035a(MarcRecord marcRecord)
            throws SQLException
    {
        // Get all of marcRecord's 035a (unique id in other system) id entries in a list
        List<String> candidate035aIDs = new ArrayList<>();
        for (Field field : marcRecord.getFields("035"))
        {
            candidate035aIDs.add( DigId.grep035a( (Datafield) field ) );
        }

        if (candidate035aIDs.isEmpty())
            return new ArrayList<>();

        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement statement = getOnSystemNumber_ps(connection, candidate035aIDs);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private List<String> getDuplicatesOnISBN(String isbn)
            throws SQLException
    {
        if (isbn == null)
            return new ArrayList<>();

        String numericIsbn = isbn.replaceAll("-", "");
        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement statement = getOnISBN_ps(connection, numericIsbn);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private List<String> getDuplicatesOnISSN(String issn)
            throws SQLException
    {
        if (issn == null)
            return new ArrayList<>();

        String numericIssn = issn.replaceAll("-", "");
        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement statement = getOnISSN_ps(connection, numericIssn);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private List<String> getDuplicatesOnHeldByHoldingFor(MarcRecord marcRecord, String relatedWithBibResourceId)
            throws SQLException
    {
        if (marcRecord.getFields("852").size() < 1)
            return new ArrayList<>();
        Datafield df = (Datafield) marcRecord.getFields("852").get(0);
        if (df.getSubfields("b").size() < 1)
            return new ArrayList<>();
        String sigel = df.getSubfields("b").get(0).getData();

        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement statement = getOnHeldByHoldingFor_ps(connection, sigel, relatedWithBibResourceId);
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

    /**
     * "System number" is our ld equivalent of marc's 035a
     */
    private PreparedStatement getOnSystemNumber_ps(Connection connection, List<String> ids)
            throws SQLException
    {
        String query = "SELECT id FROM lddb WHERE data#>'{@graph,0,systemNumber}' ??| ?";
        PreparedStatement statement = connection.prepareStatement(query);

        Array tmpArr = connection.createArrayOf("text", ids.toArray());
        statement.setObject(1, tmpArr, Types.ARRAY);
        return statement;
    }

    private PreparedStatement getOnISBN_ps(Connection connection, String isbn)
            throws SQLException
    {
        // required to be completely numeric (base 11, 0-9+x).
        if (!isbn.matches("[\\dxX]+"))
            isbn = "0";

        String query = "SELECT id FROM lddb WHERE data#>'{@graph,1,identifiedBy}' @> ?";
        PreparedStatement statement =  connection.prepareStatement(query);
        
        statement.setObject(1, "[{\"@type\": \"ISBN\", \"value\": \"" + isbn + "\"}]", java.sql.Types.OTHER);

        return  statement;
    }

    private PreparedStatement getOnISSN_ps(Connection connection, String issn)
            throws SQLException
    {
        // required to be completely numeric (base 11, 0-9+x).
        if (!issn.matches("[\\dxX]+"))
            issn = "0";

        String query = "SELECT id FROM lddb WHERE data#>'{@graph,1,identifiedBy}' @> ?";
        PreparedStatement statement =  connection.prepareStatement(query);

        statement.setObject(1, "[{\"@type\": \"ISSN\", \"value\": \"" + issn + "\"}]", java.sql.Types.OTHER);

        return  statement;
    }

    private PreparedStatement getOnHeldByHoldingFor_ps(Connection connection, String heldBy, String holdingForId)
            throws SQLException
    {
        String libraryUri = LegacyIntegrationTools.legacySigelToUri(heldBy);

        String query = "SELECT id FROM lddb WHERE data#>>'{@graph,1,hasComponent,0,heldBy,0,@id}' = ? AND data#>>'{@graph,1,holdingFor,@id}' = ? AND collection = 'hold'";
        PreparedStatement statement = connection.prepareStatement(query);

        statement.setString(1, libraryUri);
        statement.setString(2, holdingForId);

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

    private class TooHighEncodingLevelException extends RuntimeException {}
}
