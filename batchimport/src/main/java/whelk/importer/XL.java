package whelk.importer;

import io.prometheus.client.Counter;
import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.Field;
import se.kb.libris.util.marc.MarcRecord;
import whelk.Document;
import whelk.IdGenerator;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.component.ElasticSearch;
import whelk.component.PostgreSQLComponent;
import whelk.converter.MarcJSONConverter;
import whelk.converter.marc.MarcFrameConverter;
import whelk.util.LegacyIntegrationTools;
import whelk.util.PropertyLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.*;
import java.util.*;

class XL
{
    private static final String PRELIMINARY_STATUS = "marc:PartialPreliminaryLevel";
    private Whelk m_whelk;
    private Parameters m_parameters;
    private Properties m_properties;
    private MarcFrameConverter m_marcFrameConverter;
    private static boolean verbose = false;

    // The predicates listed here are those that must always be represented as lists in jsonld, even if the list
    // has only a single member.
    private Set<String> m_forcedSetTerms;

    private final static String IMPORT_SYSTEM_CODE = "batch import";

    XL(Parameters parameters) throws IOException
    {
        m_parameters = parameters;
        verbose = m_parameters.getVerbose();
        m_properties = PropertyLoader.loadProperties("secret");
        PostgreSQLComponent storage = new PostgreSQLComponent(m_properties.getProperty("sqlUrl"), m_properties.getProperty("sqlMaintable"));
        ElasticSearch elastic = new ElasticSearch(m_properties.getProperty("elasticHost"), m_properties.getProperty("elasticCluster"), m_properties.getProperty("elasticIndex"));
        m_whelk = new Whelk(storage, elastic);
        m_whelk.loadCoreData();
        m_forcedSetTerms = new JsonLd(m_whelk.getDisplayData(), m_whelk.getVocabData()).getForcedSetTerms();
        m_marcFrameConverter = new MarcFrameConverter();
    }

    /**
     * Write a ISO2709 MarcRecord to LibrisXL. returns a resource ID if the resulting document (merged or new) was in "bib".
     * This ID should then be passed (as 'relatedWithBibResourceId') when importing any subsequent related holdings post.
     * Returns null when supplied a hold post.
     */
    String importISO2709(MarcRecord incomingMarcRecord,
                         String relatedWithBibResourceId,
                         Counter importedBibRecords,
                         Counter importedHoldRecords,
                         Counter enrichedBibRecords,
                         Counter enrichedHoldRecords,
                         Counter encounteredMulBibs)
            throws Exception
    {
        String collection = "bib"; // assumption
        if (incomingMarcRecord.getLeader(6) == 'u' || incomingMarcRecord.getLeader(6) == 'v' ||
                incomingMarcRecord.getLeader(6) == 'x' || incomingMarcRecord.getLeader(6) == 'y')
            collection = "hold";

        Set<String> duplicateIDs = getDuplicates(incomingMarcRecord, collection, relatedWithBibResourceId);

        String resultingResourceId = null;

        //System.err.println("Incoming [" + collection + "] document had: " + duplicateIDs.size() + " existing duplicates:\n" + duplicateIDs);

        // If an incoming holding record is marked deleted, attempt to find any duplicates for it in Libris and delete them.
        if (collection.equals("hold") && incomingMarcRecord.getLeader(5) == 'd')
        {
            for (String id : duplicateIDs)
                m_whelk.remove(id, IMPORT_SYSTEM_CODE, null, "hold");
            return null;
        }

        if (duplicateIDs.size() == 0) // No coinciding documents, simple import
        {
            resultingResourceId = importNewRecord(incomingMarcRecord, collection, relatedWithBibResourceId);

            if (collection.equals("bib"))
                importedBibRecords.inc();
            else
                importedHoldRecords.inc();
        }
        else if (duplicateIDs.size() == 1)
        {
            // Enrich (or "merge")
            resultingResourceId = enrichRecord( (String) duplicateIDs.toArray()[0], incomingMarcRecord, collection, relatedWithBibResourceId );

            if (collection.equals("bib"))
                enrichedBibRecords.inc();
            else
                enrichedHoldRecords.inc();
        }
        else
        {
            // Multiple coinciding documents.
            encounteredMulBibs.inc();

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
                String selectedDuplicateId = duplicateList.get(0);
                if (!selectedDuplicateId.startsWith(Document.getBASE_URI().toString()))
                    selectedDuplicateId = Document.getBASE_URI().toString() + selectedDuplicateId;
                resultingResourceId = m_whelk.getStorage().getThingId(selectedDuplicateId);
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
            rdfDoc.setRecordStatus(PRELIMINARY_STATUS);
            m_whelk.createDocument(rdfDoc, IMPORT_SYSTEM_CODE, null, collection, false);
        }
        else
        {
            if ( verbose )
            {
                System.out.println("info: Would now (if --live had been specified) have written the following json-ld to whelk as a new record:\n"
                + rdfDoc.getDataAsString());
            }
        }

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
                                if (encodingLevel == null || !encodingLevel.equals(PRELIMINARY_STATUS))
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
                if ( verbose )
                {
                    System.out.println("info: Not enriching id: " + ourId + ", because it no longer has encoding level marc:PartialPreliminaryLevel");
                }
            }
        }
        else
        {
            Document doc = m_whelk.getStorage().load( ourId );
            enrich( doc, rdfDoc );
            if ( verbose )
            {
                System.out.println("info: Would now (if --live had been specified) have written the following (merged) json-ld to whelk:\n");
                System.out.println("id:\n" + doc.getShortId());
                System.out.println("data:\n" + doc.getDataAsString());
            }
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
        for (String term : m_forcedSetTerms)
            specialRules.put(term, Graph.PREDICATE_RULES.RULE_AGGREGATE);
        specialRules.put("created", Graph.PREDICATE_RULES.RULE_PREFER_ORIGINAL);
        specialRules.put("controlNumber", Graph.PREDICATE_RULES.RULE_PREFER_ORIGINAL);
        specialRules.put("modified", Graph.PREDICATE_RULES.RULE_PREFER_INCOMING);
        specialRules.put("marc:encLevel", Graph.PREDICATE_RULES.RULE_PREFER_ORIGINAL);

        originalGraph.enrichWith(withGraph, specialRules);

        Map enrichedData = JsonldSerializer.serialize(originalGraph.getTriples(), m_forcedSetTerms);
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
        List<String> results = new ArrayList<>();
        for (Field field : marcRecord.getFields("035"))
        {
            String systemNumber = DigId.grep035a( (Datafield) field );

            try(Connection connection = m_whelk.getStorage().getConnection();
                PreparedStatement statement = getOnSystemNumber_ps(connection, systemNumber);
                ResultSet resultSet = statement.executeQuery())
            {
                results.addAll( collectIDs(resultSet) );
            }
        }
        return results;
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

        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement statement = getOnISSN_ps(connection, issn);
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
        String library = LegacyIntegrationTools.legacySigelToUri(sigel);

        try(Connection connection = m_whelk.getStorage().getConnection();
            PreparedStatement statement = getOnHeldByHoldingFor_ps(connection, library, relatedWithBibResourceId);
            ResultSet resultSet = statement.executeQuery())
        {
            return collectIDs(resultSet);
        }
    }

    private PreparedStatement getOnId_ps(Connection connection, String id)
            throws SQLException
    {
        String query = "SELECT id FROM lddb__identifiers WHERE iri = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, id);
        return statement;
    }

    /**
     * "System number" is our ld equivalent of marc's 035a
     */
    private PreparedStatement getOnSystemNumber_ps(Connection connection, String systemNumber)
            throws SQLException
    {
        String query = "SELECT id FROM lddb WHERE data#>'{@graph,0,identifiedBy}' @> ?";
        PreparedStatement statement =  connection.prepareStatement(query);

        statement.setObject(1, "[{\"@type\": \"SystemNumber\", \"value\": \"" + systemNumber + "\"}]", java.sql.Types.OTHER);

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
        // (base 11, 0-9+x and SINGLE hyphens only).
        if (!issn.matches("^(-[xX\\d]|[xX\\d])+$"))
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

        // Here be dragons. The allways-works query is this:
        /*String query =
                "SELECT lddb.id from lddb " +
                "INNER JOIN lddb__identifiers id1 ON lddb.data#>>'{@graph,1,itemOf,@id}' = id1.iri " +
                "INNER JOIN lddb__identifiers id2 ON id1.id = id2.id " +
                "WHERE " +
                "data#>>'{@graph,1,heldBy,@id}' = ? " +
                "AND " +
                "id2.iri = ?";*/

        // This query REQUIRES that links be on the primary ID only. This works beacuse of link-finding step2, but if
        // that should ever change this query would break.

        String query = "SELECT id from lddb WHERE data#>>'{@graph,1,heldBy,@id}' = ? AND data#>>'{@graph,1,itemOf,@id}' = ? AND deleted = false";

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
