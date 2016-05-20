package whelk.importer;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.*;

import java.io.IOException;
import java.util.*;

public class EnrichTest
{
    private static final ObjectMapper s_mapper = new ObjectMapper();

    private static final String MAIN_ID = "main id";

    private void testEnrich(String jsonldOriginal, String jsonldIncoming, String jsonldExpectedResult)
            throws IOException
    {
        Map originalData = s_mapper.readValue(jsonldOriginal, HashMap.class);
        Map incomingData = s_mapper.readValue(jsonldIncoming, HashMap.class);
        Map expectedResultData = s_mapper.readValue(jsonldExpectedResult, HashMap.class);

        JsonldSerializer serializer = new JsonldSerializer();
        List<String[]> originalTriples = serializer.deserialize(originalData);
        List<String[]> otherTriples = serializer.deserialize(incomingData);

        Graph originalGraph = new Graph(originalTriples);
        Graph otherGraph = new Graph(otherTriples);
        Map<String, Graph.PREDICATE_RULES> specialRules = new HashMap<>();

        originalGraph.enrichWith(otherGraph, specialRules);

        Map enrichedData = JsonldSerializer.serialize(originalGraph.getTriples(), new HashSet<>());
        JsonldSerializer.normalize(enrichedData, MAIN_ID);

        if ( ! rdfEquals( enrichedData, expectedResultData ) )
        {
            System.out.println("not equal:");
            System.out.println("enriched result:\n"+enrichedData);
            System.out.println("expected result:\n"+expectedResultData);
            Assert.assertTrue( false );
        }
    }

    private boolean rdfEquals(Map m1, Map m2)
    {
        if (m1.size() != m2.size())
            return false;

        for (Object key : m1.keySet())
        {
            Object value = m1.get(key);

            if (!m2.containsKey(key))
                return false;

            if (value instanceof Map)
            {
                if ( ! rdfEquals( (Map) value, (Map) m2.get(key)) )
                    return false;
            }
            else if (value instanceof List)
            {
                if ( ! rdfEquals( (List) value, (List) m2.get(key)) )
                    return false;
            }
            else if ( ! value.equals(m2.get(key)) )
                return false;
        }

        return true;
    }

    private boolean rdfEquals(List l1, List l2)
    {
        if (l1.size() != l2.size())
            return false;

        Set s1 = new HashSet(l1);
        Set s2 = new HashSet(l2);

        for (Object entry1 : s1)
        {
            boolean existsAMatch = false;
            for (Object entry2 : s2)
            {
                if (entry1 instanceof Map && entry2 instanceof Map)
                {
                    if ( rdfEquals( (Map) entry1, (Map) entry2) )
                        existsAMatch = true;
                }
                else if (entry1 instanceof List && entry2 instanceof List)
                {
                    if ( rdfEquals( (List) entry1, (List) entry2) )
                        existsAMatch = true;
                }
            }
            if (!existsAMatch)
                return false;
        }

        return true;
    }


    @Test
    public void identity() throws Exception
    {
        // Enriching a graph with itself must not change the graph

        String original = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"@id\": \"some id\",\n" +
                "            \"bnode subobject\": {\n" +
                "                \"some literal\": \"some value\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        testEnrich(original, original, original);
    }

    @Test
    public void extraGraphListObject() throws Exception
    {
        // Adding another object in the @graph list. Otherwise identical

        String original = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"@id\": \"some id\",\n" +
                "            \"bnode subobject\": {\n" +
                "                \"some literal\": \"some value\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        String incoming = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"@id\": \"some id\",\n" +
                "            \"bnode subobject\": {\n" +
                "                \"some literal\": \"some value\"\n" +
                "            }\n" +
                "        },\n" +
                "\t{\n" +
                "            \"@id\": \"other id\",\n" +
                "            \"bnode subobject\": {\n" +
                "                \"some literal\": \"some value\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        testEnrich(original, incoming, incoming);
    }

    @Test
    public void extraInfoInBNode() throws Exception
    {
        // Adding an extra key value pair in a BNode

        String original = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"@id\": \"some id\",\n" +
                "            \"bnode subobject\": {\n" +
                "                \"some literal\": \"some value\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        String incoming = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"@id\": \"some id\",\n" +
                "            \"bnode subobject\": {\n" +
                "                \"other literal\": \"other value\",\n" +
                "                \"some literal\": \"some value\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        testEnrich(original, incoming, incoming);
    }

    @Test
    public void extraInfoNestedBNode() throws Exception
    {
        // Adding an extra key value pair in a BNode, containing another nested BNode that must be matched

        String original = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"some subobject\": {\n" +
                "                \"some literal\": \"some value\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"this bnode\": \"should not match\"\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        String incoming = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"other literal\": \"other value\",\n" +
                "            \"some subobject\": {\n" +
                "                \"some literal\": \"some value\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        String expectedResult = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"other literal\": \"other value\",\n" +
                "            \"some subobject\": {\n" +
                "                \"some literal\": \"some value\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"this bnode\": \"should not match\"\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        testEnrich(original, incoming, expectedResult);
    }

    @Test
    public void combinedNamedNode() throws Exception
    {
        // Combining properties of two named nodes with the same name

        String original = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"@id\": \"some id\",\n" +
                "            \"some literal\" : \"some value\"\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        String incoming = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"@id\": \"some id\",\n" +
                "            \"other literal\" : \"other value\"\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        String expectedResult = "{\n" +
                "    \"@graph\": [\n" +
                "        {\n" +
                "            \"@id\": \"some id\",\n" +
                "            \"other literal\": \"other value\",\n" +
                "            \"some literal\": \"some value\"\n" +
                "        }\n" +
                "    ]\n" +
                "}\n";

        testEnrich(original, incoming, expectedResult);
    }
}
