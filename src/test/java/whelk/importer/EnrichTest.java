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

        if (!enrichedData.equals(expectedResultData))
        {
            System.out.println("not equal:");
            System.out.println(enrichedData);
            System.out.println(expectedResultData);
            Assert.assertTrue( false );
        }
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
