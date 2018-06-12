package whelk.importer;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

import whelk.Document;
import whelk.Whelk;
import whelk.triples.*;
import whelk.util.PropertyLoader;

public class CliEnrich
{
    public static void enrich(String originalFilename, String withFilename)
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();

        JsonldSerializer serializer = new JsonldSerializer();
        Map originalData = mapper.readValue(readFileFully(withFilename), HashMap.class);
        List<String[]> otherTriples = serializer.deserialize(originalData);
        Map withData = mapper.readValue(readFileFully(originalFilename), HashMap.class);
        List<String[]> originalTriples = serializer.deserialize(withData);

        Graph originalGraph = new Graph(originalTriples);
        Graph otherGraph = new Graph(otherTriples);

        //Map<String, Graph.PREDICATE_RULES> specialRules = new HashMap<>();

        Properties properties = PropertyLoader.loadProperties("secret");
        Whelk whelk = Whelk.createLoadedSearchWhelk(properties);
        Set<String> repeatableTerms = whelk.getJsonld().getRepeatableTerms();

        Map<String, Graph.PREDICATE_RULES> specialRules = new HashMap<>();
        for (String term : repeatableTerms)
            specialRules.put(term, Graph.PREDICATE_RULES.RULE_AGGREGATE);
        specialRules.put("created", Graph.PREDICATE_RULES.RULE_PREFER_ORIGINAL);
        specialRules.put("controlNumber", Graph.PREDICATE_RULES.RULE_PREFER_ORIGINAL);
        specialRules.put("modified", Graph.PREDICATE_RULES.RULE_PREFER_INCOMING);
        specialRules.put("marc:encLevel", Graph.PREDICATE_RULES.RULE_PREFER_ORIGINAL);

        originalGraph.enrichWith(otherGraph, specialRules);

        Map enrichedData = JsonldSerializer.serialize(originalGraph.getTriples(), new HashSet<>());
        boolean deleteUnreferencedData = true;
        JsonldSerializer.normalize(enrichedData, new Document(originalData).getCompleteId(), deleteUnreferencedData);
        System.out.println(mapper.writeValueAsString(enrichedData));
    }

    private static String readFileFully(String filename)
            throws Exception
    {
        File file = new File(filename);
        FileInputStream stream = new FileInputStream(file);
        byte[] data = new byte[(int)file.length()];
        stream.read(data);
        stream.close();
        return new String(data, "UTF-8");
    }
}
