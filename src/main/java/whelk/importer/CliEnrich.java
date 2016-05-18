package whelk.importer;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CliEnrich
{
    public static void enrich(String originalFilename, String withFilename)
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();

        JsonldSerializer serializer = new JsonldSerializer();
        List<String[]> otherTriples = serializer.deserialize(mapper.readValue(readFileFully(withFilename), HashMap.class));
        List<String[]> originalTriples = serializer.deserialize(mapper.readValue(readFileFully(originalFilename), HashMap.class));

        Graph originalGraph = new Graph(originalTriples);
        Graph otherGraph = new Graph(otherTriples);

        //System.out.println( "original:\n" + originalGraph );
        //System.out.println( "other:\n" + otherGraph );

        originalGraph.enrichWith(otherGraph);

        //System.out.println( "enriched:\n" + originalGraph );

        Map enrichedData = JsonldSerializer.serialize(originalGraph.getTriples());
        JsonldSerializer.normalize(enrichedData, "noid");
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
