package whelk.util;

import whelk.Document;
import whelk.Whelk;
import whelk.component.ElasticSearch;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static whelk.util.Jackson.mapper;

// TODO: factor out and use MarcFrameCli.addJsonLd to allow for local data.
public class DataViewCli {
    private static final List<String> COMMANDS = List.of("card", "chip", "embellish", "index");

    private static Whelk whelk;

    public static void main(String[] args) throws Exception {
        if (System.getProperty("xl.secret.properties") == null) {
            throw new IllegalStateException("Missing system property 'xl.secret.properties'");
        }

        whelk = Whelk.createLoadedCoreWhelk();

        String cmd = "card";
        List<String> refs;

        if (args.length > 1) {
            cmd = args[0];
            if (!COMMANDS.contains(cmd)) {
                throw new IllegalArgumentException("Unknown command: " + cmd);
            }
            refs = Arrays.asList(args).subList(1, args.length);
        } else {
            refs = Arrays.asList(args);
        }

        for (String ref : refs) {
            Object result = view(cmd, ref);
            System.out.println(mapper.writeValueAsString(result));
        }
    }

    @SuppressWarnings("unchecked")
    private static Object view(String cmd, String ref) throws Exception {
        File file = new File(ref);
        Map<String, Object> data = file.exists() ? mapper.readValue(file, Map.class) : null;
        switch (cmd) {
            // TODO: most useful if local data can be loaded (see TODO above).
            //case 'card':
            //    break
            //case 'chip':
            //    break
            case "embellish":
                if (data != null) {
                    Document doc = new Document(data);
                    whelk.embellish(doc);
                    return doc.data;
                } else {
                    Document doc = whelk.getDocument(ref);
                    if (doc == null) {
                        throw new IllegalArgumentException(ref + " is not found?");
                    }
                    System.err.println(cmd + ": " + doc.getId() + " (data is " + sizeOf(doc.data) + " bytes)");
                    whelk.embellish(doc);
                    System.err.println("Done (data is now " + sizeOf(doc.data) + " bytes)");
                    return doc.data;
                }
            case "index":
                Document doc = new Document(data);
                return ElasticSearch.getShapeForIndex(doc, whelk);
        }
        return null;
    }

    private static int sizeOf(Object data) throws Exception {
        return mapper.writeValueAsString(data).length();
    }
}
