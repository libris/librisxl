package transform;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.JsonLd;
import whelk.triples.JsonldSerializer;
import whelk.util.TransformScript;

import java.io.*;
import java.util.Map;

public class Main
{
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException, TransformScript.TransformSyntaxException
    {
        if (args.length == 0)
        {
            javax.swing.SwingUtilities.invokeLater(new Runnable()
            {
                public void run()
                {
                    new ExecuteGui();
                }
            });
        }
        else if (args[0].equals("generate"))
            generateAndPrintTransform(args);
        else if (args[0].equals("execute"))
            executeScript(args);
        else
            System.err.println(
                    "Usage:\n" +
                            "java -jar transform.jar generate [file1] [file2]\n" +
                            "  Generate a diff script from using values found in the streams\n" +
                            "  each element in order in file1 and file2 is expected to represent the \"the same\"" +
                            "  record, in the new format forms.\n" +
                            "java -jar transform.jar execute [scriptfile] [file]\n" +
                            "  Execute the given script on each json document in [file], one document per line.");
    }

    private static void generateAndPrintTransform(String[] args) throws IOException, TransformScript.TransformSyntaxException
    {
        BufferedReader json1Reader = new BufferedReader(new FileReader(args[1]));
        BufferedReader json2Reader = new BufferedReader(new FileReader(args[2]));

        String jsonString;

        Syntax syntax1 = new Syntax();
        while ( (jsonString = json1Reader.readLine()) != null)
        {
            Map data = mapper.readValue(jsonString, Map.class);
            Document doc = new Document(data);
            data = JsonLd.frame(doc.getCompleteId(), data);
            syntax1.expandSyntaxToCover(data);
        }
        json1Reader.close();

        Syntax syntax2 = new Syntax();
        while ( (jsonString = json2Reader.readLine()) != null)
        {
            Map data = mapper.readValue(jsonString, Map.class);
            Document doc = new Document(data);
            data = JsonLd.frame(doc.getCompleteId(), data);
            syntax2.expandSyntaxToCover(data);
        }
        json2Reader.close();

        json1Reader = new BufferedReader(new FileReader(args[1]));
        json2Reader = new BufferedReader(new FileReader(args[2]));

        ScriptGenerator scriptGenerator = SyntaxDiffReduce.generateScript(syntax1, syntax2, json1Reader, json2Reader);

        json1Reader.close();
        json2Reader.close();

        System.out.println(scriptGenerator);
    }

    private static void executeScript(String[] args) throws IOException, TransformScript.TransformSyntaxException
    {
        BufferedReader scriptReader = new BufferedReader(new FileReader(args[1]));
        StringBuilder scriptText = new StringBuilder();
        for(String line = scriptReader.readLine(); line != null; line = scriptReader.readLine())
        {
            scriptText.append(line + "\n");
        }

        TransformScript script = new TransformScript(scriptText.toString());

        BufferedReader jsonReader = new BufferedReader(new FileReader(args[2]));
        for(String line = jsonReader.readLine(); line != null; line = jsonReader.readLine())
        {
            Map data = mapper.readValue(line, Map.class);
            Map transformed = script.executeOn(data);
            transformed = JsonLd.flatten(transformed);
            JsonldSerializer.normalize(transformed, (String) Document._get(Document.getRecordIdPath(), data), true);
            System.out.println(mapper.writeValueAsString(transformed));
        }
    }
}
