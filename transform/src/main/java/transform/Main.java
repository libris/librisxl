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
        else if (args[0].equals("syntax"))
            generateAndPrintSyntax(args);
        else if (args[0].equals("transform"))
            generateAndPrintTransform(args);
        else if (args[0].equals("execute"))
            executeScript(args);
        else
            System.err.println(
                    "Usage:\n" +
                            "java -jar transform.jar syntax [file1]\n" +
                            "  Generate a syntax that convers all json in [file1], one json document per line.\n" +
                            "\n" +
                            "java -jar transform.jar transform [syntax1file] [syntax2file] [file1] [file2]\n" +
                            "  Generate a diff script from syntax1 to syntax2 using values found in the streams\n" +
                            "  each element in order in file1 and file2 is expected to represent the \"the same\"" +
                            "  record, in the various format forms.\n" +
                            "java -jar transform.jar execute [scriptfile] [file]\n" +
                            "  Execute the given script on each json document in [file], one document per line.");
    }

    private static void generateAndPrintSyntax(String[] args) throws IOException
    {
        BufferedReader in = new BufferedReader(new FileReader(args[1]));

        String jsonString;
        Syntax syntax = new Syntax();
        while ( (jsonString = in.readLine()) != null)
        {
            Map data = mapper.readValue(jsonString, Map.class);
            Document doc = new Document(data);
            data = JsonLd.frame(doc.getCompleteId(), data);
            syntax.expandSyntaxToCover(data);
        }
        in.close();
        System.out.println(syntax);
    }

    private static void generateAndPrintTransform(String[] args) throws IOException, TransformScript.TransformSyntaxException
    {
        BufferedReader syntax1Reader = new BufferedReader(new FileReader(args[1]));
        BufferedReader syntax2Reader = new BufferedReader(new FileReader(args[2]));
        Syntax syntax1 = new Syntax(syntax1Reader);
        Syntax syntax2 = new Syntax(syntax2Reader);
        syntax1Reader.close();
        syntax2Reader.close();

        BufferedReader json1Reader = new BufferedReader(new FileReader(args[3]));
        BufferedReader json2Reader = new BufferedReader(new FileReader(args[4]));

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
