package transform;

import groovy.lang.Tuple2;
import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.triples.JsonldSerializer;
import whelk.util.TransformScript;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class Main
{
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException, TransformScript.TransformSyntaxException, SQLException
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
                            "java -jar transform.jar generate [fromSecret.properties] [toSecret.properties] [collection] [from|to]\n" +
                            "  Generate a diff script between two live environments. The first and second parameters \n" +
                            "  must be paths to secret.properties file for the [from] and [to] environments.\n" +
                            "  The third parameter should be either \"auth\", \"bib\", \"hold\" or \"definitions\"" +
                            "  the fourth parameter must be either \"from\" or \"to\". \"from\" means use all records in" +
                            "  the [from] env and find corresponding records in [to]. \"to\" means vice versa.\n" +
                            "  normally for a new release you want this: \n" +
                            "  java -jar transform.jar generate prodSecret.properties devSecret.properties [collection] to\n\n" +
                            "java -jar transform.jar execute [scriptfile] [file]\n" +
                            "  Execute the given script on each json document in [file], one document per line.\n\n");
    }

    private static void generateAndPrintTransform(String[] args) throws IOException, TransformScript.TransformSyntaxException, SQLException
    {
        if (args.length < 5)
        {
            System.err.println("Incorrect usage.");
            return;
        }
        String fromProperties = args[1];
        String toProperties = args[2];
        String collection = args[3];
        String toOrFrom = args[4];

        Whelk fromWhelk = loadWhelk(fromProperties);
        Whelk toWhelk = loadWhelk(toProperties);

        Whelk selectIDsFrom;
        if (toOrFrom.equals("to"))
            selectIDsFrom = toWhelk;
        else if (toOrFrom.equals("from"))
            selectIDsFrom = fromWhelk;
        else
        {
            System.err.println("fourth parameter must be [to|from]");
            return;
        }

        Syntax fromSyntax = new Syntax();
        Syntax toSyntax = new Syntax();

        // First pass over each record, to build the syntax(es).
        try (Connection connection = selectIDsFrom.getStorage().getConnection();
             PreparedStatement statement = getSelectIDsStatement(connection, collection);
             ResultSet resultSet = statement.executeQuery())
        {
            while (resultSet.next())
            {
                String id = resultSet.getString(1);
                Document fromDoc = fromWhelk.getStorage().load(id);
                Document toDoc = toWhelk.getStorage().load(id);

                if (fromDoc == null || toDoc == null)
                    continue;

                Map fromData = JsonLd.frame(fromDoc.getCompleteId(), fromDoc.data);
                Map toData = JsonLd.frame(toDoc.getCompleteId(), toDoc.data);
                fromSyntax.expandSyntaxToCover(fromData);
                toSyntax.expandSyntaxToCover(toData);
            }
        }

        // First pass over each record, to build the syntax(es).
        try (Connection connection = selectIDsFrom.getStorage().getConnection();
             PreparedStatement statement = getSelectIDsStatement(connection, collection);
             ResultSet resultSet = statement.executeQuery())
        {
            while (resultSet.next())
            {
                String id = resultSet.getString(1);
                Document fromDoc = fromWhelk.getStorage().load(id);
                Document toDoc = toWhelk.getStorage().load(id);

                if (fromDoc == null || toDoc == null)
                    continue;

                Map fromData = JsonLd.frame(fromDoc.getCompleteId(), fromDoc.data);
                Map toData = JsonLd.frame(toDoc.getCompleteId(), toDoc.data);
                fromSyntax.expandSyntaxToCover(fromData);
                toSyntax.expandSyntaxToCover(toData);
            }
        }

        // Second pass to trace values
        try (Connection connection = selectIDsFrom.getStorage().getConnection();
             PreparedStatement statement = getSelectIDsStatement(connection, collection);
             ResultSet resultSet = statement.executeQuery())
        {

            ScriptGenerator scriptGenerator = SyntaxDiffReduce.generateScript(fromSyntax, toSyntax, new RecordPairIterator(resultSet, fromWhelk, toWhelk));
            System.out.println(scriptGenerator);
        }


    }

    static class RecordPairIterator implements Iterator<Tuple2<String, String>>
    {
        private ResultSet m_idsResultSet;
        private Whelk m_fromWhelk;
        private Whelk m_toWhelk;
        private boolean m_done = false;

        public RecordPairIterator(ResultSet idsResultSet, Whelk fromWhelk, Whelk toWhelk)
        {
            m_idsResultSet = idsResultSet;
            m_fromWhelk = fromWhelk;
            m_toWhelk = toWhelk;
        }

        @Override
        public Tuple2<String, String> next()
        {
            try
            {
                Document fromDoc = null;
                Document toDoc = null;

                while (fromDoc == null || toDoc == null)
                {
                    if (!m_idsResultSet.next())
                    {
                        m_done = true;
                        return null;
                    }
                    String id = m_idsResultSet.getString(1);
                    fromDoc = m_fromWhelk.getStorage().load(id);
                    toDoc = m_toWhelk.getStorage().load(id);
                }

                return new Tuple2<>(fromDoc.getDataAsString(), toDoc.getDataAsString());
            } catch (SQLException e)
            {
                System.err.println(e.toString());
                return null;
            }
        }

        @Override
        public boolean hasNext()
        {
            return !m_done;
        }
    }

    private static PreparedStatement getSelectIDsStatement(Connection connection, String collection) throws SQLException
    {
        String sql = "SELECT id FROM lddb WHERE collection = ?";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, collection);
        return statement;
    }

    private static Whelk loadWhelk(String secrectPropertiesPath) throws IOException
    {
        InputStream propStream = new FileInputStream(new File(secrectPropertiesPath));
        Properties props = new Properties();
        props.load(propStream);
        return new Whelk(props);
    }

    /*
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
    }*/

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
