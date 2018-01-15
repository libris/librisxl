package whelk.util;

import com.google.common.collect.Lists;
import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.JsonLd;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

public class TransformScript
{
    private boolean m_modeFramed = false;
    private List<Operation> m_operations = new ArrayList<>();
    public static class TransformSyntaxException extends Exception
    {
        public TransformSyntaxException(String message)
        {
            super(message);
        }
    }

    private interface Operation
    {
        public void execute(Map json, Map<String, Object> context);
    }

    /*******************************************************************************************************************
     * Parsing
     ******************************************************************************************************************/

    public TransformScript(String scriptText) throws TransformSyntaxException
    {
        LinkedList<String> symbolList = new LinkedList<>();

        boolean buildingQuotedSymbol = false;
        StringBuilder symbol = new StringBuilder();
        int i = 0;
        while (i < scriptText.length())
        {
            if (buildingQuotedSymbol)
            {
                char c = scriptText.charAt(i++);
                while (c != '\"' && i < scriptText.length())
                {
                    symbol.append(c);
                    c = scriptText.charAt(i++);
                }
                symbolList.add(symbol.toString());
                symbol = new StringBuilder();
                buildingQuotedSymbol = false;
                ++i;
            } else {
                char c = scriptText.charAt(i);
                if (c == '#') // line comment, skip until "\n"
                {
                    while (c != '\n' && i < scriptText.length())
                        c = scriptText.charAt(i++);
                }
                else if (c == '\"')
                {
                    buildingQuotedSymbol = true;
                    ++i;
                }
                else if (!Character.isWhitespace(c))
                {
                    symbol.append(c);
                    ++i;
                }
                else // whitespace
                {
                    if (symbol.length() > 0)
                        symbolList.add(symbol.toString());
                    symbol = new StringBuilder();
                    while(Character.isWhitespace(c) && i < scriptText.length()) // end of symbol skip until next non-whitespace
                    {
                        ++i;
                        if (i < scriptText.length())
                            c = scriptText.charAt(i);
                    }
                }
            }
        }
        if (symbol.length() > 0)
            symbolList.add(symbol.toString());

        if (buildingQuotedSymbol)
            throw new TransformSyntaxException("Mismatched quotes.");

        parseScript(symbolList);
    }

    private void parseScript(LinkedList<String> symbols) throws TransformSyntaxException
    {
        if (symbols.size() < 2)
            throw new TransformSyntaxException("The script did not begin with MODE [FRAMED/NORMAL] (required).");

        String symbol = symbols.pollFirst();
        if (!symbol.equalsIgnoreCase("MODE"))
            throw new TransformSyntaxException("The script did not begin with MODE [FRAMED/NORMAL] (required).");

        symbol = symbols.pollFirst();
        if (!symbol.equalsIgnoreCase("FRAMED") && !symbol.equalsIgnoreCase("NORMAL"))
            throw new TransformSyntaxException("The script did not begin with MODE [FRAMED/NORMAL] (required).");

        if (symbol.equalsIgnoreCase("FRAMED"))
            m_modeFramed = true;

        m_operations = parseStatementList(symbols);
    }

    private List<Operation> parseStatementList(LinkedList<String> symbols) throws TransformSyntaxException
    {
        List<Operation> operations = new ArrayList<>();

        while(!symbols.isEmpty())
        {
            String symbol = symbols.pollFirst();

            if (symbol == null)
                throw new TransformSyntaxException("Unexpected end of script.");

            switch (symbol) {
                case "MOVE":
                case "move":
                    operations.add( parseMoveStatement(symbols) );
                    break;
                case "FOREACH":
                case "foreach":
                    operations.add( parseForEachStatement(symbols) );
                    break;
                case "SET":
                case "set":
                    operations.add( parseSetStatement(symbols) );
                    break;
                case "DELETE":
                case "delete":
                    operations.add( parseDeleteStatement(symbols) );
                    break;
                case "{":
                    operations.addAll( parseStatementList(symbols) );
                    break;
                case "}":
                    return operations;
                default:
                    throw new TransformSyntaxException("Unexpected symbol: \"" + symbol + "\"");
            }
        }

        // End of script
        return operations;
    }

    private MoveOperation parseMoveStatement(LinkedList<String> symbols) throws TransformSyntaxException
    {
        if (symbols.size() < 3)
            throw new TransformSyntaxException("'MOVE' must be followed by [pathFrom '->' pathTo]");

        String from = symbols.pollFirst();
        String arrow = symbols.pollFirst();
        String to = symbols.pollFirst();
        if (!arrow.equals("->") || !isValidPath(from) || !isValidPath(to))
            throw new TransformSyntaxException("'MOVE' must be followed by [pathFrom '->' pathTo]");

        return new MoveOperation(from, to);
    }

    private SetOperation parseSetStatement(LinkedList<String> symbols) throws TransformSyntaxException
    {
        if (symbols.size() < 3)
            throw new TransformSyntaxException("'SET' must be followed by [_LITERAL '->' pathTo]");

        String literal = symbols.pollFirst();
        String arrow = symbols.pollFirst();
        String to = symbols.pollFirst();
        if (!arrow.equals("->") || !isValidPath(to))
            throw new TransformSyntaxException("'SET' must be followed by [_LITERAL '->' pathTo]");

        return new SetOperation(literal, to);
    }

    private DeletedOperation parseDeleteStatement(LinkedList<String> symbols) throws TransformSyntaxException
    {
        if (symbols.size() < 1)
            throw new TransformSyntaxException("'DELETE' must be followed by [path]");

        String path = symbols.pollFirst();
        if (!isValidPath(path))
            throw new TransformSyntaxException("'DELETE' must be followed by [path]");

        return new DeletedOperation(path);
    }

    private ForEachOperation parseForEachStatement(LinkedList<String> symbols) throws TransformSyntaxException
    {
        if (symbols.size() < 4)
            throw new TransformSyntaxException("'FOREACH' must be followed by [identifier ':' pathToList STATEMENT]");

        String iteratorSymbol = symbols.pollFirst();
        String colon = symbols.pollFirst();
        String path = symbols.pollFirst();
        if ( iteratorSymbol == null || Character.isDigit(iteratorSymbol.charAt(0)))
            throw new TransformSyntaxException("Expected non-numerical iterator symbol.");
        if (path == null || !isValidPath(path))
            throw new TransformSyntaxException("'FOREACH' must be followed by [identifier ':' pathToList STATEMENT]");
        if (colon == null || !colon.equals(":"))
            throw new TransformSyntaxException("'FOREACH' must be followed by [identifier ':' pathToList STATEMENT]");

        List<Operation> operations = parseStatementList(symbols);
        return new ForEachOperation(path, iteratorSymbol, operations);
    }

    private boolean isValidPath(String symbol)
    {
        return !symbol.contains("{") && !symbol.contains("}");
    }

    /*******************************************************************************************************************
     * Execution
     ******************************************************************************************************************/

    private class MoveOperation implements Operation
    {
        private String m_fromPath;
        private String m_toPath;

        public MoveOperation(String fromPath, String toPath)
        {
            m_fromPath = fromPath;
            m_toPath = toPath;
        }

        public void execute(Map json, Map<String, Object> context)
        {
            List<Object> fromPath = Arrays.asList( withIntAsInteger(m_fromPath.split(",")) );
            List<Object> fromPathWithSymbols = insertContextSymbolsIntoPath(fromPath, context);

            List<Object> toPath = Arrays.asList( withIntAsInteger(m_toPath.split(",")) );
            List<Object> toPathWithSymbols = insertContextSymbolsIntoPath(toPath, context);

            Object value = Document._get(fromPathWithSymbols, json);

            if (value == null)
                return;

            Type containerType;
            if (toPathWithSymbols.get(toPathWithSymbols.size()-1) instanceof String)
                containerType = HashMap.class;
            else
                containerType = ArrayList.class;

            Document._removeLeafObject(fromPathWithSymbols, json);
            Document._set(toPathWithSymbols, value, containerType, json);
            pruneBranch(fromPathWithSymbols, json);
        }
    }

    private class SetOperation implements Operation
    {
        private String m_value;
        private String m_toPath;

        public SetOperation(String value, String toPath)
        {
            m_value = value;
            m_toPath = toPath;
        }

        public void execute(Map json, Map<String, Object> context)
        {
            List<Object> toPath = Arrays.asList( withIntAsInteger(m_toPath.split(",")) );
            List<Object> toPathWithSymbols = insertContextSymbolsIntoPath(toPath, context);

            Type containerType;
            if (toPathWithSymbols.get(toPathWithSymbols.size()-1) instanceof String)
                containerType = HashMap.class;
            else
                containerType = ArrayList.class;

            Document._set(toPathWithSymbols, m_value, containerType, json);
        }
    }

    private class DeletedOperation implements Operation
    {
        private String m_path;

        public DeletedOperation(String path)
        {
            m_path = path;
        }

        public void execute(Map json, Map<String, Object> context)
        {
            List<Object> path = Arrays.asList( withIntAsInteger(m_path.split(",")) );
            List<Object> pathWithSymbols = insertContextSymbolsIntoPath(path, context);

            Document._removeLeafObject(pathWithSymbols, json);
            pruneBranch(pathWithSymbols, json);
        }
    }

    private class ForEachOperation implements Operation
    {
        private String m_listPath;
        private List<Operation> m_operations;
        private String m_iteratorSymbol;

        public ForEachOperation(String listPath, String iteratorSymbol, List<Operation> operations)
        {
            m_listPath = listPath;
            m_operations = operations;
            m_iteratorSymbol = iteratorSymbol;
        }

        public void execute(Map json, Map<String, Object> context)
        {
            List<Object> path = Arrays.asList( withIntAsInteger(m_listPath.split(",")) );
            List<Object> pathWithSymbols = insertContextSymbolsIntoPath(path, context);

            Object listObject = Document._get(pathWithSymbols, json);
            if (listObject instanceof List)
            {
                List list = (List) listObject;

                for (int i = list.size()-1; i > -1; --i) // For each iterator-index (in reverse) do:
                {
                    for (Operation op : m_operations)
                    {
                        Map<String, Object> nextContext = new HashMap<>();
                        nextContext.putAll(context); // inherit scope
                        nextContext.put(m_iteratorSymbol, i);
                        op.execute(json, nextContext);
                    }
                }
            }
        }
    }

    private Object[] withIntAsInteger(String[] pathArray)
    {
        Object[] result = new Object[pathArray.length];

        for (int i = 0; i < pathArray.length; ++i)
        {
            if (pathArray[i].matches("^\\d+$")) // if its a number
                result[i] = new Integer(pathArray[i]);
            else
                result[i] = pathArray[i];
        }

        return result;
    }

    private List<Object> insertContextSymbolsIntoPath(List<Object> path, Map<String, Object> context)
    {
        List<Object> resultingPath = new ArrayList<>();
        for (int i = 0; i < path.size(); ++i)
        {
            Object step = path.get(i);
            if (context.containsKey(step))
                resultingPath.add(context.get(step));
            else
                resultingPath.add(step);
        }
        return resultingPath;
    }

    /**
     * Delete data structure only where it is empty
     */
    private void pruneBranch(List<Object> path, Map json)
    {
        for (int i = 0; i < path.size(); ++i)
        {
            List<Object> subPath = path.subList(0, path.size()-i);
            Object container = Document._get( subPath, json );
            if (container instanceof List)
            {
                if (((List) container).isEmpty())
                    Document._removeLeafObject(subPath, json);
            } else if (container instanceof Map)
            {
                if (((Map) container).isEmpty())
                    Document._removeLeafObject(subPath, json);
            }
        }
    }

    public String executeOn(String json) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        Map data = mapper.readValue(json, Map.class);
        Document doc = new Document(data);
        if (m_modeFramed)
            data = JsonLd.frame(doc.getShortId(), data);

        for (Operation op : m_operations)
            op.execute(data, new HashMap<>());
        return mapper.writeValueAsString(data);
    }

    public Map executeOn(Map data) throws IOException
    {
        Document doc = new Document(data);
        if (m_modeFramed)
            data = JsonLd.frame(doc.getShortId(), data);

        for (Operation op : m_operations)
            op.execute(data, new HashMap<>());
        return data;
    }
}
