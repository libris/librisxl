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
        public Object execute(Map json, Map<String, Object> context);
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
                case "LET":
                case "let":
                    operations.add( parseLetStatement(symbols) );
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
            throw new TransformSyntaxException("'SET' must be followed by [value_statement '->' pathTo]");

        ValueOperation valueOp = parseValueStatement(symbols);
        String arrow = symbols.pollFirst();
        String to = symbols.pollFirst();
        if (!arrow.equals("->") || !isValidPath(to))
            throw new TransformSyntaxException("'SET' must be followed by [value_statement '->' pathTo]");

        return new SetOperation(valueOp, to);
    }

    private LetOperation parseLetStatement(LinkedList<String> symbols) throws TransformSyntaxException
    {
        if (symbols.size() < 3)
            throw new TransformSyntaxException("'LET' must be followed by [name '=' value_statement]");

        String name = symbols.pollFirst();
        String eqSign = symbols.pollFirst();

        if (!eqSign.equals("="))
            throw new TransformSyntaxException("'LET' must be followed by [name '=' value_statement]");

        return new LetOperation(name, parseValueStatement(symbols));
    }

    private ValueOperation parseValueStatement(LinkedList<String> symbols) throws TransformSyntaxException
    {
        ValueOperation leftOperand = parseUnaryValueStatement(symbols);
        String next = symbols.peekFirst();
        String arithmeticOps = "+-/*";
        if (next != null && arithmeticOps.contains(next))
        {
            String binaryOperator = symbols.pollFirst();
            ValueOperation rightOperand = parseValueStatement(symbols);
            return new BinaryValueOperation(leftOperand, rightOperand, binaryOperator);
        }
        else
            return leftOperand;
    }

    private ValueOperation parseUnaryValueStatement(LinkedList<String> symbols) throws TransformSyntaxException
    {
        if (symbols.size() < 1)
            throw new TransformSyntaxException("A value_statement must consist of either a literal value, or a composite of value_statements");

        String symbol = symbols.pollFirst();

        if (symbol.equals("("))
        {
            ValueOperation subOp = new ValueOperation(parseValueStatement(symbols));
            String closingPar = symbols.pollFirst();
            if (!closingPar.equals(")"))
                throw new TransformSyntaxException("Mismatched parenthesis");
            return subOp;
        } else if (symbol.equals("*"))
        {
            return new DerefValueOperation(symbols.pollFirst());
        } else if (symbol.equals("sizeof"))
        {
            return new SizeofValueOperation(symbols.pollFirst());
        } else
        {
            return new LiteralValueOperation(symbol);
        }
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

        public Object execute(Map json, Map<String, Object> context)
        {
            List<Object> fromPath = Arrays.asList( withIntAsInteger(m_fromPath.split(",")) );
            List<Object> fromPathWithSymbols = insertContextSymbolsIntoPath(fromPath, context);

            List<Object> toPath = Arrays.asList( withIntAsInteger(m_toPath.split(",")) );
            List<Object> toPathWithSymbols = insertContextSymbolsIntoPath(toPath, context);

            Object value = Document._get(fromPathWithSymbols, json);

            if (value == null)
                return null;

            Type containerType;
            if (toPathWithSymbols.get(toPathWithSymbols.size()-1) instanceof String)
                containerType = HashMap.class;
            else
                containerType = ArrayList.class;

            Document._removeLeafObject(fromPathWithSymbols, json);
            Document._set(toPathWithSymbols, value, containerType, json);
            pruneBranch(fromPathWithSymbols, json);
            return null;
        }
    }

    private class SetOperation implements Operation
    {
        private ValueOperation m_value;
        private String m_toPath;

        public SetOperation(ValueOperation value, String toPath)
        {
            m_value = value;
            m_toPath = toPath;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            List<Object> toPath = Arrays.asList( withIntAsInteger(m_toPath.split(",")) );
            List<Object> toPathWithSymbols = insertContextSymbolsIntoPath(toPath, context);

            Type containerType;
            if (toPathWithSymbols.get(toPathWithSymbols.size()-1) instanceof String)
                containerType = HashMap.class;
            else
                containerType = ArrayList.class;

            Document._set(toPathWithSymbols, m_value.execute(json, context), containerType, json);
            return null;
        }
    }

    private class LetOperation implements Operation
    {
        private Operation m_valueOp;
        private String m_name;
        public LetOperation(String name, Operation valueOperation)
        {
            m_valueOp = valueOperation;
            m_name = name;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            context.put(m_name, m_valueOp.execute(json, context));
            return null;
        }
    }

    private class ValueOperation implements Operation
    {
        private ValueOperation m_subOp;
        ValueOperation() {}
        public ValueOperation(ValueOperation subOperation)
        {
            m_subOp = subOperation;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            return m_subOp.execute(json, context);
        }
    }

    private class LiteralValueOperation extends ValueOperation
    {
        Object m_value;

        public LiteralValueOperation(Object value)
        {
            m_value = value;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            // The value might be a variable name
            if (m_value instanceof String && context.containsKey(m_value))
                return context.get(m_value);
            if (m_value instanceof String && ((String) m_value).matches("-?\\d+"))
                return Integer.parseInt((String)m_value);
            return m_value;
        }
    }

    private class DerefValueOperation extends ValueOperation
    {
        String m_path;

        public DerefValueOperation(String path)
        {
            m_path = path;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            List<Object> path = Arrays.asList( withIntAsInteger(m_path.split(",")) );
            List<Object> pathWithSymbols = insertContextSymbolsIntoPath(path, context);

            return Document._get(pathWithSymbols, json);
        }
    }

    private class SizeofValueOperation extends ValueOperation
    {
        String m_path;

        public SizeofValueOperation(String path)
        {
            m_path = path;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            List<Object> path = Arrays.asList( withIntAsInteger(m_path.split(",")) );
            List<Object> pathWithSymbols = insertContextSymbolsIntoPath(path, context);

            Object atPath = Document._get(pathWithSymbols, json);
            if (atPath instanceof List)
                return ((List) atPath).size();
            else if (atPath instanceof Map)
                return ((Map) atPath).keySet().size();
            else if (atPath instanceof String)
                return ((String) atPath).length();
            return 0;
        }
    }

    private class BinaryValueOperation extends ValueOperation
    {
        private ValueOperation m_leftOperand;
        private ValueOperation m_rightOperand;
        private String m_operator;

        public BinaryValueOperation(ValueOperation leftOperand, ValueOperation rightOperand, String operator)
        {
            m_leftOperand = leftOperand;
            m_rightOperand = rightOperand;
            m_operator = operator;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            Object concreteLeftValue = m_leftOperand.execute(json, context);
            Object concreteRightValue = m_rightOperand.execute(json, context);

            if (concreteLeftValue instanceof String || concreteRightValue instanceof String)
            {
                if (m_operator.equals("+"))
                    return "" + concreteLeftValue + concreteRightValue; // String concatenation
                else
                    throw new RuntimeException("Type mismatch. Cannot combine " + concreteLeftValue + " with " + concreteRightValue + " using " + m_operator);
            }

            // Both values must now be integers
            int left = ((Integer) concreteLeftValue);
            int right = ((Integer) concreteRightValue);

            switch (m_operator)
            {
                case "+":
                    return left + right;
                case "-":
                    return left - right;
                case "*":
                    return left * right;
                case "/":
                    return left / right;
            }
            return null;
        }
    }

    private class DeletedOperation implements Operation
    {
        private String m_path;

        public DeletedOperation(String path)
        {
            m_path = path;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            List<Object> path = Arrays.asList( withIntAsInteger(m_path.split(",")) );
            List<Object> pathWithSymbols = insertContextSymbolsIntoPath(path, context);

            Document._removeLeafObject(pathWithSymbols, json);
            pruneBranch(pathWithSymbols, json);
            return null;
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

        public Object execute(Map json, Map<String, Object> context)
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
            return null;
        }
    }

    /*******************************************************************************************************************
     * Helpers
     ******************************************************************************************************************/

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

        HashMap<String, Object> context = new HashMap<>();

        for (Operation op : m_operations)
            op.execute(data, context);
        return mapper.writeValueAsString(data);
    }

    public Map executeOn(Map data) throws IOException
    {
        Document doc = new Document(data);
        if (m_modeFramed)
            data = JsonLd.frame(doc.getShortId(), data);

        HashMap<String, Object> context = new HashMap<>();

        for (Operation op : m_operations)
            op.execute(data, context);
        return data;
    }
}
