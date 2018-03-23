package whelk.util;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.JsonLd;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

public class TransformScript
{
    private boolean m_modeFramed = false;
    private StatementListOperation m_rootStatement;
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
        int reservedSymbolLength = 0;
        while (i < scriptText.length())
        {
            if (buildingQuotedSymbol)
            {
                Character c = scriptText.charAt(i++);
                while (c != '\"' && i < scriptText.length())
                {
                    symbol.append(c);
                    c = scriptText.charAt(i++);
                }
                symbolList.add(symbol.toString());
                symbol = new StringBuilder();
                buildingQuotedSymbol = false;
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
                else if ( (reservedSymbolLength = isReserverdOperator(i, scriptText)) != 0 )
                {
                    addToSymbolList(symbol, symbolList);

                    symbolList.add( scriptText.substring(i, i+reservedSymbolLength) );
                    i += reservedSymbolLength;
                    while(i < scriptText.length() && Character.isWhitespace(scriptText.charAt(i))) // skip until next non-whitespace
                        ++i;
                }
                else if (!Character.isWhitespace(c))
                {
                    symbol.append(c);
                    ++i;
                }
                else // whitespace
                {
                    addToSymbolList(symbol, symbolList);
                    while(i < scriptText.length() && Character.isWhitespace(scriptText.charAt(i))) // skip until next non-whitespace
                        ++i;
                }
            }
        }
        if (symbol.length() > 0)
            symbolList.add(symbol.toString());

        if (buildingQuotedSymbol)
            throw new TransformSyntaxException("Mismatched quotes.");

        //System.out.println(symbolList);

        parseScript(symbolList);
    }

    private void addToSymbolList(StringBuilder currentSymbol, LinkedList<String> symbolList)
    {
        if (currentSymbol.length() > 0)
            symbolList.add(currentSymbol.toString());
        currentSymbol.setLength(0);
    }

    private int isReserverdOperator(int i, String scriptText)
    {
        String[] reserverdOperators = {
                "==", "!=", "->", ">=", "<=", "&&", "||", "!", "<", ">", "(", ")", "{", "}", "*", "+", "-", "/", "="};
        for (int j = 0; j < reserverdOperators.length; ++j)
        {
            if (scriptText.length() < i + reserverdOperators[j].length())
                return 0;
            if (scriptText.substring(i, i+reserverdOperators[j].length()).equals(reserverdOperators[j]))
                return reserverdOperators[j].length();
        }
        return 0;
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

        //m_operations = parseStatementList(symbols);
        m_rootStatement = parseStatementList(symbols);
    }

    private Operation parseStatement(LinkedList<String> symbols) throws TransformSyntaxException
    {
        String symbol = symbols.pollFirst();

        if (symbol == null)
            throw new TransformSyntaxException("Unexpected end of script.");

        switch (symbol) {
            case "MOVE":
            case "move":
                return parseMoveStatement(symbols);
            case "FOR":
            case "for":
                return parseForEachStatement(symbols);
            case "IF":
            case "if":
                return parseIfStatement(symbols);
            case "SET":
            case "set":
                return parseSetStatement(symbols);
            case "LET":
            case "let":
                return parseLetStatement(symbols);
            case "DELETE":
            case "delete":
                return parseDeleteStatement(symbols);
            case "{":
                return parseStatementList(symbols);
            default:
                throw new TransformSyntaxException("Unexpected symbol: \"" + symbol + "\"");
        }
    }

    private StatementListOperation parseStatementList(LinkedList<String> symbols) throws TransformSyntaxException
    {
        List<Operation> operations = new ArrayList<>();

        while(!symbols.isEmpty())
        {
            String symbol = symbols.peekFirst();
            if (symbol.equals("}"))
            {
                symbols.pollFirst();
                return new StatementListOperation(operations);
            }
            operations.add( parseStatement(symbols) );
        }

        // End of script
        return new StatementListOperation(operations);
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

        HashSet<String> binaryOps = new HashSet<>();
        binaryOps.add("+");
        binaryOps.add("-");
        binaryOps.add("*");
        binaryOps.add("/");
        binaryOps.add("==");
        binaryOps.add("!=");
        binaryOps.add("<");
        binaryOps.add(">");
        binaryOps.add("<=");
        binaryOps.add(">=");
        binaryOps.add("&&");
        binaryOps.add("||");
        binaryOps.add("!");

        if (next != null && binaryOps.contains(next))
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
            ValueOperation subOp = parseValueStatement(symbols);
            String closingPar = symbols.pollFirst();
            if (!closingPar.equals(")"))
                throw new TransformSyntaxException("Mismatched parenthesis");
            return subOp;
        } else if (symbol.equals("*"))
        {
            return new DerefValueOperation(symbols.pollFirst());
        } else if (symbol.equals("!"))
        {
            return new NotValueOperation(parseValueStatement(symbols));
        } else if (symbol.equals("sizeof"))
        {
            return new SizeofValueOperation(parseUnaryValueStatement(symbols));
        } else if (symbol.equals("substring"))
        {
            ValueOperation stringParameter = parseValueStatement(symbols);
            ValueOperation startParameter = parseValueStatement(symbols);
            ValueOperation endParameter = parseValueStatement(symbols);
            return new SubStringValueOperation(stringParameter, startParameter, endParameter);
        } else if (symbol.equals("startswith"))
        {
            ValueOperation completeStringParameter = parseValueStatement(symbols);
            ValueOperation searchStringParameter = parseValueStatement(symbols);
            return new StringStartsWithValueOperation(completeStringParameter, searchStringParameter);
        } else if (symbol.equals("endswith"))
        {
            ValueOperation completeStringParameter = parseValueStatement(symbols);
            ValueOperation searchStringParameter = parseValueStatement(symbols);
            return new StringEndsWithValueOperation(completeStringParameter, searchStringParameter);
        } else if (symbol.equals("contains"))
        {
            ValueOperation completeStringParameter = parseValueStatement(symbols);
            ValueOperation searchStringParameter = parseValueStatement(symbols);
            return new StringContainsValueOperation(completeStringParameter, searchStringParameter);
        } else if (symbol.equals("indexof"))
        {
            ValueOperation completeStringParameter = parseValueStatement(symbols);
            ValueOperation searchStringParameter = parseValueStatement(symbols);
            return new StringIndexOfValueOperation(completeStringParameter, searchStringParameter);
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

        Operation operations = parseStatement(symbols);
        return new ForEachOperation(path, iteratorSymbol, operations);
    }

    private IfOperation parseIfStatement(LinkedList<String> symbols) throws TransformSyntaxException
    {
        if (symbols.size() < 3)
            throw new TransformSyntaxException("'IF' must be followed by a boolean value or expression.");
        ValueOperation value = parseValueStatement(symbols);
        Operation operations = parseStatement(symbols);
        return new IfOperation(value, operations);
    }

    private boolean isValidPath(String symbol)
    {
        return !symbol.contains("{") && !symbol.contains("}");
    }

    /*******************************************************************************************************************
     * Execution
     ******************************************************************************************************************/

    private class StatementListOperation implements Operation
    {
        private List<Operation> m_operations = new ArrayList<>();

        public StatementListOperation(List<Operation> operations)
        {
            m_operations = operations;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            Map<String, Object> nextContext = new HashMap<>();
            nextContext.putAll(context); // inherit scope
            for (Operation operation : m_operations)
            {
                operation.execute(json, nextContext);
            }
            return null;
        }
    }

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
            if (m_value instanceof String && ((String) m_value).equalsIgnoreCase("true") )
                return true;
            if (m_value instanceof String && ((String) m_value).equalsIgnoreCase("false") )
                return false;
            if (m_value instanceof String && ((String) m_value).equalsIgnoreCase("null") )
                return null;
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

    private class NotValueOperation extends ValueOperation
    {
        ValueOperation m_valueOperation;

        public NotValueOperation(ValueOperation valueOperation)
        {
            m_valueOperation = valueOperation;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            Object value = m_valueOperation.execute(json, context);
            if ( ! (value instanceof Boolean) )
                throw new RuntimeException("Type mismatch. Cannot combine logic not (!) with non boolean value: " + value);
            return !((Boolean)value);
        }
    }

    private class SizeofValueOperation extends ValueOperation
    {
        ValueOperation m_value;

        public SizeofValueOperation(ValueOperation value)
        {
            m_value = value;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            Object value = m_value.execute(json, context);

            if (value instanceof List)
                return ((List) value).size();
            else if (value instanceof Map)
                return ((Map) value).keySet().size();
            else if (value instanceof String)
                return ((String) value).length();
            return 0;
        }
    }

    private class SubStringValueOperation extends ValueOperation
    {
        ValueOperation m_completeString;
        ValueOperation m_startIndex;
        ValueOperation m_endIndex;

        public SubStringValueOperation(ValueOperation completeString, ValueOperation startIndex, ValueOperation endIndex)
        {
            m_completeString = completeString;
            m_startIndex = startIndex;
            m_endIndex = endIndex;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            Object string = m_completeString.execute(json, context);
            Object startIndex = m_startIndex.execute(json, context);
            Object endIndex = m_endIndex.execute(json, context);

            if (!(string instanceof String))
                throw new RuntimeException("Type mismatch. Cannot call substring on non-string: " + string);
            if (!(startIndex instanceof Integer) || !(endIndex instanceof Integer))
                throw new RuntimeException("Type mismatch. Both expressions following substring must evaluate to integers.");

            return ((String) string).substring( (Integer) startIndex, (Integer) endIndex);
        }
    }

    private class StringStartsWithValueOperation extends ValueOperation
    {
        ValueOperation m_completeString;
        ValueOperation m_searchString;

        public StringStartsWithValueOperation(ValueOperation completeString, ValueOperation searchString)
        {
            m_completeString = completeString;
            m_searchString = searchString;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            Object completeString = m_completeString.execute(json, context);
            Object searchString = m_searchString.execute(json, context);

            if (!(searchString instanceof String) || !(completeString instanceof String))
                throw new RuntimeException("Type mismatch. Cannot call startsWith on non-strings");

            return ((String) completeString).startsWith( (String) searchString );
        }
    }

    private class StringEndsWithValueOperation extends ValueOperation
    {
        ValueOperation m_completeString;
        ValueOperation m_searchString;

        public StringEndsWithValueOperation(ValueOperation completeString, ValueOperation searchString)
        {
            m_completeString = completeString;
            m_searchString = searchString;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            Object completeString = m_completeString.execute(json, context);
            Object searchString = m_searchString.execute(json, context);

            if (!(searchString instanceof String) || !(completeString instanceof String))
                throw new RuntimeException("Type mismatch. Cannot call endsWith on non-strings");

            return ((String) completeString).endsWith( (String) searchString );
        }
    }

    private class StringContainsValueOperation extends ValueOperation
    {
        ValueOperation m_completeString;
        ValueOperation m_searchString;

        public StringContainsValueOperation(ValueOperation completeString, ValueOperation searchString)
        {
            m_completeString = completeString;
            m_searchString = searchString;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            Object completeString = m_completeString.execute(json, context);
            Object searchString = m_searchString.execute(json, context);

            if (!(searchString instanceof String) || !(completeString instanceof String))
                throw new RuntimeException("Type mismatch. Cannot call contains on non-strings");

            return ((String) completeString).contains( (String) searchString );
        }
    }

    private class StringIndexOfValueOperation extends ValueOperation
    {
        ValueOperation m_completeString;
        ValueOperation m_searchString;

        public StringIndexOfValueOperation(ValueOperation completeString, ValueOperation searchString)
        {
            m_completeString = completeString;
            m_searchString = searchString;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            Object completeString = m_completeString.execute(json, context);
            Object searchString = m_searchString.execute(json, context);

            if (!(searchString instanceof String) || !(completeString instanceof String))
                throw new RuntimeException("Type mismatch. Cannot call indexOf on non-strings");

            return ((String) completeString).indexOf( (String) searchString );
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
                else if (m_operator.equals("=="))
                    return concreteLeftValue.equals(concreteRightValue); // String comparison
                else
                    throw new RuntimeException("Type mismatch. Cannot combine " + concreteLeftValue + " with " + concreteRightValue + " using " + m_operator);
            }

            if (concreteLeftValue instanceof Boolean || concreteRightValue instanceof Boolean)
            {
                if (!(concreteLeftValue instanceof Boolean) || !(concreteRightValue instanceof Boolean))
                    throw new RuntimeException("Type mismatch. Cannot combine booleans with other types");

                else if (m_operator.equals("=="))
                    return concreteLeftValue.equals(concreteRightValue); // Boolean to boolean comparison
                else if (m_operator.equals("&&"))
                    return ((Boolean) concreteLeftValue) && ((Boolean) concreteRightValue);
                else if (m_operator.equals("||"))
                    return ((Boolean) concreteLeftValue) || ((Boolean) concreteRightValue);

                throw new RuntimeException("Type mismatch. Cannot combine " + concreteLeftValue + " with " + concreteRightValue + " using " + m_operator);
            }

            if (concreteLeftValue == null)
            {
                if (concreteRightValue == null && m_operator.equals("=="))
                    return true;

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
                case "==":
                    return left == right;
                case "!=":
                    return left != right;
                case "<":
                    return left < right;
                case ">":
                    return left > right;
                case "<=":
                    return left <= right;
                case ">=":
                    return left >= right;
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
        private Operation m_operations;
        private String m_iteratorSymbol;

        public ForEachOperation(String listPath, String iteratorSymbol, Operation operations)
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
                    Map<String, Object> nextContext = new HashMap<>();
                    nextContext.putAll(context); // inherit scope
                    nextContext.put(m_iteratorSymbol, i);
                    m_operations.execute(json, nextContext);
                }
            }
            return null;
        }
    }

    private class IfOperation implements Operation
    {
        private Operation m_operations;
        private ValueOperation m_booleanValue;

        public IfOperation(ValueOperation booleanValue, Operation operations)
        {
            m_operations = operations;
            m_booleanValue = booleanValue;
        }

        public Object execute(Map json, Map<String, Object> context)
        {
            if ( (Boolean) m_booleanValue.execute(json, context) )
                m_operations.execute(json, context);
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
        m_rootStatement.execute(data, context);
        return mapper.writeValueAsString(data);
    }

    public Map executeOn(Map data)
    {
        Document doc = new Document(data);
        if (m_modeFramed)
            data = JsonLd.frame(doc.getShortId(), data);

        HashMap<String, Object> context = new HashMap<>();
        m_rootStatement.execute(data, context);
        return data;
    }
}
