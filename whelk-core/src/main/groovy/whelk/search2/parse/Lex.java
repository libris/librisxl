package whelk.search2.parse;

import whelk.exception.InvalidQueryException;

import java.util.*;

public class Lex {
    public static class MutableInteger {
        public MutableInteger(int i) {
            value = i;
        }

        public void increase(int with) {
            value += with;
        }

        int value;
    }

    public enum TokenName {
        OPERATOR,
        KEYWORD,
        STRING,
        QUOTED_STRING
    }

    public record Symbol(TokenName name, String value, int offset) {
    }

    public static LinkedList<Symbol> lexQuery(String queryString) throws InvalidQueryException {
        LinkedList<Symbol> symbols = new LinkedList<>();
        StringBuilder query = new StringBuilder(queryString);
        MutableInteger offset = new MutableInteger(0);

        for (Symbol symbol = getNextSymbol(query, offset); symbol != null; symbol = getNextSymbol(query, offset))
            symbols.add(symbol);

        return symbols;
    }

    private static void consumeWhiteSpace(StringBuilder query, MutableInteger offset) {
        while (!query.isEmpty() && Character.isWhitespace(query.charAt(0))) {
            query.deleteCharAt(0);
            offset.increase(1);
        }
    }

    private static final List<Character> reservedCharsInString = Arrays.asList('!', '<', '>', '=', '~', '(', ')', ':');

    private static Symbol getNextSymbol(StringBuilder query, MutableInteger offset) throws InvalidQueryException {
        consumeWhiteSpace(query, offset);
        if (query.isEmpty())
            return null;

        int symbolOffset = offset.value;

        // Special multi-char symbols that need not be whitespace separated:
        if (query.length() >= 2) {
            if (query.substring(0, 2).equals(">=")) {
                query.deleteCharAt(0);
                query.deleteCharAt(0);
                offset.increase(2);
                return new Symbol(TokenName.OPERATOR, ">=", symbolOffset);
            }
            if (query.substring(0, 2).equals("<=")) {
                query.deleteCharAt(0);
                query.deleteCharAt(0);
                offset.increase(2);
                return new Symbol(TokenName.OPERATOR, "<=", symbolOffset);
            }
        }

        // quoted strings
        if (query.charAt(0) == '"') {
            query.deleteCharAt(0);
            offset.increase(1);

            StringBuilder symbolValue = new StringBuilder();
            while (true) {
                if (query.isEmpty())
                    throw new InvalidQueryException("Lexer error: Unclosed double quote, started at character index: " + symbolOffset);
                char c = query.charAt(0);
                query.deleteCharAt(0);
                offset.increase(1);
                if (c == '"')
                    return new Symbol(TokenName.QUOTED_STRING, symbolValue.toString(), symbolOffset);
                else if (c == '\\') { // char escaping ...
                    char escapedC = query.charAt(0);
                    query.deleteCharAt(0);
                    offset.increase(1);
                    if (query.isEmpty())
                        throw new InvalidQueryException("Lexer error: Escaped EOF at character index: " + symbolOffset);
                    symbolValue.append(escapedC);
                } else {
                    symbolValue.append(c);
                }
            }
        }

        // whitespace separated strings
        {
            StringBuilder symbolValue = new StringBuilder();
            while (true) {
                char c = query.charAt(0);
                if (c == '"')
                    throw new InvalidQueryException("Lexer error: Double quote illegal at character index: " + offset.value);
                if (reservedCharsInString.contains(c)) {
                    if (symbolValue.isEmpty()) {
                        symbolValue.append(c);
                        query.deleteCharAt(0);
                        offset.increase(1);
                        return new Symbol(TokenName.OPERATOR, symbolValue.toString(), symbolOffset);
                    }
                    break;
                } else if (c == '\\') { // char escaping ...
                    query.deleteCharAt(0);
                    offset.increase(1);
                    if (query.isEmpty())
                        throw new InvalidQueryException("Lexer error: Escaped EOF at character index: " + symbolOffset);
                    char escapedC = query.charAt(0);
                    symbolValue.append(escapedC);
                    query.deleteCharAt(0);
                    offset.increase(1);
                } else {
                    query.deleteCharAt(0);
                    offset.increase(1);
                    if (Character.isWhitespace(c))
                        break;
                    symbolValue.append(c);
                    if (query.isEmpty())
                        break;
                }
            }
            TokenName name;

            // These words (when not quoted) are keywords
            switch (symbolValue.toString()) {
                case "AND":
                case "OR":
                case "NOT":
                    name = TokenName.KEYWORD;
                    symbolValue = new StringBuilder(symbolValue.toString().toLowerCase());
                    break;
                default:
                    name = TokenName.STRING;
            }

            return new Symbol(name, symbolValue.toString(), symbolOffset);
        }
    }

}
