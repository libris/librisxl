package whelk.search2.querytree;

import whelk.search2.Query;

import java.util.List;

import java.util.Objects;
import java.util.stream.Collectors;

import static whelk.search2.Query.Connective.AND;

public record FreeText(Property.TextQuery textQuery, List<Token> tokens, Query.Connective connective) implements Node, Value {
    public FreeText(Property.TextQuery textQuery, Token token) {
        this(textQuery, List.of(token), AND);
    }

    public FreeText(Token token) {
        this(null, token);
    }

    public FreeText(String s) {
        this(new Token.Raw(s));
    }

    @Override
    public String queryForm() {
        return joinTokens();
    }

    @Override
    public Node getInverse() {
        return new Not(this);
    }

    @Override
    public boolean isMultiToken() {
        return tokens.size() > 1;
    }

    @Override
    public boolean isRangeOpCompatible() {
        return tokens.size() == 1 && tokens.getFirst().isDigits();
    }

    @Override
    public String toString() {
        return toQueryString();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FreeText ft && ft.toString().equals(toString());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(toString());
    }

    public String toEsString() {
        return joinTokens(tokens, " ");
    }

    public FreeText withTokens(List<Token> tokens) {
        return new FreeText(textQuery, tokens, connective);
    }

    public boolean isDigits() {
        return tokens.stream().allMatch(Token::isDigits);
    }

    private String joinTokens() {
        return switch (connective) {
            case AND -> joinTokens(tokens, " ");
            case OR -> joinTokens(tokens, " OR ");
        };
    }

    private static String joinTokens(List<Token> tokens, String delimiter) {
        return tokens.stream().map(Token::formatted).collect(Collectors.joining(delimiter));
    }
}
