package whelk.search2.querytree.value;

import whelk.search2.Operator;
import whelk.search2.Query;
import whelk.search2.querytree.selector.Property;
import whelk.search2.querytree.node.Condition;

import java.util.List;

import java.util.Objects;
import java.util.stream.Collectors;

import static whelk.search2.Query.Connective.AND;

public record FreeText(List<Token> tokens, Query.Connective connective) implements Value {
    public FreeText(Token token) {
        this(List.of(token), AND);
    }

    public FreeText(String s) {
        this(new Token.Raw(s));
    }

    @Override
    public String queryForm() {
        return joinTokens();
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
    public boolean equals(Object obj) {
        return obj instanceof FreeText ft && ft.queryForm().equals(queryForm());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queryForm());
    }

    public String toEsString() {
        return joinTokens(tokens, " ");
    }

    public FreeText withTokens(List<Token> tokens) {
        return new FreeText(tokens, connective);
    }

    public Condition asNode() {
        return new Condition(new Property.TextQuery(), Operator.EQUALS, this);
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
