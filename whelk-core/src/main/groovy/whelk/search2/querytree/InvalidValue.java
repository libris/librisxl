package whelk.search2.querytree;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static whelk.JsonLd.TYPE_KEY;

public final class InvalidValue extends Resource {
    private enum Reason {
        FORBIDDEN,
        AMBIGUOUS
    }

    private final Token token;
    private final Reason reason;

    public InvalidValue(Token token, Reason reason) {
        this.token = token;
        this.reason = reason;
    }

    public static InvalidValue ambiguous(Token token) {
        return new InvalidValue(token, Reason.AMBIGUOUS);
    }

    public static InvalidValue ambiguous(String value) {
        return ambiguous(new Token.Raw(value));
    }

    public static InvalidValue forbidden(Token token) {
        return new InvalidValue(token, Reason.FORBIDDEN);
    }

    public static InvalidValue forbidden(String value) {
        return forbidden(new Token.Raw(value));
    }

    @Override
    public String getType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> description() {
        var m = new LinkedHashMap<String, Object>();
        m.put(TYPE_KEY, "_Invalid");
        m.put("label", token.value());
        return m;
    }

    @Override
    public String jsonForm() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String queryForm() {
        return token.value();
    }

    @Override
    public String toString() {
        return token.value();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof InvalidValue iv && iv.queryForm().equals(queryForm());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queryForm());
    }
}