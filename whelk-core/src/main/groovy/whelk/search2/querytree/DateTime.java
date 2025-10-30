package whelk.search2.querytree;

import whelk.search.QueryDateTime;
import whelk.search2.QueryUtil;

import java.time.format.DateTimeParseException;
import java.util.Objects;

public record DateTime(QueryDateTime dateTime, Token token) implements Value {
    public DateTime(QueryDateTime dateTime) {
        this(dateTime, null);
    }

    @Override
    public String queryForm() {
        if (token != null) {
            return token.formatted();
        }
        String dateString = dateTime.toDateString();
        return dateString.contains(":") ? QueryUtil.quote(dateString) : dateString;
    }

    @Override
    public String toString() {
        return dateTime.toDateString();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof DateTime dt && dt.toString().equals(toString());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(toString());
    }

    public static DateTime parse(String s, Token token) {
        try {
            return new DateTime(QueryDateTime.parse(s), token);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }
}
