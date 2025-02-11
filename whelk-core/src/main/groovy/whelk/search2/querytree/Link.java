package whelk.search2.querytree;

import whelk.search2.Disambiguate;

import java.util.Collections;
import java.util.Objects;

public record Link(String iri, Object chip, String raw) implements Value {
    public Link(String iri, Object chip) {
        this(iri, chip, null);
    }

    public Link(String string) {
        this(string, Collections.emptyMap());
    }

    @Override
    public Object description() {
        return chip;
    }

    @Override
    public String raw() {
        return raw != null ? raw : Disambiguate.toPrefixed(iri);
    }

    @Override
    public String jsonForm() {
        return iri;
    }

    @Override
    public String toString() {
        return Disambiguate.toPrefixed(iri);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Link l && l.iri().equals(iri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(iri);
    }
}
