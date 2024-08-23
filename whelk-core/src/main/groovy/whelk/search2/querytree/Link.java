package whelk.search2.querytree;

import whelk.search2.Disambiguate;

import java.util.Collections;
import java.util.Objects;

public record Link(String iri, Object chip) implements Value {
    public Link(String string) {
        this(string, Collections.emptyMap());
    }

    @Override
    public String string() {
        return iri;
    }

    @Override
    public Object description() {
        return chip;
    }

    @Override
    public String canonicalForm() {
        return Disambiguate.toPrefixed(iri);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Link && ((Link) o).iri().equals(iri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(iri);
    }
}
