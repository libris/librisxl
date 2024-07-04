package whelk.search2.querytree;

import whelk.search2.Disambiguate;

public record Link(String string) implements Value {
    @Override
    public String canonicalForm() {
        return Disambiguate.toPrefixed(string);
    }
}
