package whelk.search2.querytree;

import whelk.search2.QueryUtil;
import whelk.search2.VocabMappings;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.asList;

public final class Link extends Resource {
    private final String iri;
    private final Map<String, Object> thing = new LinkedHashMap<>();
    private final Map<String, Object> chip = new LinkedHashMap<>();

    private Token token;

    public Link(String iri) {
        this.iri = iri;
    }

    public Link(String iri, Token token) {
        this.iri = iri;
        this.token = token;
    }

    public Link(String iri, Map<String, Object> thing) {
        this.iri = iri;
        setThing(thing);
    }

    public void setChip(Map<String, Object> chip) {
        this.chip.clear();
        this.chip.putAll(chip);
    }

    public void setThing(Map<String, Object> thing) {
        this.chip.clear();
        this.thing.putAll(thing);
    }

    public String iri() {
        return iri;
    }

    public Map<String, Object> thing() {
        return thing;
    }

    public Token token() {
        return token;
    }

    @Override
    public Map<String, Object> description() {
        return chip;
    }

    @Override
    public String queryForm() {
        return token != null ? token.formatted() : QueryUtil.quote(VocabMappings.toPrefixed(iri));
    }

    @Override
    public String jsonForm() {
        return iri;
    }

    @Override
    public String toString() {
        return VocabMappings.toPrefixed(iri);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Link l && l.iri().equals(iri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(iri);
    }

    @Override
    public String getType() {
        return (String) asList(thing.get(TYPE_KEY)).getFirst();
    }
}
