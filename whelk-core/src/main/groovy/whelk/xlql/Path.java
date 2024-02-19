package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;

public class Path {
    public List<DefaultField> defaultFields;
    public String property;
    public List<String> path;

    Path(String property, List<String> path) {
        this.property = property;
        this.path = new ArrayList<>(path);
    }

    Path(List<String> path) {
        this.path = new ArrayList<>(path);
    }

    Path(Path p) {
        this(p.property, p.path);
    }

    Path copy() {
        return new Path(this);
    }

    public void prependMeta() {
        path.add(0, JsonLd.RECORD_KEY);
    }

    public void appendId() {
        path.add(JsonLd.ID_KEY);
    }

    public void appendUnderscoreStr() {
        path.add(JsonLd.SEARCH_KEY);
    }

    public void setWorkToInstancePath() {
        path.add(0, JsonLd.WORK_KEY);
        path.add(0, JsonLd.REVERSE_KEY);
    }

    public void setInstanceToWorkPath() {
        path.add(0, JsonLd.WORK_KEY);
    }

    public void expandChainAxiom(Disambiguate disambiguate) {
        Disambiguate.PropertyChain pc = disambiguate.expandChainAxiom(path);
        this.path = pc.path();
        this.defaultFields = pc.defaultFields();
    }

    public String stringify() {
        return path.stream().collect(Collectors.joining("."));
    }
}