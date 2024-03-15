package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;

public class Path {
    public List<DefaultField> defaultFields;
    public List<String> path;

    private static final Map<String, String> substitutions = Map.of("rdf:type", JsonLd.TYPE_KEY);

    Path(List<String> path) {
        this.path = getLdPath(path);
    }

    Path(Path p) {
        this.path = getLdPath(p.path);
        this.defaultFields = new ArrayList<>(p.defaultFields);
    }

    Path copy() {
        return new Path(this);
    }

    private List<String> getLdPath(List<String> path) {
        return path.stream().map(this::substitute).collect(Collectors.toList());
    }

    private String substitute(String property) {
        return Optional.ofNullable(substitutions.get(property)).orElse(property);
    }

    public void prependMeta() {
        path.addFirst(JsonLd.RECORD_KEY);
    }

    public void appendId() {
        path.add(JsonLd.ID_KEY);
    }

    public void appendUnderscoreStr() {
        path.add(JsonLd.SEARCH_KEY);
    }

    public void setWorkToInstancePath() {
        path.addFirst(JsonLd.WORK_KEY);
        path.addFirst(JsonLd.REVERSE_KEY);
        if (defaultFields != null) {
            defaultFields.forEach(df ->
                    {
                        df.path().addFirst(JsonLd.WORK_KEY);
                        df.path().addFirst(JsonLd.REVERSE_KEY);
                    }
            );
        }
    }

    public void setInstanceToWorkPath() {
        path.addFirst(JsonLd.WORK_KEY);
        if (defaultFields != null) {
            defaultFields.forEach(df -> df.path().addFirst(JsonLd.WORK_KEY));
        }
    }

    public void expandChainAxiom(Disambiguate disambiguate) {
        Disambiguate.PropertyChain pc = disambiguate.expandChainAxiom(path);
        this.path = pc.path();
        this.defaultFields = pc.defaultFields();
    }

    public String stringify() {
        return String.join(".", path);
    }
}