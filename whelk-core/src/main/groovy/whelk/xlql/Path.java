package whelk.xlql;

import whelk.JsonLd;

import java.util.*;

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
        this.property = p.property;
        this.path = new ArrayList<>(p.path);
        this.defaultFields = new ArrayList<>(p.defaultFields);
    }

    Path copy() {
        return new Path(this);
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