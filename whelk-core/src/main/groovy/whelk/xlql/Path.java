package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;

public class Path {
    public String property;
    public List<String> path;

    Path(String property) {
        this.property = property;
        this.path = new ArrayList<>(Arrays.asList(property));
    }

    Path(String property, List<String> path) {
        this.property = property;
        this.path = new ArrayList<>(path);
    }

    Path(Path p) {
        this(p.property, p.path);
    }

    Path copy() {
        return new Path(this);
    }

    public void prependMeta() {
        path.add(0, JsonLd.getRECORD_KEY());
    }

    public void appendId() {
        path.add(JsonLd.getID_KEY());
    }

    public void appendUnderscoreStr() {
        path.add(JsonLd.getSEARCH_KEY());
    }

    public void setWorkToInstancePath() {
        path.add(0, JsonLd.getWORK_KEY());
        path.add(0, JsonLd.getREVERSE_KEY());
    }

    public void setInstanceToWorkPath() {
        path.add(0, JsonLd.getWORK_KEY());
    }

    public void expandChainAxiom(Disambiguate disambiguate) {
        this.path = disambiguate.expandChainAxiom(path);
    }

    public String stringify() {
        return path.stream().collect(Collectors.joining("."));
    }
}