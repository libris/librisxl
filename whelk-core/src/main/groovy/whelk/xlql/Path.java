package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;

public class Path {
    public List<DefaultField> defaultFields;
    public List<String> path;

    // TODO: Get substituions from context instead?
    private static final Map<String, String> substitutions = Map.of(
            "rdf:type", JsonLd.TYPE_KEY,
            "hasItem", String.format("%s.itemOf", JsonLd.REVERSE_KEY),
            "hasInstance", String.format("%s.instanceOf", JsonLd.REVERSE_KEY)
    );

    public Path(List<String> path) {
        this.path = new ArrayList<>(path);
    }

    Path(Path p) {
        this.path = new ArrayList<>(p.path);
        this.defaultFields = new ArrayList<>(p.defaultFields);
    }

    Path copy() {
        return new Path(this);
    }

    public List<Path> expand(String property, Disambiguate disambiguate, Disambiguate.OutsetType outsetType) {
        List<Path> altPaths = new ArrayList<>(List.of(this));

        expandChainAxiom(disambiguate);

        if (disambiguate.isType(property)) {
            return altPaths;
        }

        String domain = disambiguate.getDomain(property);

        Disambiguate.DomainCategory domainCategory = disambiguate.getDomainCategory(domain);
        if (domainCategory == Disambiguate.DomainCategory.ADMIN_METADATA) {
            prependMeta();
        }

        switch (outsetType) {
            case WORK -> {
                switch (domainCategory) {
                    // The property p appears only on instance, modify path to @reverse.instanceOf.p...
                    case INSTANCE, EMBODIMENT -> setWorkToInstancePath();
                    case CREATION_SUPER, UNKNOWN -> {
                        // The property p may appear on instance, add alternative path @reverse.instanceOf.p...
                        Path copy = copy();
                        copy.setWorkToInstancePath();
                        altPaths.add(copy);
                    }
                }
            }
            case INSTANCE -> {
                switch (domainCategory) {
                    // The property p appears only work, modify path to instanceOf.p...
                    case WORK -> setInstanceToWorkPath();
                    case CREATION_SUPER, UNKNOWN -> {
                        // The property p may appear on work, add alternative path instanceOf.p...
                        Path copy = copy();
                        copy.setInstanceToWorkPath();
                        altPaths.add(copy);
                    }
                }
            }
        }

        return altPaths;
    }

    public void prependMeta() {
        path.addFirst(JsonLd.RECORD_KEY);
    }

    public void appendId() {
        if (!path.getLast().equals(JsonLd.ID_KEY)) {
            path.add(JsonLd.ID_KEY);
        }
    }

    public void appendUnderscoreStr() {
        if (!path.getLast().equals(JsonLd.SEARCH_KEY)) {
            path.add(JsonLd.SEARCH_KEY);
        }
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
        return path.stream().map(this::substitute).collect(Collectors.joining("."));
    }

    private String substitute(String property) {
        return Optional.ofNullable(substitutions.get(property)).orElse(property);
    }

    @Override
    public String toString() {
        var s = new StringBuilder();
        s.append(path);
        if (!defaultFields.isEmpty()) {
            s.append("(defaultFields: ").append(defaultFields).append(")");
        }
        return s.toString();
    }
}