package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.xlql.Disambiguate.RDF_TYPE;

public class Path {
    public List<String> stem;
    public List<Branch> branches;
    private List<List<String>> roots;

    public record Branch(List<String> path, SimpleQueryTree.Value value) {
        Branch(List<String> path) {
            this(path, null);
        }
    }

    String path;

    // TODO: Get substituions from context instead?
    private static final Map<String, String> substitutions = Map.of(
            RDF_TYPE, JsonLd.TYPE_KEY,
            "hasItem", String.format("%s.itemOf", JsonLd.REVERSE_KEY),
            "hasInstance", String.format("%s.instanceOf", JsonLd.REVERSE_KEY)
    );

    public Path(List<String> path) {
        this.stem = path;
        this.branches = new ArrayList<>();
        this.roots = new ArrayList<>();
    }

    public void expand(String property, Disambiguate disambiguate, Disambiguate.OutsetType outsetType) {
        expand(property, disambiguate, outsetType, null);
    }

    public void expand(String property, Disambiguate disambiguate, Disambiguate.OutsetType outsetType, SimpleQueryTree.Value value) {
        expandChainAxiom(disambiguate);

        if (disambiguate.isProperty(property) && !disambiguate.isType(property)) {
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
                        // The property p may appear on instance, add alternative path @reverse.instanceOf.p...
                        case CREATION_SUPER, UNKNOWN -> setAltWorkToInstancePath();
                    }
                }
                case INSTANCE -> {
                    switch (domainCategory) {
                        // The property p appears only work, modify path to instanceOf.p...
                        case WORK -> setInstanceToWorkPath();
                        // The property p may appear on work, add alternative path instanceOf.p...
                        case CREATION_SUPER, UNKNOWN -> setAltInstanceToWorkPath();
                    }
                }
            }

            addSuffixAndValue(disambiguate, value);
        }
    }

    public List<Path> getAltStems() {
        return roots.isEmpty()
                ? List.of(this)
                : roots.stream()
                .map(r -> new Path(concat(r, stem)))
                .toList();
    }

    public Path attachBranch(Branch b) {
        return new Path(concat(stem, b.path()));
    }

    private List<String> concat(List<String> a, List<String> b) {
        return Stream.concat(a.stream(), b.stream()).toList();
    }

    public void addSuffixAndValue(Disambiguate disambiguate, SimpleQueryTree.Value value) {
        if (branches.isEmpty()) {
            getSuffix(stem.getLast(), disambiguate, value)
                    .map(List::of)
                    .map(b -> new Branch(b, value))
                    .ifPresent(branches::add);
        } else {
            branches = branches.stream().map(b -> {
                        var val = b.value() != null ? b.value() : value;
                        return getSuffix(b.path.getLast(), disambiguate, val)
                                .map(List::of)
                                .map(s -> new Branch(concat(b.path(), s), val))
                                .orElse(new Branch(b.path(), val));
                    })
                    .toList();
        }
    }

    Optional<String> getSuffix(String key, Disambiguate disambiguate, SimpleQueryTree.Value value) {
        if (disambiguate.isObjectProperty(key) && !disambiguate.hasVocabValue(key)) {
            return value instanceof SimpleQueryTree.Literal
                    ? Optional.of(JsonLd.SEARCH_KEY)
                    : Optional.of(JsonLd.ID_KEY);
        }
        return Optional.empty();
    }

    private void prependMeta() {
        extendRoots(JsonLd.RECORD_KEY);
    }

    private void setInstanceToWorkPath() {
        extendRoots(JsonLd.WORK_KEY);
    }

    private void setAltInstanceToWorkPath() {
        branchOutRoot(JsonLd.WORK_KEY);
    }

    private void setWorkToInstancePath() {
        extendRoots(List.of(JsonLd.REVERSE_KEY, JsonLd.WORK_KEY));
    }

    private void setAltWorkToInstancePath() {
        branchOutRoot(List.of(JsonLd.REVERSE_KEY, JsonLd.WORK_KEY));
    }

    public void expandChainAxiom(Disambiguate disambiguate) {
        disambiguate.expandChainAxiom(this);
    }

    public String stringify() {
        if (path == null) {
            this.path = stem.stream().map(this::substitute).collect(Collectors.joining("."));
        }
        return path;
    }

    private String substitute(String property) {
        return Optional.ofNullable(substitutions.get(property)).orElse(property);
    }

    private void extendRoots(String key) {
        extendRoots(List.of(key));
    }

    private void extendRoots(List<String> path) {
        if (roots.isEmpty()) {
            roots.add(path);
        } else {
            roots.replaceAll(r -> concat(path, r));
        }
    }

    private void branchOutRoot(String key) {
        branchOutRoot(List.of(key));
    }

    private void branchOutRoot(List<String> path) {
        roots = roots.isEmpty()
                ? List.of(path, Collections.emptyList())
                : roots.stream()
                .flatMap(r -> Stream.of(r, concat(path, r)))
                .toList();
    }
}