package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.RECORD_KEY;
import static whelk.JsonLd.Rdfs.RDF_TYPE;
import static whelk.JsonLd.SEARCH_KEY;


public class Path {
    // TODO: Get substitutions from context instead?
    private static final Map<String, String> substitutions = Map.of(
            RDF_TYPE, JsonLd.TYPE_KEY,
            "hasItem", String.format("%s.itemOf", JsonLd.REVERSE_KEY),
            "hasInstance", String.format("%s.instanceOf", JsonLd.REVERSE_KEY)
    );

    private final List<Subpath> path;

    public Path(List<Subpath> path) {
        this.path = path;
    }

    public Path(Subpath subpath) {
        this.path = List.of(subpath);
    }

    public List<Subpath> path() {
        return path;
    }

    public Subpath first() {
        return path.getFirst();
    }

    public Subpath last() {
        return path.getLast();
    }

    public Optional<Property> lastProperty() {
        return getPropertyPath().reversed().stream().findFirst();
    }

    public Optional<Property> firstProperty() {
        return getPropertyPath().stream().findFirst();
    }

    public List<Property> getPropertyPath() {
        return path.stream()
                .filter(Property.class::isInstance)
                .map(Property.class::cast)
                .toList();
    }

    public boolean isValid() {
        return path.stream().allMatch(Subpath::isValid);
    }

    @Override
    public String toString() {
        return path.stream()
                .map(Subpath::toString)
                .collect(Collectors.joining("."));
    }

    public String fullSearchPath() {
        return path.stream()
                .map(Subpath::toString)
                .map(Path::substitute)
                .collect(Collectors.joining("."));
    }

    public String asKey() {
        return path.stream()
                .map(Subpath::key)
                .map(Key::toString)
                .collect(Collectors.joining("."));
    }

    public ExpandedPath expand(JsonLd jsonLd) {
        return expand(jsonLd, null);
    }

    public ExpandedPath expand(JsonLd jsonLd, Value value) {
        List<Subpath> expandedPath = expandShortHand(path);
        if (shouldAddSuffix(jsonLd, value)) {
            expandedPath.add(new Key.RecognizedKey(value instanceof Literal ? SEARCH_KEY : ID_KEY));
        }
        firstProperty().ifPresent(property -> {
            if (property.hasDomainAdminMetadata(jsonLd)) {
                expandedPath.addFirst(new Property(RECORD_KEY, jsonLd));
            }
        });
        return new ExpandedPath(expandedPath, this);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Path p && p.path().equals(path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    private boolean shouldAddSuffix(JsonLd jsonLd, Value value) {
        return !(value instanceof Literal l && l.isWildcard())
                && last() instanceof Property p
                && p.isObjectProperty()
                && !p.isType()
                && !jsonLd.isVocabTerm(p.name());
    }

    private static List<Subpath> expandShortHand(List<Subpath> path) {
        return path.stream()
                .flatMap(sp -> sp instanceof Property p ? p.expand().stream() : Stream.of(sp))
                .collect(Collectors.toList());
    }

    private static String substitute(String property) {
        return Optional.ofNullable(substitutions.get(property)).orElse(property);
    }

    public static class ExpandedPath extends Path {
        private Path origPath;

        ExpandedPath(List<Subpath> path, Path origPath) {
            super(path);
            this.origPath = origPath;
        }

        ExpandedPath(List<Subpath> path) {
            super(path);
        }

        ExpandedPath(Key key) {
            super(key);
        }

        @Override
        public ExpandedPath expand(JsonLd jsonLd, Value value) {
            return this;
        }

        public List<ExpandedPath> getAltPaths(JsonLd jsonLd, Collection<String> types) {
            if (origPath != null && origPath.first() instanceof Property p) {
                List<ExpandedPath> altPaths = p.getApplicableIntegralRelations(jsonLd, types).stream()
                        .map(ir -> Stream.concat(Stream.of(ir), path().stream()))
                        .map(Stream::toList)
                        .map(ExpandedPath::new)
                        .collect(Collectors.toList());

                if (altPaths.isEmpty() || types.stream().anyMatch(t -> p.mayAppearOnType(t, jsonLd))) {
                    altPaths.add(this);
                }

                return altPaths;
            }

            return List.of(this);
        }
    }
}
