package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.EsMappings;
import whelk.search2.QueryUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

    private Token token;

    public Path(List<Subpath> path, Token token) {
        this.path = path;
        this.token = token;
    }

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

    // As represented in indexed docs
    public String jsonForm() {
        return path.stream()
                .map(Subpath::toString)
                .map(Path::substitute)
                .collect(Collectors.joining("."));
    }

    // As represented in query string
    public String queryForm() {
        if (token != null) {
            return token.formatted();
        }
        String s = path.stream().map(Subpath::queryForm).collect(Collectors.joining("."));
        return s.contains(":") ? QueryUtil.quote(s) : s;
    }

    public ExpandedPath expand(JsonLd jsonLd) {
        return expand(jsonLd, null);
    }

    public ExpandedPath expand(JsonLd jsonLd, Value value) {
        List<Subpath> expandedPath = expandShortHand(path);

        getSuffix(value).ifPresent(expandedPath::add);

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

    public Optional<String> getEsNestedStem(EsMappings esMappings) {
        String esPath = jsonForm();
        if (esMappings.isNestedTypeField(esPath)) {
            return Optional.of(esPath);
        }
        return esMappings.getNestedTypeFields().stream().filter(esPath::startsWith).findFirst();
    }

    private Optional<Key> getSuffix(Value value) {
        if (last() instanceof Property p && p.isObjectProperty()) {
            return switch (value) {
                case DateTime ignored -> Optional.empty();
                case FreeText freeText -> freeText.isWild() ? Optional.empty() : Optional.of(new Key.RecognizedKey(SEARCH_KEY));
                case Numeric ignored -> Optional.empty();
                case Resource resource -> switch (resource) {
                    case InvalidValue ignored -> Optional.empty();
                    case Link ignored -> Optional.of(new Key.RecognizedKey(ID_KEY));
                    case VocabTerm ignored -> Optional.empty();
                };
                case Term ignored -> Optional.empty();
                case null -> p.isType() || p.isVocabTerm() ? Optional.empty() : Optional.of(new Key.RecognizedKey(ID_KEY));
            };
        }
        return Optional.empty();
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

        public List<ExpandedPath> getAltPaths(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
            if (origPath != null && origPath.first() instanceof Property p && !p.isPlatformTerm() && !p.isRdfType()) {
                Set<Property> integralRelations = rdfSubjectTypes.stream()
                        .map(t -> QueryUtil.getIntegralRelationsForType(t, jsonLd))
                        .flatMap(List::stream)
                        .collect(Collectors.toSet());

                List<ExpandedPath> altPaths = new ArrayList<>();

                integralRelations.stream()
                        .map(ir -> applyIntegralRelation(ir, jsonLd))
                        .forEach(path -> path.ifPresent(altPaths::add));

                if (altPaths.isEmpty() || rdfSubjectTypes.stream().anyMatch(t -> p.mayAppearOnType(t, jsonLd))) {
                    altPaths.add(this);
                }

                return altPaths;
            }

            return List.of(this);
        }

        private Optional<ExpandedPath> applyIntegralRelation(Property integral, JsonLd jsonLd) {
            if (first() instanceof Property p) {
                if (integral.isInverseOf(p)) {
                    List<Subpath> adjustedPath = new ArrayList<>(path());
                    adjustedPath.removeFirst();
                    return Optional.of(new ExpandedPath(adjustedPath));
                } else {
                    boolean followIntegralRelation = integral.range().stream().anyMatch(irRangeType -> p.mayAppearOnType(irRangeType, jsonLd));
                    if (followIntegralRelation && !p.name().equals(RECORD_KEY)) {
                        List<Subpath> adjustedPath = new ArrayList<>(path()) {{
                            addFirst(integral);
                        }};
                        return Optional.of(new ExpandedPath(adjustedPath));
                    }
                }
            }
            return Optional.empty();
        }

        public List<ExpandedPath> getAlt2Paths(JsonLd jsonLd) {
            // TODO this should be the responsibility of Property?
            if (origPath != null && origPath.first() instanceof Property p && jsonLd.indexMapTermsOf.containsKey(p.name())) {
                List<ExpandedPath> altPaths = new ArrayList<>();
                if (jsonLd.indexMapTermsOf.containsKey(p.name())) {
                    for (String indexMap : jsonLd.indexMapTermsOf.get(p.name)) {
                        for (String ix : List.of("find", "identify")) { // FIXME where should we get these?
                            altPaths.add(new ExpandedPath(List.of(
                                    new Key.RecognizedKey(indexMap),
                                    new Key.RecognizedKey(ix),
                                    new Key.RecognizedKey(ID_KEY)
                            )));
                        }
                    }
                }

                return altPaths;
            }

            return Collections.emptyList();
        }
    }
}
