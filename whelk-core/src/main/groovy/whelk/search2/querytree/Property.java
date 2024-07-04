package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.Disambiguate;
import whelk.search2.OutsetType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public record Property(String name, Optional<String> code) {
    public Property(String name) {
        this(name, Optional.empty());
    }

    public String canonicalForm() {
        return code.orElse(name);
    }

    public Node expand(Disambiguate disambiguate, OutsetType outsetType) {
        return new Expand(disambiguate, outsetType).expanded;
    }

    public boolean isRdfType() {
        return Disambiguate.RDF_TYPE.equals(name);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Property) {
            return ((Property) o).name().equals(name);
        }
        if (o instanceof String) {
            return name.equals(o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    private class Expand {
        private Node expanded;
        private Disambiguate.DomainCategory domainCategory;

        Expand(Disambiguate disambiguate, OutsetType outsetType) {
            var p = Property.this.name;

            this.expanded = disambiguate.isShortHand(p)
                    ? disambiguate.expandChainAxiom(p)
                    : new PathValue(List.of(p));

            if (disambiguate.isProperty(p) && !disambiguate.isType(p)) {
                String domain = disambiguate.getDomain(p);
                this.domainCategory = disambiguate.getDomainCategory(domain);
                if (domainCategory == Disambiguate.DomainCategory.ADMIN_METADATA) {
                    prependMetaKey();
                }
                appendSuffix(disambiguate);
                setAlternativePaths(outsetType);
            }
        }

        private void setAlternativePaths(OutsetType outsetType) {
            this.expanded = setAlternativePaths(expanded, outsetType);
        }

        private Node setAlternativePaths(Node n, OutsetType outsetType) {
            return switch (outsetType) {
                case WORK -> switch (domainCategory) {
                    // The property p appears only on instance, modify path to @reverse.instanceOf.p...
                    case INSTANCE, EMBODIMENT -> setWorkToInstancePath(n);
                    // The property p may appear on instance, add alternative path @reverse.instanceOf.p...
                    case CREATION_SUPER, UNKNOWN -> new Or(List.of(n, setWorkToInstancePath(n)));
                    default -> n;
                };
                case INSTANCE -> switch (domainCategory) {
                    // The property p appears only work, modify path to instanceOf.p...
                    case WORK -> setWorkToInstancePath(n);
                    // The property p may appear on work, add alternative path instanceOf.p...
                    case CREATION_SUPER, UNKNOWN -> new Or(List.of(n, setInstanceToWorkPath(n)));
                    default -> n;
                };
                case RESOURCE -> n;
            };
        }

        private static Node setWorkToInstancePath(Node n) {
            return modifyPath(n, pathValue -> pathValue.prepend(List.of(JsonLd.REVERSE_KEY, JsonLd.WORK_KEY)));
        }

        private static Node setInstanceToWorkPath(Node n) {
            return modifyPath(n, pathValue -> pathValue.prepend(JsonLd.WORK_KEY));
        }

        private static Node modifyPath(Node n, Function<PathValue, PathValue> modifier) {
            return switch (n) {
                case Group group -> group.mapAndRebuild(c -> modifyPath(c, modifier));
                case PathValue pathValue -> modifier.apply(pathValue);
                default -> n;
            };
        }

        private void appendSuffix(Disambiguate disambiguate) {
            this.expanded = appendSuffix(expanded, disambiguate);
        }

        private static Node appendSuffix(Node n, Disambiguate disambiguate) {
            Function<PathValue, PathValue> modifier = pathValue -> getSuffix(pathValue, disambiguate)
                    .map(pathValue::append)
                    .orElse(pathValue);
            return modifyPath(n, modifier);
        }

        private static Optional<String> getSuffix(PathValue pathValue, Disambiguate disambiguate) {
            var key = pathValue.getPath().getLast();
            if (disambiguate.isObjectProperty(key) && !disambiguate.hasVocabValue(key)) {
                return pathValue.value() instanceof Literal
                        ? Optional.of(JsonLd.SEARCH_KEY)
                        : Optional.of(JsonLd.ID_KEY);
            }
            return Optional.empty();
        }

        private void prependMetaKey() {
            this.expanded = prependMetaKey(expanded);
        }

        private static Node prependMetaKey(Node n) {
            return modifyPath(n, pathValue -> pathValue.prepend(JsonLd.RECORD_KEY));
        }
    }
}
