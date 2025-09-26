package whelk.search2;

import groovy.transform.PackageScope;
import whelk.JsonLd;
import whelk.search.QueryDateTime;
import whelk.search2.querytree.DateTime;
import whelk.search2.querytree.InvalidValue;
import whelk.search2.querytree.Key;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Numeric;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.Subpath;
import whelk.search2.querytree.Token;
import whelk.search2.querytree.Value;
import whelk.search2.querytree.VocabTerm;

import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static whelk.JsonLd.LD_KEYS;
import static whelk.JsonLd.looksLikeIri;
import static whelk.search2.QueryUtil.encodeUri;
import static whelk.search2.VocabMappings.expandPrefixed;

public class Disambiguate {
    private final JsonLd jsonLd;

    private final VocabMappings vocabMappings;
    private final Map<String, Filter.AliasedFilter> filterAliasMappings;

    private enum VocabTermType {
        CLASS,
        ENUM
    }

    public Disambiguate(VocabMappings vocabMappings, AppParams appParams, JsonLd jsonLd) {
        this.vocabMappings = vocabMappings;
        this.filterAliasMappings = getFilterAliasMappings(appParams.siteFilters.getAliasedFilters());
        this.jsonLd = jsonLd;
    }

    // For test only
    @PackageScope
    public Disambiguate(VocabMappings vocabMappings, Collection<Filter.AliasedFilter> filterAliasMappings, JsonLd jsonLd) {
        this.vocabMappings = vocabMappings;
        this.filterAliasMappings = getFilterAliasMappings(filterAliasMappings);
        this.jsonLd = jsonLd;
    }

    public Subpath mapKey(String key, int offset) {
        // TODO: Look up all indexed keys starting with underscore?
        if (LD_KEYS.contains(key) || key.startsWith("_")) {
            return new Key.RecognizedKey(key, offset);
        }

        Optional<String> mappedProperty = getMappedTerm(key, vocabMappings.propertyAliasMappings);
        if (mappedProperty.isPresent()) {
            return new Property(mappedProperty.get(), jsonLd, new Key.RecognizedKey(key, offset));
        }

        Set<String> multipleMappedProperties = getMappedTermsForAmbiguous(key, vocabMappings.ambiguousPropertyAliases);
        if (multipleMappedProperties.isEmpty()) {
            return new Key.UnrecognizedKey(key, offset);
        }

        Optional<String> equalPropertyKey = multipleMappedProperties.stream().filter(key::equalsIgnoreCase).findFirst();
        if (equalPropertyKey.isPresent()) {
            return new Property(equalPropertyKey.get(), jsonLd, new Key.RecognizedKey(key, offset));
        }

        Optional<Property> propertyWithCode = multipleMappedProperties.stream()
                .map(pKey -> new Property(pKey, jsonLd, new Key.RecognizedKey(key, offset)))
                .filter(property -> property.definition().containsKey("librisQueryCode"))
                .findFirst();
        if (propertyWithCode.isPresent()) {
            return propertyWithCode.get();
        }

        return new Key.AmbiguousKey(key, offset);
    }

    public Optional<Value> mapValueForProperty(Property property, String value) {
        return mapValueForProperty(property, value, null);
    }

    public Optional<Value> mapValueForProperty(Property property, Token token) {
        return mapValueForProperty(property, token.value(), token);
    }

    public Optional<Value> mapValueForProperty(Property property, String value, Token token) {
        if (value.equals(Operator.WILDCARD)) {
            return Optional.empty();
        }
        if (property.isXsdDate()) {
            try {
                QueryDateTime parsedDate = QueryDateTime.parse(value);
                return Optional.of(new DateTime(parsedDate, token));
            } catch (DateTimeParseException ignored) {
                return Optional.empty();
            }
        }
        if (property.isType() || property.isVocabTerm()) {
            Set<String> mappedTerms = mapToVocabTerm(value, property.isType() ? VocabTermType.CLASS : VocabTermType.ENUM);
            return switch (mappedTerms.size()) {
                case 0 -> Optional.of(token != null ? InvalidValue.forbidden(token) : InvalidValue.forbidden(value));
                case 1 -> mappedTerms.stream().findFirst().map(term -> new VocabTerm(term, jsonLd.vocabIndex.get(term), token));
                default -> mappedTerms.stream().filter(value::equalsIgnoreCase).findFirst()
                        .map(v -> (Value) new VocabTerm(v, jsonLd.vocabIndex.get(v), token))
                        .or(() -> Optional.of(token != null ? InvalidValue.ambiguous(token) : InvalidValue.ambiguous(value)));
            };
        }
        if (property.isObjectProperty()) {
            String expanded = expandPrefixed(value);
            if (looksLikeIri(expanded)) {
                return Optional.of(new Link(encodeUri(expanded), token));
            }
        }
        if (value.matches("\\d+")) {
            /*
            TODO?
            Optimally we would also check here that the given property is a DatatypeProperty and not an ObjectProperty
            since only the former may have a numeric values, however there are fields such as reverseLinks.totalItemsByRelation.p
            where p is not a DatatypeProperty but still the field has numeric values, so a query like
            reverseLinks.totalItemsByRelation.instanceOf>1 wouldn't work due to the value 1 not being typed as Numeric
            and by extension the query wouldn't pass as a valid range query.
            */
            try {
                return Optional.of(new Numeric(Long.parseLong(value), token));
            } catch (NumberFormatException ignored) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    public Optional<Filter.AliasedFilter> mapToFilter(String alias) {
        return Optional.ofNullable(filterAliasMappings.get(alias.toLowerCase()));
    }

    public Property.TextQuery getTextQueryProperty() {
        return new Property.TextQuery(jsonLd);
    }

    private Set<String> mapToVocabTerm(String s, VocabTermType vocabTermType) {
        Map<String, String> unambiguousMappings = switch (vocabTermType) {
            case CLASS -> vocabMappings.classAliasMappings;
            case ENUM -> vocabMappings.enumAliasMappings;
        };
        Map<String, Set<String>> ambiguousMappings = switch (vocabTermType) {
            case CLASS -> vocabMappings.ambiguousClassAliases;
            case ENUM -> vocabMappings.ambiguousEnumAliases;
        };
        return getMappedTerm(s, unambiguousMappings)
                .map(Set::of)
                .orElse(getMappedTermsForAmbiguous(s, ambiguousMappings));
    }

    private static Optional<String> getMappedTerm(String alias, Map<String, String> unambiguousMappings) {
        return Optional.ofNullable(unambiguousMappings.get(alias.toLowerCase()));
    }

    private static Set<String> getMappedTermsForAmbiguous(String alias, Map<String, Set<String>> ambiguousMappings) {
        return ambiguousMappings.getOrDefault(alias.toLowerCase(), Collections.emptySet());
    }

    private Map<String, Filter.AliasedFilter> getFilterAliasMappings(Collection<Filter.AliasedFilter> aliasedFilters) {
        return aliasedFilters.stream()
                .collect(Collectors.toMap(af -> af.alias().toLowerCase(), Function.identity()));
    }
}
