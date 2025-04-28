package whelk.search2;

import groovy.transform.PackageScope;
import whelk.JsonLd;
import whelk.search2.querytree.InvalidValue;
import whelk.search2.querytree.Key;
import whelk.search2.querytree.Link;
import whelk.search2.querytree.Literal;
import whelk.search2.querytree.Property;
import whelk.search2.querytree.Subpath;
import whelk.search2.querytree.Value;
import whelk.search2.querytree.VocabTerm;

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

    public Subpath mapKey(String key) {
        // TODO: Look up all indexed keys starting with underscore?
        if (LD_KEYS.contains(key) || key.startsWith("_")) {
            return new Key.RecognizedKey(key);
        }

        Optional<String> mappedProperty = getMappedTerm(key, vocabMappings.propertyAliasMappings);
        if (mappedProperty.isPresent()) {
            return new Property(mappedProperty.get(), jsonLd, key);
        }

        Set<String> multipleMappedProperties = getMappedTermsForAmbiguous(key, vocabMappings.ambiguousPropertyAliases);
        if (multipleMappedProperties.isEmpty()) {
            return new Key.UnrecognizedKey(key);
        }

        Optional<String> equalPropertyKey = multipleMappedProperties.stream().filter(key::equalsIgnoreCase).findFirst();
        if (equalPropertyKey.isPresent()) {
            return new Property(equalPropertyKey.get(), jsonLd, key);
        }

        Optional<Property> propertyWithCode = multipleMappedProperties.stream()
                .map(pKey -> new Property(pKey, jsonLd, key))
                .filter(property -> property.definition().containsKey("librisQueryCode"))
                .findFirst();
        if (propertyWithCode.isPresent()) {
            return propertyWithCode.get();
        }

        return new Key.AmbiguousKey(key);
    }

    public Value getValueForProperty(Property property, String rawValue) {
        if (rawValue.equals(Operator.WILDCARD)) {
            return new Literal(rawValue);
        }
        if (property.isType()) {
            return mapVocabTermValue(rawValue, VocabTermType.CLASS);
        }
        if (property.isVocabTerm()) {
            return mapVocabTermValue(rawValue, VocabTermType.ENUM);
        }
        if (property.isObjectProperty()) {
            String expanded = expandPrefixed(rawValue);
            if (looksLikeIri(expanded)) {
                return new Link(encodeUri(expanded), rawValue);
            }
        }
        return new Literal(rawValue);
    }

    public Optional<Filter.AliasedFilter> mapToFilter(String alias) {
        return Optional.ofNullable(filterAliasMappings.get(alias.toLowerCase()));
    }

    public Property.TextQuery getTextQueryProperty() {
        return new Property.TextQuery(jsonLd);
    }

    private Value mapVocabTermValue(String value, VocabTermType vocabTermType) {
        Map<String, String> unambiguousMappings = switch (vocabTermType) {
            case CLASS -> vocabMappings.classAliasMappings;
            case ENUM -> vocabMappings.enumAliasMappings;
        };
        Map<String, Set<String>> ambiguousMappings = switch (vocabTermType) {
            case CLASS -> vocabMappings.ambiguousClassAliases;
            case ENUM -> vocabMappings.ambiguousEnumAliases;
        };

        Optional<String> mappedValue = getMappedTerm(value, unambiguousMappings);
        if (mappedValue.isPresent()) {
            return new VocabTerm(mappedValue.get(), jsonLd.vocabIndex.get(mappedValue.get()), value);
        }

        Set<String> multipleMappedValues = getMappedTermsForAmbiguous(value, ambiguousMappings);
        if (multipleMappedValues.isEmpty()) {
            return new InvalidValue.ForbiddenValue(value);
        }

        return multipleMappedValues.stream().filter(value::equalsIgnoreCase).findFirst()
                .map(v -> (Value) new VocabTerm(v, jsonLd.vocabIndex.get(v), value))
                .orElse(new InvalidValue.AmbiguousValue(value));
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
