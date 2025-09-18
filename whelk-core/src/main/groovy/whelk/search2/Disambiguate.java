package whelk.search2;

import groovy.transform.PackageScope;
import whelk.JsonLd;
import whelk.search2.querytree.*;
import whelk.util.Restrictions;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.JsonLd.LD_KEYS;
import static whelk.JsonLd.Rdfs.RDF_TYPE;
import static whelk.JsonLd.VOCAB_KEY;
import static whelk.JsonLd.looksLikeIri;
import static whelk.search2.QueryUtil.encodeUri;
import static whelk.search2.VocabMappings.expandPrefixed;

public class Disambiguate {
    private final JsonLd jsonLd;

    private final VocabMappings vocabMappings;
    private final Map<String, FilterAlias> filterAliasMappings;

    private final List<String> nsPrecedenceOrder;

    public Disambiguate(VocabMappings vocabMappings, Collection<FilterAlias> appFilterAliases, Collection<FilterAlias.QueryDefinedAlias> queryFilterAliases, JsonLd jsonLd) {
        this.vocabMappings = vocabMappings;
        this.filterAliasMappings = getFilterAliasMappings(appFilterAliases, queryFilterAliases);
        this.jsonLd = jsonLd;
        this.nsPrecedenceOrder = List.of("rdf", "librissearch", (String) jsonLd.context.get(VOCAB_KEY), "bibdb", "marc"); // FIXME
    }

    // For test only
    @PackageScope
    public Disambiguate(VocabMappings vocabMappings, Collection<FilterAlias> filterAliasMappings, JsonLd jsonLd) {
        this.vocabMappings = vocabMappings;
        this.filterAliasMappings = getFilterAliasMappings(filterAliasMappings, List.of());
        this.jsonLd = jsonLd;
        this.nsPrecedenceOrder = List.of("rdf", "librissearch", (String) jsonLd.context.get(VOCAB_KEY), "bibdb", "marc"); // FIXME
    }

    public Subpath mapKey(String key, int offset) {
        for (String ns : nsPrecedenceOrder) {
            Set<String> mappedProperties = vocabMappings.properties()
                    .getOrDefault(key.toLowerCase(), Map.of())
                    .getOrDefault(ns, Set.of());
            if (mappedProperties.size() == 1) {
                String p = getUnambiguous(mappedProperties);
                if (RDF_TYPE.equals(p)) {
                    return new Property.RdfType(jsonLd, new Key.RecognizedKey(key, offset));
                }
                return new Property(getUnambiguous(mappedProperties), jsonLd, new Key.RecognizedKey(key, offset));
            }
            if (mappedProperties.size() > 1) {
                // Ambiguous
                Optional<String> equalPropertyKey = mappedProperties.stream().filter(key::equalsIgnoreCase).findFirst();
                if (equalPropertyKey.isPresent()) {
                    return new Property(equalPropertyKey.get(), jsonLd, new Key.RecognizedKey(key, offset));
                }
                Optional<Property> propertyWithCode = mappedProperties.stream()
                        .map(pKey -> new Property(pKey, jsonLd, new Key.RecognizedKey(key, offset)))
                        .filter(property -> property.definition().containsKey("librisQueryCode"))
                        .findFirst();
                if (propertyWithCode.isPresent()) {
                    return propertyWithCode.get();
                }
                return new Key.AmbiguousKey(key, offset);
            }
        }

        // TODO: Get valid keys from ES index?
        if (LD_KEYS.contains(key) || key.startsWith("_")) {
            return new Key.RecognizedKey(key, offset);
        }

        return new Key.UnrecognizedKey(key, offset);
    }

    public Optional<Value> mapValueForProperty(Property property, String value) {
        return mapValueForProperty(property, value, null);
    }

    public Optional<Value> mapValueForProperty(Property property, Token token) {
        return mapValueForProperty(property, token.value(), token);
    }

    public Restrictions.Narrowed tryNarrow(String property, Value value) {
        if (value instanceof Link link) {
            return jsonLd.restrictions.tryNarrow(whelk.Link.of(property, link.iri()));
        }

        return null;
    }

    private Optional<Value> mapValueForProperty(Property property, String value, Token token) {
        if (value.equals(Operator.WILDCARD)) {
            return Optional.empty();
        }
        if (property.isXsdDate()) {
            var yearRange = YearRange.parse(value, token);
            if (yearRange != null) {
                return Optional.of(yearRange);
            }
            return Optional.ofNullable(DateTime.parse(value, token));
        }
        if (property.isType() || property.isVocabTerm()) {
            for (String ns : nsPrecedenceOrder) {
                var mappings = property.isType() ? vocabMappings.classes() : vocabMappings.enums();
                Set<String> mappedClasses = mappings.getOrDefault(value.toLowerCase(), Map.of()).getOrDefault(ns, Set.of());
                if (mappedClasses.size() == 1) {
                    return mappedClasses.stream().findFirst().map(term -> new VocabTerm(term, jsonLd.vocabIndex.get(term), token));
                }
                if (mappedClasses.size() > 1) {
                    // Ambiguous
                    Optional<String> equalTermKey = mappedClasses.stream().filter(value::equalsIgnoreCase).findFirst();
                    if (equalTermKey.isPresent()) {
                        return equalTermKey.map(term -> new VocabTerm(term, jsonLd.vocabIndex.get(term), token));
                    }
                    return Optional.of(token != null ? InvalidValue.ambiguous(token) : InvalidValue.ambiguous(value));
                }
            }
            return Optional.of(token != null ? InvalidValue.forbidden(token) : InvalidValue.forbidden(value));
        }
        if (property.isObjectProperty()) {
            String expanded = expandPrefixed(value);
            if (looksLikeIri(expanded)) {
                return Optional.of(new Link(encodeUri(expanded), token));
            }
        }
        /*
        TODO?
        Optimally we would also check here that the given property is a DatatypeProperty and not an ObjectProperty
        since only the former may have a numeric values, however there are fields such as reverseLinks.totalItemsByRelation.p
        where p is not a DatatypeProperty but still the field has numeric values, so a query like
        reverseLinks.totalItemsByRelation.instanceOf>1 wouldn't work due to the value 1 not being typed as Numeric
        and by extension the query wouldn't pass as a valid range query.
        */
        var numeric = Numeric.parse(value, token);
        if (numeric != null) {
            return Optional.of(numeric);
        }
        if (property.isDatatypeProperty()) {
            var yearRange = YearRange.parse(value, token);
            if (yearRange != null) {
                return Optional.of(yearRange);
            }
        }
        return Optional.empty();
    }

    public Optional<FilterAlias> mapToFilter(String alias) {
        return Optional.ofNullable(filterAliasMappings.get(alias.toLowerCase()));
    }

    public Property.TextQuery getTextQueryProperty() {
        return new Property.TextQuery(jsonLd);
    }

    private static String getUnambiguous(Set<String> mappedTerms) {
        return mappedTerms.iterator().next();
    }

    private Map<String, FilterAlias> getFilterAliasMappings(Collection<FilterAlias> appFilterAliases, Collection<FilterAlias.QueryDefinedAlias> queryFilterAliases) {
        return Stream.concat(appFilterAliases.stream(), queryFilterAliases.stream())
                .collect(Collectors.toMap(fa -> fa.alias().toLowerCase(), Function.identity()));
    }
}
