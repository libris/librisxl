package whelk.search2;

import whelk.JsonLd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static whelk.JsonLd.ALTERNATE_PROPERTIES;
import static whelk.JsonLd.CACHE_RECORD_TYPE;
import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.Rdfs.RANGE;
import static whelk.JsonLd.RECORD_TYPE;
import static whelk.JsonLd.SEARCH_KEY;
import static whelk.JsonLd.Rdfs.SUB_PROPERTY_OF;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.VIRTUAL_RECORD_TYPE;
import static whelk.JsonLd.asList;
import static whelk.search2.QueryUtil.mustWrap;
import static whelk.search2.QueryUtil.shouldWrap;
import static whelk.util.DocumentUtil.getAtPath;

public class EsBoost {
    private static final int CHIP_BOOST = 200;
    private static final int STR_BOOST = 100;
    private static final int CARD_BOOST = 10;

    JsonLd jsonLd;
    Chips chipLenses;
    Cards cardLenses;

    private final Map<String, List<String>> boostFieldsByType = new HashMap<>();

    EsBoost(JsonLd jsonLd) {
        this.jsonLd = jsonLd;
        this.chipLenses = collectChipLenses();
        this.cardLenses = collectCardLenses();
    }

    public List<String> getBoostFields(Collection<String> types) {
        List<String> sortedTypes = types.stream().sorted().toList();

        String typeKey = String.join(",", sortedTypes);

        List<String> boostFields = boostFieldsByType.getOrDefault(typeKey, computeBoostFields(sortedTypes));
        boostFieldsByType.put(typeKey, boostFields);

        return boostFields;
    }

    public static Map<String, Object> addConstantBoosts(Map<String, Object> esQuery) {
        List<Map<String, Object>> constantBoosts = List.of(recordsOverCacheRecordsBoost());

        var mustClause = new ArrayList<>();
        mustClause.add(esQuery);
        mustClause.addAll(constantBoosts);

        return mustWrap(mustClause);
    }

    private List<String> computeBoostFields(List<String> types) {
        /* FIXME:
           lensBoost.computeBoostFieldsFromLenses does not give a good result for Concept.
           Use hand-tuned boosting instead until we improve boosting/ranking in general. See LXL-3399 for details.
        */
        List<String> conceptTypes = new ArrayList<>();
        List<String> otherTypes = new ArrayList<>();
        for (String s : types) {
            if (jsonLd.isSubClassOf(s, "Concept")) {
                conceptTypes.add(s);
            } else {
                otherTypes.add(s);
            }
        }

        Map<String, Integer> boostFields;

        if (conceptTypes.isEmpty()) {
            boostFields = computeBoostFieldsFromLenses(otherTypes);
        } else {
            boostFields = CONCEPT_BOOST.stream()
                    .map(s -> s.split("\\^"))
                    .collect(Collectors.toMap(parts -> parts[0], parts -> Integer.parseInt(parts[1])));
            if (!otherTypes.isEmpty()) {
                computeBoostFieldsFromLenses(otherTypes).forEach(boostFields::putIfAbsent);
            }
        }

        return boostFields.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .map(e -> e.getKey() + "^" + e.getValue())
                .toList();
    }

    private Map<String, Integer> computeBoostFieldsFromLenses(List<String> types) {
        Map<String, Integer> boostFields = new HashMap<>();

        boostFields.put(SEARCH_KEY, STR_BOOST);

        var baseTypes = List.of("Identity", "Instance", "Item");

        collectLenses().forEach(lensGroup ->
                lensGroup.collectBoostFields(types, baseTypes)
                        .forEach(boostFields::putIfAbsent)
        );

        return boostFields;
    }

    private List<LensGroup> collectLenses() {
        List<LensGroup> lensGroups = new ArrayList<>();
        lensGroups.add(chipLenses);
        lensGroups.add(cardLenses);
        return lensGroups;
    }

    private Chips collectChipLenses() {
        List<Lens> chips = new ArrayList<>();

        ((Map<?, ?>) getAtPath(jsonLd.displayData, List.of("lensGroups", "chips", "lenses"), Collections.emptyMap()))
                .forEach((type, lens) -> chips.add(new EsBoost.Chip((String) type, (Map<?, ?>) lens)));

        return new Chips(chips);
    }

    private Cards collectCardLenses() {
        List<Lens> cards = new ArrayList<>();

        ((Map<?, ?>) getAtPath(jsonLd.displayData, List.of("lensGroups", "cards", "lenses"), Collections.emptyMap()))
                .forEach((type, lens) -> cards.add(new EsBoost.Card((String) type, (Map<?, ?>) lens)));

        return new Cards(cards);
    }

    private sealed abstract class LensGroup permits Cards, Chips {
        abstract List<Lens> lenses();

        abstract Lens newLens(String type, Map<?, ?> lens);

        Map<String, Integer> collectBoostFields(List<String> types, List<String> baseTypes) {
            Map<String, Integer> boostFields = new HashMap<>();
            getLensesForTypes(types, baseTypes)
                    .forEach(lens -> boostFields.putAll(lens.collectBoostFields()));
            return boostFields;
        }

        Map<?, ?> getLensForType(String type) {
            var lensMap = Map.of("lenses", lenses().stream().collect(Collectors.toMap(Lens::type, Lens::lens)));
            return jsonLd.getLensFor(Map.of(TYPE_KEY, type), lensMap);
        }

        private List<Lens> getLensesForTypes(List<String> types, List<String> baseTypes) {
            return !types.isEmpty() ? getLensesForTypes(types) : getLensesForBaseTypes(baseTypes);
        }

        private List<Lens> getLensesForTypes(List<String> types) {
            List<Lens> lenses = new ArrayList<>();
            types.forEach(t ->
                    Optional.ofNullable(getLensForType(t))
                            .map(lens -> newLens(t, lens))
                            .ifPresent(lenses::add)
            );
            return lenses;
        }

        private List<Lens> getLensesForBaseTypes(List<String> baseTypes) {
            return lenses().stream()
                    .filter(c -> baseTypes.stream().anyMatch(c::partiallyAppliesTo))
                    .toList();
        }
    }

    private final class Chips extends LensGroup {
        List<Lens> chips;

        Chips(List<Lens> chips) {
            this.chips = chips;
        }

        @Override
        List<Lens> lenses() {
            return chips;
        }

        @Override
        Lens newLens(String type, Map<?, ?> lens) {
            return new Chip(type, lens);
        }
    }

    private final class Cards extends LensGroup {
        List<Lens> cards;

        Cards(List<Lens> cards) {
            this.cards = cards;
        }

        @Override
        List<Lens> lenses() {
            return cards;
        }

        @Override
        Lens newLens(String type, Map<?, ?> lens) {
            return new Card(type, lens);
        }
    }

    private sealed abstract class Lens permits EsBoost.Card, Chip {
        abstract String type();

        abstract Map<?, ?> lens();

        abstract Map<String, Integer> collectBoostFields();

        boolean partiallyAppliesTo(String baseType) {
            return jsonLd.isSubClassOf((String) lens().get("classLensDomain"), baseType);
        }
    }

    private final class Chip extends Lens {
        String type;
        Map<?, ?> lens;

        Chip(String type, Map<?, ?> lens) {
            this.type = type;
            this.lens = lens;
        }

        @Override
        String type() {
            return type;
        }

        @Override
        Map<?, ?> lens() {
            return lens;
        }

        @Override
        Map<String, Integer> collectBoostFields() {
            return EsBoost.this.collectBoostFields(lens, CHIP_BOOST);
        }
    }

    private final class Card extends Lens {
        String type;
        Map<?, ?> lens;

        Card(String type, Map<?, ?> lens) {
            this.type = type;
            this.lens = lens;
        }

        @Override
        String type() {
            return type;
        }

        @Override
        Map<?, ?> lens() {
            return lens;
        }

        @Override
        Map<String, Integer> collectBoostFields() {
            Map<String, Integer> boostFields = new HashMap<>();
            getPropertiesToShow(lens).stream()
                    .map(EsBoost.this::computeCardPropertyBoosts)
                    .forEach(boostFields::putAll);
            return boostFields;
        }
    }

    private Map<String, Integer> collectBoostFields(Map<?, ?> lens, int boost) {
        Map<String, Integer> boostFields = new HashMap<>();

        for (String key : getPropertiesToShow(lens)) {
            Map<String, Object> term = jsonLd.vocabIndex.get(key);
            if (term != null) {
                String termType = (String) term.get(TYPE_KEY);
                if ("ObjectProperty".equals(termType)) {
                    key = key + "." + SEARCH_KEY;
                }
                boostFields.put(key, boost);
            } else if (jsonLd.isLangContainer(jsonLd.context.get(key))) {
                boostFields.put(key + "." + jsonLd.locales.getFirst(), boost);
            }
        }

        return boostFields;
    }

    private static List<String> getPropertiesToShow(Map<?, ?> lens) {
        var properties = new LinkedHashSet<String>();

        for (var dfn : (List<?>) lens.get("showProperties")) {
            if (dfn instanceof String) {
                properties.add((String) dfn);
            } else if (dfn instanceof Map) {
                for (var ap : asList(((Map<?, ?>) dfn).get(ALTERNATE_PROPERTIES))) {
                    for (var prop : asList(ap)) {
                        if (prop instanceof String) {
                            properties.add((String) prop);
                        } else if (prop instanceof Map) {
                            var subPropertyOf = ((Map<?, ?>) prop).get(SUB_PROPERTY_OF);
                            if (subPropertyOf != null) {
                                properties.add((String) subPropertyOf);
                            }
                        }
                    }
                }
            }
        }

        return properties.stream().toList();
    }

    private Map<String, Integer> computeCardPropertyBoosts(String prop) {
        Map<String, Integer> boostFields = new HashMap<>();

        Map<String, Object> dfn = jsonLd.vocabIndex.get(prop);

        // Follow the object property range to append chip properties to the boosted path.
        if (dfn != null) {
            if ("ObjectProperty".equals(dfn.get(TYPE_KEY))) {
                Optional<String> rangeKey = Optional.ofNullable(dfn.get(RANGE))
                        .map(r -> r instanceof List ? ((List<?>) r).getFirst() : r)
                        .map(Map.class::cast)
                        .map(r -> (String) r.get(ID_KEY))
                        .map(jsonLd::toTermKey);
                if (rangeKey.isPresent() && jsonLd.isSubClassOf(rangeKey.get(), "QualifiedRole")) {
                    var rangeChipLens = chipLenses.getLensForType(rangeKey.get());
                    collectBoostFields(rangeChipLens, CARD_BOOST).forEach((k, v) -> boostFields.put(prop + "." + k, v));
                    return boostFields;
                }
                boostFields.put(prop + "." + SEARCH_KEY, CARD_BOOST);
            } else {
                boostFields.put(prop, CARD_BOOST);
            }
        } else if (jsonLd.isLangContainer(jsonLd.context.get(prop))) {
            boostFields.put(prop + "." + jsonLd.locales.getFirst(), CARD_BOOST);
        }

        return boostFields;
    }

    private static Map<String, Object> recordsOverCacheRecordsBoost() {
        var recordType = JsonLd.RECORD_KEY + '.' + JsonLd.TYPE_KEY;

        var recordBoost = Map.of(
                "constant_score", Map.of(
                        "filter", Map.of("term", Map.of(recordType, RECORD_TYPE)),
                        "boost", 1000)
        );
        var virtualRecordBoost = Map.of(
                "constant_score", Map.of(
                        "filter", Map.of("term", Map.of(recordType, VIRTUAL_RECORD_TYPE)),
                        "boost", 1000)
        );
        var cacheRecordBoost = Map.of(
                "constant_score", Map.of(
                        "filter", Map.of("term", Map.of(recordType, CACHE_RECORD_TYPE)),
                        "boost", 1)
        );

        return shouldWrap(List.of(recordBoost, virtualRecordBoost, cacheRecordBoost));
    }

    private static final List<String> CONCEPT_BOOST = List.of(
            "prefLabel^1500",
            "prefLabelByLang.sv^1500",
            "label^500",
            "labelByLang.sv^500",
            "code^200",
            "termComponentList._str.exact^125",
            "termComponentList._str^75",
            "altLabel^150",
            "altLabelByLang.sv^150",
            "hasVariant.prefLabel.exact^150",
            "_str.exact^100",
            "inScheme._str.exact^100",
            "inScheme._str^100",
            "inCollection._str.exact^10",
            "broader._str.exact^10",
            "exactMatch._str.exact^10",
            "closeMatch._str.exact^10",
            "broadMatch._str.exact^10",
            "related._str.exact^10",
            "scopeNote^10",
            "keyword._str.exact^10"
    );

    public sealed interface ScoreFunction permits FieldValueFactor, MatchingFieldValue {
        Map<String, Object> toEs();
        List<String> paramList();
    }

    public record FieldValueFactor(String field, float factor, String modifier, float missing, float weight) implements ScoreFunction {
        @Override
        public Map<String, Object> toEs() {
            return Map.of(
                    "field_value_factor", Map.of(
                            "field", field,
                            "factor", factor,
                            "modifier", modifier,
                            "missing", missing),
                    "weight", weight);
        }

        @Override
        public List<String> paramList() {
            return List.of("fvf", field, Float.toString(factor), modifier, Float.toString(missing), Float.toString(weight));
        }
    }

    public record MatchingFieldValue(String field, String value, float boost) implements ScoreFunction {
        @Override
        public Map<String, Object> toEs() {
            return Map.of("script_score", Map.of(
                            "script", String.format("doc['%s'].value == '%s' ? %f : 0", field, value, boost)));
        }

        @Override
        public List<String> paramList() {
            return List.of("mfv", field, value, Float.toString(boost));
        }
    }
}
