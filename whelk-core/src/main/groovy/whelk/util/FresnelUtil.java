package whelk.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.JsonLd;
import whelk.JsonLd.Rdfs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static whelk.util.FresnelUtil.LangCode.ORIGINAL_SCRIPT_FIRST;

// https://www.w3.org/2005/04/fresnel-info/manual/

// TODO handle subPropertyOf
//   -- https://www.w3.org/2005/04/fresnel-info/fsl/#rdfsowl
//   -- https://github.com/libris/definitions/blob/41b0ac7b7089974dc1d1c41f221c038f1353df75/source/vocab/display.jsonld#L171
// TODO fallback style for things that fall outside the class hierarchy?
// TODO defer language selection?
// TODO bad data - blank nodes without type?

public class FresnelUtil {

    public enum LensGroupName {
        Full(List.of("full", "cards")),
        Card(List.of("cards")),
        Chip(List.of("chips")),
        Token(List.of("tokens", "chips")),

        SearchCard(List.of("search-cards", "cards")),
        SearchChip(List.of("search-chips", "chips"));

        final List<String> groups;

        LensGroupName(List<String> groups) {
            this.groups = groups;
        }
    }

    // https://www.w3.org/2005/04/fresnel-info/manual/
    public static class Fresnel {
        public static String Format = "fresnel:Format";
        public static String Group = "fresnel:Group";
        public static String Lens = "fresnel:Lens";
        public static String classFormatDomain = "fresnel:classFormatDomain";
        public static String contentAfter = "fresnel:contentAfter";
        public static String contentBefore = "fresnel:contentBefore";
        public static String contentFirst = "fresnel:contentFirst";
        public static String contentLast = "fresnel:contentLast";
        public static String extends_ = "fresnel:extends";
        public static String group = "fresnel:group";
        public static String propertyFormat = "fresnel:propertyFormat";
        public static String propertyFormatDomain = "fresnel:propertyFormatDomain";
        public static String propertyStyle = "fresnel:propertyStyle";
        public static String resourceFormat = "fresnel:resourceFormat";
        public static String resourceStyle = "fresnel:resourceStyle";
        public static String super_ = "fresnel:super";
        public static String valueFormat = "fresnel:valueFormat";
        public static String valueStyle = "fresnel:valueStyle";

        // these are used without prefix in display.jsonld
        // we currently we just treat everything as plain JSON keys there
        public static String showProperties = "showProperties";
        public static String alternateProperties = "alternateProperties";

        public static String WILD_PROPERTY = "*";
    }

    // https://github.com/libris/definitions/blob/develop/source/vocab/base.ttl
    static class Base {
        public static String Resource = "Resource";
        public static String StructuredValue = "StructuredValue";
        public static String Identity = "Identity";
    }

    public enum Options {
        DEFAULT,
        TAKE_ALL_ALTERNATE
    }

    private record DerivedCacheKey(Object types, DerivedLens lens) {}
    private record LensCacheKey(Object types, LensGroupName lensGroupName) {}

    private static final Logger logger = LogManager.getLogger(FresnelUtil.class);

    //TODO load from display.jsonld
    private static final Map<String, Object> DEFAULT_LENS = Map.of(
            JsonLd.TYPE_KEY, Fresnel.Lens,
            Fresnel.showProperties, List.of(
                    Map.of(Fresnel.alternateProperties, List.of(
                            // TODO this is the expanded form with xByLang like in JsonLd
                            "prefLabel", "prefLabelByLang", "label", "labelByLang", "name", "nameByLang", "@id"
                    ))
            )
    );

    JsonLd jsonLd;
    List<LangCode> fallbackLocales;
    Formats formats;

    private final Map<DerivedCacheKey, Lens> derivedLensCache = new ConcurrentHashMap<>();
    private final Map<LensCacheKey, Lens> lensCache = new ConcurrentHashMap<>();

    public FresnelUtil(JsonLd jsonLd) {
        this.jsonLd = jsonLd;
        this.fallbackLocales = jsonLd.locales.stream().map(LangCode::new).toList();
        this.formats = new Formats(getUnsafe(jsonLd.displayData, "formatters", null));
    }

    public Lensed applyLens(Object thing, LensGroupName lens) {
        return applyLens(thing, lens, Options.DEFAULT);
    }

    public Lensed applyLens(Object thing, LensGroupName lens, Options options) {
        // TODO
        if (!isTypedNode(thing)) {
            throw new IllegalArgumentException("Thing is not typed node: " + thing);
        }

        return (Lensed) applyLens(thing, lens, options, null);
    }

    public Lensed applyLens(Object thing, DerivedLens derived) {
        return applyLens(thing, derived, Options.DEFAULT);
    }

    public Lensed applyLens(Object thing, DerivedLens derived, Options options) {
        // TODO
        if (!(thing instanceof Map<?, ?> t)) {
            throw new IllegalArgumentException("Thing is not typed node: " + thing);
        }

        var types = t.get(JsonLd.TYPE_KEY);
        var lens = derivedLensCache.computeIfAbsent(new DerivedCacheKey(types, derived), k -> {
            var base = findLens(t, derived.base);
            var minus = derived.minus.stream().map(l -> findLens(t, l)).toList();
            return base.minus(minus, derived.subLens);
        });

        return (Lensed) applyLens(thing, lens, options, null);
    }

    public Decorated format(Lensed lensed, LangCode locale) {
        var f = new Formatter(locale);
        return switch(lensed) {
            case Node n -> f.displayDecorate(n);
            case TransliteratedNode n -> f.displayDecorate(n);
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void insertComputedLabels(Object data, LangCode locale) {
        DocumentUtil.traverse(data, (var value, var path) -> {
            if (value instanceof Map node && node.containsKey(JsonLd.TYPE_KEY)) {
                try {
                    var label = format(applyLens(value, FresnelUtil.LensGroupName.Chip), locale).asString();
                    node.put(JsonLd.Platform.COMPUTED_LABEL, label);
                    // TODO Check if structured value and don't compute for sub-nodes?
                } catch (Exception e) {
                    logger.warn("Error computing label for {}: {}", data, e, e);
                }
            }

            return DocumentUtil.NOP;
        });
    }

    private Object applyLens(
            Object value,
            LensGroupName lensGroupName,
            Options options,
            LangCode selectedLang
    ) {
        if (!(value instanceof Map<?, ?> thing)) {
            // literal
            return value;
        }
        var lens = findLens(thing, lensGroupName);

        return applyLens(value, lens, options, selectedLang);
    }

    private Object applyLens(
            Object value,
            Lens lens,
            Options options,
            LangCode selectedLang
    ) {
        if (!(value instanceof Map<?, ?> thing)) {
            // literal
            return value;
        }

        List<LangCode> scripts = selectedLang == null ? scriptAlternatives(thing) : Collections.emptyList();
        if (!scripts.isEmpty()) {
            TransliteratedNode n = new TransliteratedNode();
            for (var script : scripts) {
                n.add(script, (Node) applyLens(thing, lens, options, script));
            }
            return n;
        }

        var result = new Node(lens, options, selectedLang);

        var showProperties = Stream.concat(Stream.of(new PropertyKey(JsonLd.TYPE_KEY), new PropertyKey(JsonLd.ID_KEY)), lens.showProperties().stream()).toList();
        for (var p : showProperties) {
            switch (p) {
                case PropertyKey k -> {
                    if (k.isIn(thing)) {
                        result.pick(thing, k);
                    }
                }
                case AlternateProperties a -> {
                    alt: for (var alternative : a.alternatives) {
                        switch (alternative) {
                            case PropertyKey k -> {
                                if (k.isIn(thing)) {
                                    result.pick(thing, k);
                                    if (options != Options.TAKE_ALL_ALTERNATE) {
                                        break alt;
                                    }
                                }
                            }
                            case RangeRestriction r -> {
                                // can never be language container
                                var k = r.subPropertyOf;
                                @SuppressWarnings("unchecked")
                                var v = ((List<Object>) JsonLd.asList(thing.get(k)))
                                        .stream()
                                        .filter(n -> isTypedNode(n) && r.range.equals(asMap(n).get(JsonLd.TYPE_KEY)))
                                        .toList();

                                if (!v.isEmpty()) {
                                    result.pick(Map.of(k, v), new PropertyKey(k));
                                    if (options != Options.TAKE_ALL_ALTERNATE) {
                                        break alt;
                                    }
                                }
                            }
                            default -> {

                            }
                        }
                    }
                }
                case InverseProperty i -> {
                    // never language container
                    if (thing.get(JsonLd.REVERSE_KEY) instanceof Map<?, ?> r && r.containsKey(i.name)) {
                        var v = r.get(i.name);
                        result.pick(Map.of(i.inverseName, v), new PropertyKey(i.inverseName));
                    }
                }
                case RangeRestriction ignored -> {
                    // Not allowed here currently
                }
                case Unrecognized ignored -> {

                }
            }
        }

        return result;
    }

    static class Format {
        static final Format DEFAULT_FORMAT = new Format();

        String id;
        String group;
        List<String> classFormatDomain;
        List<String> propertyFormatDomain;
        FormatDetails resourceFormat;
        FormatDetails propertyFormat;
        FormatDetails valueFormat;
        Style resourceStyle;
        Style propertyStyle;
        Style valueStyle;

        private Format() {
            classFormatDomain = Collections.emptyList();
            propertyFormatDomain = Collections.emptyList();
            var emptyDetails = new FormatDetails(null, null, null, null);
            resourceFormat = emptyDetails;
            propertyFormat = emptyDetails;
            valueFormat = emptyDetails;
            var emptyStyle = new Style(Collections.emptyList());
            resourceStyle = emptyStyle;
            propertyStyle = emptyStyle;
            valueStyle = emptyStyle;
        }

        public static Format parse(Map<?,?> f) {
            Format format = new Format();
            format.classFormatDomain = getUnsafe(f, Fresnel.classFormatDomain, Collections.emptyList());
            format.propertyFormatDomain = getUnsafe(f, Fresnel.propertyFormatDomain, Collections.emptyList());
            format.resourceFormat = FormatDetails.parse(getUnsafe(f, Fresnel.resourceFormat, null));
            format.propertyFormat = FormatDetails.parse(getUnsafe(f, Fresnel.propertyFormat, null));
            format.valueFormat = FormatDetails.parse(getUnsafe(f, Fresnel.valueFormat, null));
            format.resourceStyle = Style.parse(getUnsafe(f, Fresnel.resourceStyle, null));
            format.propertyStyle = Style.parse(getUnsafe(f, Fresnel.propertyStyle, null));
            format.valueStyle = Style.parse(getUnsafe(f, Fresnel.valueStyle, null));
            return format;
        }
    }

    final class PropertyKey implements PropertySelector {
        String name;

        public PropertyKey(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        boolean hasLangAlias() {
            return jsonLd.langContainerAlias.containsKey(name);
        }

        public String langAlias() {
            return (String) jsonLd.langContainerAlias.get(name);
        }

        boolean isTypeVocabTerm() {
            return jsonLd.isVocabTerm(name);
        }

        private boolean isIn(Map<?, ?> thing) {
            if (Rdfs.RDF_TYPE.equals(name)) {
                return thing.containsKey(JsonLd.TYPE_KEY);
            }
            return thing.containsKey(name) || (hasLangAlias() && thing.containsKey(langAlias()));
        }

        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();
            s.append(name);
            if (hasLangAlias()) {
                s.append(" (").append(langAlias()).append(")");
            }
            if (isTypeVocabTerm()) {
                s.append(" (vocab)");
            }
            return s.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            PropertyKey that = (PropertyKey) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }
    }

    // FIXME naming
    public sealed abstract class Lensed permits Node, TransliteratedNode {
        public String asString() {
            return printTo(new StringBuilder()).toString();
        }

        public abstract Lensed firstProperty();

        // FIXME: Temporary method for experimenting with indexing of _topChipStr field
        public abstract Lensed tmpFirstProperty();

        protected abstract StringBuilder printTo(StringBuilder s);
    }

    public final class Node extends Lensed {
        record Property(String name, Object value) {}

        Options options;
        LangCode selectedLang;
        String id;
        String type;
        List<Property> orderedProps = new ArrayList<>();
        Lens lens;

        Node(Lens lens, Options options, LangCode selectedLang) {
            this.lens = lens;
            this.options = options;
            this.selectedLang = selectedLang;
        }

        void pick(Map<?, ?> thing, PropertyKey p) {
            // TODO JsonLd class expands lang container aliases. Do we want that?

            if (Rdfs.RDF_TYPE.equals(p.name)) {
                var type = firstType(thing); // TODO how to handle multiple types?
                orderedProps.add(new Property(p.name, mapVocabTerm(type)));
                return;
            }

            if (thing.containsKey(p.name)) {
                Object value = thing.get(p.name);
                if (JsonLd.TYPE_KEY.equals(p.name)) {
                    type = (String) first(value); // TODO how to handle multiple types?
                }
                else if (JsonLd.ID_KEY.equals(p.name)) {
                    id = (String) value;
                }
                else if (p.isTypeVocabTerm()) {
                    if (value instanceof List<?> list) {
                        var values = list.stream().map(this::mapVocabTerm).toList();
                        orderedProps.add(new Property(p.name, values));
                    }
                    else {
                        orderedProps.add(new Property(p.name, mapVocabTerm(value)));
                    }
                }
                else {
                    if (value instanceof List<?> list) {
                        var values = list.stream().map(v -> applyLens(v, lens.subLensGroup, options, selectedLang)).toList();
                        orderedProps.add(new Property(p.name, values));
                    }
                    else {
                        orderedProps.add(new Property(p.name, applyLens(value, lens.subLensGroup, options, selectedLang)));
                    }
                }
            }
            if (p.hasLangAlias() && thing.containsKey(p.langAlias())) {
                @SuppressWarnings("unchecked")
                Map<String, Object> langContainer = (Map<String, Object>) thing.get(p.langAlias());
                if (selectedLang != null) {
                    // TODO should we remember here that these are script alts?
                    if(langContainer.containsKey(selectedLang.code)) {
                        orderedProps.add(new Property(p.name(), langContainer.get(selectedLang.code)));
                    }
                } else {
                    orderedProps.add(new Property(p.name(), new LanguageContainer(langContainer)));
                }
            }
        }

        @Override
        public Node firstProperty() {
            var result = new Node(lens, options, selectedLang);
            result.id = id;
            result.type = type;
            result.orderedProps = orderedProps.isEmpty()
                    ? Collections.emptyList()
                    : List.of(orderedProps.getFirst());
            return result;
        }

        @Override
        public Node tmpFirstProperty() {
            var result = new Node(lens, options, selectedLang);
            result.id = id;
            result.type = type;
            if (orderedProps.isEmpty()) {
                result.orderedProps = Collections.emptyList();
            } else {
                Node.Property first = orderedProps.getFirst();
                if (first.name().equals("hasTitle")) {
                    (first.value() instanceof Collection<?> c ? c : List.of(first.value()))
                            .stream()
                            .filter(Node.class::isInstance)
                            .map(Node.class::cast)
                            .forEach(n ->
                                    n.orderedProps = n.orderedProps
                                            .stream()
                                            .filter(p -> p.name().equals("mainTitle"))
                                            .toList()
                            );
                }
                result.orderedProps = List.of(first);
            }
            return result;
        }

        @Override
        protected StringBuilder printTo(StringBuilder s) {
            orderedProps.forEach(prop -> printTo(s, prop.value));
            return s;
        }

        private void printTo(StringBuilder s, Object value) {
            if (value == null) {
                return;
            }
            if(!s.isEmpty() && s.charAt(s.length() - 1) != ' ') {
                s.append(" ");
            }
            switch(value) {
                case Collection<?> l -> l.forEach(v -> printTo(s, v));
                case LanguageContainer l -> l.languages.values().forEach(v -> printTo(s, v));
                case Lensed l -> l.printTo(s);
                default -> s.append(value);
            }
        }

        private Object mapVocabTerm(Object value) {
            if (value instanceof String s) {
                var def = jsonLd.vocabIndex.get(s);
                return applyLens(def != null ? def : s, lens.subLensGroup, options, selectedLang);
            } else {
                // bad data
                return applyLens(value, lens.subLensGroup, options, selectedLang);
            }
        }
    }

    public final class TransliteratedNode extends Lensed {
        Map<LangCode, Node> transliterations = new HashMap<>();
        void add(LangCode langCode, Node node) {
            transliterations.put(langCode, node);
        }

        @Override
        public Lensed firstProperty() {
            var result = new TransliteratedNode();
            transliterations.forEach((langCode, node) -> {
                result.transliterations.put(langCode, node.firstProperty());
            });
            return result;
        }

        @Override
        public Lensed tmpFirstProperty() {
            var result = new TransliteratedNode();
            transliterations.forEach((langCode, node) -> {
                result.transliterations.put(langCode, node.tmpFirstProperty());
            });
            return result;
        }

        @Override
        protected StringBuilder printTo(StringBuilder s) {
            if(!s.isEmpty() && s.charAt(s.length() - 1) != ' ') {
                s.append(" ");
            }
            transliterations.values().forEach(node -> node.printTo(s));
            return s;
        }
    }

    public static class LanguageContainer {
        Map<LangCode, List<String>> languages = new HashMap<>();

        public LanguageContainer(Map<?, ?> container) {
            for (var e : container.entrySet()) {
                @SuppressWarnings("unchecked")
                List<Object> l = JsonLd.asList(e.getValue());
                languages.put(new LangCode((String) e.getKey()), mapWithIndex(l, (v, i) -> (String) v));
            }
        }

        public List<String> pick(LangCode langCode, List<LangCode> fallbackLocales) {
            if (languages.containsKey(langCode)) {
                return languages.get(langCode);
            }

            for (LangCode fallbackLocale : fallbackLocales) {
                if (languages.containsKey(fallbackLocale)) {
                    return languages.get(fallbackLocale);
                }
            }

            var randomLang = languages.keySet().stream().findFirst();
            return randomLang.map(languages::get).orElse(Collections.emptyList());
        }

        public boolean isTransliterated() {
            return languages.keySet().stream().anyMatch(LangCode::isTransliterated);
        }
    }

    private Lens findLens(Map<?,?> thing, LensGroupName lensGroupName) {
        var types = thing.get(JsonLd.TYPE_KEY);
        var cacheKey = new LensCacheKey(types, lensGroupName);

        return lensCache.computeIfAbsent(cacheKey, k -> {
            for (var groupName : lensGroupName.groups) {
                @SuppressWarnings("unchecked")
                var group = ((Map<String, Map<String,Object>>) jsonLd.displayData.get("lensGroups")).get(groupName);
                @SuppressWarnings("unchecked")
                var lens = (Map<String, Object>) jsonLd.getLensFor(thing, group);
                if (lens != null) {
                    return new Lens(lens, subLens(lensGroupName));
                }
            }

            return new Lens(DEFAULT_LENS, subLens(lensGroupName));
        });
    }

    public class Lens {
        private final LensGroupName subLensGroup;
        private final List<PropertySelector> showProperties;

        public Lens(Map<String, Object> lensDefinition, LensGroupName subLensGroup) {
            this.subLensGroup = subLensGroup;

            @SuppressWarnings("unchecked")
            var showProperties = (List<Object>) lensDefinition.get(Fresnel.showProperties);
            this.showProperties = parseShowProperties(showProperties);
        }

        private Lens(List<PropertySelector> showProperties, LensGroupName subLensGroup) {
            this.showProperties = showProperties;
            this.subLensGroup = subLensGroup;
        }

        List<PropertySelector> showProperties() {
            return showProperties;
        }

        private List<PropertySelector> parseShowProperties(List<Object> showProperties) {
            return showProperties.stream().map(p -> {
                if (JsonLd.isAlternateProperties(p)) {
                    return new AlternateProperties(alternatives(p));
                }
                if (p instanceof List<?> list) {
                    // expanded lang alias, i.e. ["x", "xByLang"] inside alternateProperties
                    // TODO remove expansion in jsonLd?
                    if (list.size() == 2) {
                        return new PropertyKey((String) list.getFirst());
                    }
                }
                if (isInverseProperty(p)) {
                    return asInverseProperty(p);
                }
                if (JsonLd.isAlternateRangeRestriction(p)) {
                    return asRangeRestriction(p);
                }
                if (p instanceof String k) {
                    // ignore langContainer aliases expanded by jsonld
                    if (!jsonLd.langContainerAliasInverted.containsKey(k)) {
                        return new PropertyKey(k);
                    }
                }
                return new Unrecognized();
            }).toList();
        }

        @SuppressWarnings("unchecked")
        private List<PropertySelector> alternatives(Object alternateProperties) {
            var alternatives = (List<Object>) ((Map<String, Object>) alternateProperties).get(JsonLd.ALTERNATE_PROPERTIES);
            return parseShowProperties(alternatives);
        }

        private boolean isInverseProperty(Object showProperty) {
            return showProperty instanceof Map && ((Map<?, ?>) showProperty).containsKey("inverseOf");
        }

        private InverseProperty asInverseProperty(Object showProperty) {
            String p = (String) ((Map<?, ?>) showProperty).get("inverseOf");
            return new InverseProperty(p, jsonLd.getInverseProperty(p));
        }

        // TODO
        Lens minus(Collection<Lens> minus, LensGroupName subLens) {
            var keep = new ArrayList<>(showProperties);

            for (var m : minus) {
                keep.removeAll(m.showProperties);
            }

            return new Lens(keep, subLens);
        }
    }

    public record DerivedLens(LensGroupName base, List<LensGroupName> minus, LensGroupName subLens) {

    }

    private static LensGroupName subLens(LensGroupName lensGroupName) {
        return switch (lensGroupName) {
            case Full -> LensGroupName.Card;
            case Card -> LensGroupName.Chip;
            case Chip, Token -> LensGroupName.Token;

            case SearchCard -> LensGroupName.SearchChip;
            case SearchChip -> LensGroupName.Token; // TODO ??
        };
    }

    private sealed interface PropertySelector permits PropertyKey, InverseProperty, AlternateProperties, RangeRestriction, Unrecognized {

    }

    private record AlternateProperties(List<PropertySelector> alternatives) implements PropertySelector {

    }

    private record Unrecognized() implements PropertySelector {

    }

    private record RangeRestriction(String subPropertyOf, String range) implements PropertySelector {}

    @SuppressWarnings("unchecked")
    private RangeRestriction asRangeRestriction(Object o) {
        Map<String, String> r = (Map<String, String>) o;
        return new RangeRestriction(r.get(Rdfs.SUB_PROPERTY_OF), r.get(Rdfs.RANGE));
    }

    private List<LangCode> scriptAlternatives(Map<?,?> thing) {
        var shouldBeGrouped = isTypedNode(thing) && (isStructuredValue(thing) || isIdentity(thing));

        if (shouldBeGrouped) {
            Set<String> codes = new HashSet<>();
            thing.entrySet().stream()
                    .filter(e -> e.getKey() instanceof String s
                            && jsonLd.langContainerAliasInverted.containsKey(s)
                            && (new LanguageContainer(asMap(e.getValue()))).isTransliterated()
                    )
                    .forEach(e ->
                            asMap(e.getValue()).keySet().forEach(k -> codes.add((String) k))
                    );
            return codes.stream().map(LangCode::new).toList();
        }
        // TODO
        return Collections.emptyList();
    }

    private boolean isStructuredValue(Map<?,?> thing) {
        return jsonLd.isSubClassOf(firstType(thing), Base.StructuredValue);
    }

    private boolean isIdentity(Map<?,?> thing) {
        return jsonLd.isSubClassOf(firstType(thing), Base.Identity);
    }



    private record InverseProperty(String name, String inverseName) implements PropertySelector {}

    public record LangCode(String code) {
        public static final Comparator<LangCode> ORIGINAL_SCRIPT_FIRST = (a, b) -> {
            if ((a.isTransliterated() && b.isTransliterated()) || (!a.isTransliterated() && !b.isTransliterated())) {
                return a.code.compareTo(b.code);
            } else if (a.isTransliterated()) {
                return 1;
            } else {
                return -1;
            }
        };

        public boolean isTransliterated() {
             return code.contains("-t-");
        }
    }

    private boolean isTypedNode(Object o) {
        return o instanceof Map && ((Map<?, ?>) o).containsKey(JsonLd.TYPE_KEY);
    }

    // TODO handle multiple types=
    private String firstType(Map<?,?> thing) {
        var types = thing.get(JsonLd.TYPE_KEY);
        if (types instanceof String) {
            return (String) types;
        }
        if (types instanceof List<?> l) {
            return (String) l.getFirst();
        }
        throw new RuntimeException("Expected type/types, found " + types);
    }

    private class Formatter {
        LangCode locale;

        Formatter (LangCode locale) {
            this.locale = locale;
        }

        public Decorated displayDecorate(Node node) {
            return formatResource(node, false, false);
        }

        public Decorated displayDecorate(TransliteratedNode node) {
            return formatTransliterated(node, false, false);
        }

        private Decorated formatResource(Node node, boolean isFirst, boolean isLast) {
            var result = new DecoratedNode();
            result.type = node.type;
            result.id = node.id;
            result.fallback = node.id != null ? jsonLd.toTermKey(node.id) : ""; // TODO
            result.display = formatProperties(node.orderedProps, node.type);
            formats.resourceStyle(node.type).apply(result);
            formats.resourceDetails(node.type).apply(result, isFirst, isLast);

            return result;
        }

        private Decorated formatTransliterated(TransliteratedNode node, boolean isFirst, boolean isLast) {
            var lang = new HashMap<LangCode, Decorated>();
            for (var e : node.transliterations.entrySet()) {
                lang.put(e.getKey(), formatResource(e.getValue(), isFirst, isLast));
            }
            return new DecoratedTransliterated(lang);
        }

        private List<Decorated> formatProperties(List<Node.Property> orderedProps, String className) {
            return mapWithIndex(orderedProps,
                    (p, ix) -> formatProperty(p, className, ix == 0, ix == orderedProps.size() - 1)
            );
        }

        private Decorated formatProperty(Node.Property property, String className, boolean isFirst, boolean isLast) {
            var result = new DecoratedProperty(property.name, formatValues(property.value, className, property.name));

            formats.propertyStyle(className, property.name).apply(result);
            // TODO implement _style sort() ?
            formats.propertyDetails(className, property.name).apply(result, isFirst, isLast);
            return result;
        }

        private List<Decorated> formatValues(Object value, String className, String propertyName) {
            var val = unwrapSingle(value);
            if (val instanceof LanguageContainer lang) {
                // can never be inside array
                return lang.isTransliterated()
                        ? List.of(new DecoratedTransliterated(lang))
                        : formatValues(lang.pick(locale, fallbackLocales), className, propertyName);
            }
            else if (val instanceof List<?> list) {
                return mapWithIndex(list,
                        (v, ix) -> formatValueInArray(v, className, propertyName, ix == 0, ix == list.size() - 1));
            } else {
                return List.of(formatSingleValue(val, className, propertyName));
            }
        }

        private Decorated formatSingleValue(Object value, String className, String propertyName) {
            switch (value) {
                case Node node -> {
                    var isFirst = true;
                    var isLast = true;
                    var result = formatResource(node, isFirst, isLast);
                    // TODO resource style vs value style???
                    formats.valueDetails(className, propertyName).apply(result, isFirst, isLast);
                    return result;
                }
                case TransliteratedNode node -> {
                    var isFirst = true;
                    var isLast = true;
                    var result = formatTransliterated(node, isFirst, isLast);
                    // TODO resource style vs value style???
                    formats.valueDetails(className, propertyName).apply(result, isFirst, isLast);
                    return result;
                }
                default -> {
                    var result = new DecoratedLiteral(value);
                    formats.valueStyle(className, propertyName).apply(result);
                    return result;
                }
            }


        }

        private Decorated formatValueInArray(Object value,
                                             String className,
                                             String propertyName,
                                             boolean isFirst,
                                             boolean isLast) {
            Decorated result = switch (value) {
                case Node node -> formatResource(node, isFirst, isLast);
                case TransliteratedNode node -> formatTransliterated(node, isFirst, isLast);
                default -> {
                    var r = new DecoratedLiteral(value);
                    formats.valueStyle(className, propertyName).apply(r);
                    yield r;
                }
            };
            formats.valueDetails(className, propertyName).apply(result, isFirst, isLast);
            return result;
        }
    }

    class Formats {
        Map<String, Format> formatIndex = new HashMap<>();

        public Formats (Map<String, Map<?, ?>> formats) {
            parse(formats);
        }

        private void parse(Map<String, Map<?,?>> formats) {
            if (formats == null) {
                return;
            }

            for (var fd : formats.entrySet()) {
                var f = fd.getValue();
                if (!fd.getKey().equals(f.get(JsonLd.ID_KEY))) {
                    logger.warn("Mismatch in format id: {} {}", fd.getKey(), f.get(JsonLd.ID_KEY));
                }
                if (!Fresnel.Format.equals(f.get(JsonLd.TYPE_KEY))) {
                    logger.warn("Unknown type, skipping {}", f.get(JsonLd.TYPE_KEY));
                    continue;
                }
                var format = Format.parse(f);
                if (!format.classFormatDomain.isEmpty() && !format.propertyFormatDomain.isEmpty()) {
                    for (var cls : format.classFormatDomain) {
                        for (var p : format.propertyFormatDomain) {
                            formatIndex.put(cls + "/" + p, format);
                        }
                    }
                } else if (!format.classFormatDomain.isEmpty()) {
                    for (var cls : format.classFormatDomain) {
                        formatIndex.put(cls, format);
                    }
                } else if (!format.propertyFormatDomain.isEmpty()) {
                    for (var p : format.propertyFormatDomain) {
                        formatIndex.put(p, format);
                    }
                }
            }
        }

        FormatDetails resourceDetails(String className) {
            return findFormat(className, f -> f.resourceFormat != null).resourceFormat;
        }

        FormatDetails propertyDetails(String className, String propertyName) {
            return findFormat(className, propertyName, f -> f.propertyFormat != null).propertyFormat;
        }

        FormatDetails valueDetails(String className, String propertyName) {
            return findFormat(className, propertyName, f -> f.valueFormat != null).valueFormat;
        }

        Style resourceStyle(String className) {
            return findFormat(className, f -> f.resourceStyle != null).resourceStyle;
        }

        Style propertyStyle(String className, String propertyName) {
            return findFormat(className, propertyName, f -> f.propertyStyle != null).propertyStyle;
        }

        Style valueStyle(String className, String propertyName) {
            return findFormat(className, propertyName, f -> f.valueStyle != null).valueStyle;
        }

        private Format findFormat(String className, Predicate<Format> predicate) {
            // TODO precompute / memoize formats for base classes ?
            List<String> classes = new ArrayList<>();
            classes.add(className);
            jsonLd.getSuperClasses(className, classes);
            for (String cls : classes) {
                if (formatIndex.containsKey(cls) && predicate.test(formatIndex.get(cls))) {
                    return formatIndex.get(cls);
                }
            }
            return Format.DEFAULT_FORMAT;
        }

        private Format findFormat(String className, String propertyName, Predicate<Format> predicate) {
            List<String> classes = new ArrayList<>();
            classes.add(className);
            jsonLd.getSuperClasses(className, classes);
            for (String cls : classes) {
                var ix = cls + "/" + propertyName;
                if (formatIndex.containsKey(ix) && predicate.test(formatIndex.get(ix))) {
                    return formatIndex.get(ix);
                }
            }
            if (formatIndex.containsKey(propertyName) && predicate.test(formatIndex.get(propertyName))) {
                return formatIndex.get(propertyName);
            }
            for (String cls : classes) {
                var ix = cls + "/" + Fresnel.WILD_PROPERTY;
                if (formatIndex.containsKey(ix) && predicate.test(formatIndex.get(ix))) {
                    return formatIndex.get(ix);
                }
            }
            return Format.DEFAULT_FORMAT;
        }
    }

    record FormatDetails(String contentBefore, String contentAfter, String contentFirst, String contentLast) {
        public void apply(Decorated result,
                          boolean isFirst,
                          boolean isLast) {
            if (isFirst && contentBefore != null) {
                if (!"".equals(contentFirst)) {
                    // TODO decide if we should generate contentBefore or contentFirst here
                    result.contentBefore = contentFirst;
                }
            } else if (contentBefore != null && !contentBefore.isEmpty()) {
                result.contentBefore = contentBefore;
            }

            if (isLast && contentLast != null) {
                if (!contentLast.isEmpty()) {
                    // TODO decide if we should generate contentAfter or contentLast here
                    result.contentAfter = contentLast;
                }
            } else if (contentAfter != null && !contentAfter.isEmpty()) {
                result.contentAfter = contentAfter;
            }
        }

        public static FormatDetails parse(Map<String, String> d) {
            if (d == null) {
                return null;
            }
            return new FormatDetails(
                    d.get(Fresnel.contentBefore),
                    d.get(Fresnel.contentAfter),
                    d.get(Fresnel.contentFirst),
                    d.get(Fresnel.contentLast)
            );
        }
    }

    record Style(List<String> styles) {
        public void apply(Decorated result) {
            var s = new ArrayList<String>();
            for (String style : styles) {
                //if (STYLERS.containsKey(style)) {
                if (style.endsWith("()")) {
                    applyFunction(result, style);
                } else {
                    s.add(style);
                }
            }
            result.style = s;
        }

        public static Style parse(List<String> styles) {
            if (styles == null) {
                return null;
            }
            return new Style(styles);
        }

        private void applyFunction(Decorated result, String name) {
            if (name.equals("isniGroupDigits()")) {
                if (result instanceof DecoratedLiteral literal) {
                    literal.value = switch (literal.value) {
                        case String s -> Unicode.formatIsni(s);
                        case List<?> list -> list.stream()
                                .filter(s -> s instanceof String)
                                .map(s -> Unicode.formatIsni((String) s))
                                .toList();
                        default -> literal.value;
                    };
                }
            }
        }
    }

    public sealed static abstract class Decorated permits DecoratedLiteral, DecoratedNode, DecoratedProperty, DecoratedTransliterated {
        public static class Fmt {
            public static String DISPLAY = "_display";
            public static String CONTENT_AFTER = "_contentAfter";
            public static String CONTENT_BEFORE = "_contentBefore";
            public static String STYLE = "_style";
            public static String LABEL = "_label";
            public static String SCRIPTS = "_scripts";
        }

        String contentBefore;
        String contentAfter;
        List<String> style;
        List<String> label; // TODO rename?

        int lastLen = 0;

        public String asString() {
            return printTo(new StringBuilder()).toString();
        }

        abstract StringBuilder printTo(StringBuilder s);

        protected void before(StringBuilder s) {
            lastLen = s.length();
            if (contentBefore != null) {
                s.append(contentBefore);
            }
        }

        protected void after(StringBuilder s) {
            if (s.length() == lastLen + (contentBefore == null ? 0 : contentBefore.length())) {
                s.setLength(lastLen); // produced empty string -> erase contentBefore
            } else if (contentAfter != null) {
                s.append(contentAfter);
            }
        }

        abstract Object asJsonLd();

        protected void asJsonLd(Map<String, Object> result) {
            if (contentBefore != null) {
                result.put(Fmt.CONTENT_BEFORE, contentBefore);
            }
            if (contentAfter != null) {
                result.put(Fmt.CONTENT_AFTER, contentAfter);
            }
            if (style != null && !style.isEmpty()) {
                result.put(Fmt.STYLE, style);
            }
            if (label != null && !label.isEmpty()) {
                result.put(Fmt.LABEL, label);
            }
        }
    }

    public final static class DecoratedNode extends Decorated {
        String type;
        String id;
        String fallback;

        List<Decorated> display;

        @Override
        StringBuilder printTo(StringBuilder s) {
            before(s);
            if (!display.isEmpty()) {
                display.forEach(decorated -> decorated.printTo(s));
            } else {
                s.append(fallback);
            }
            after(s);
            return s;
        }

        public Object asJsonLd() {
            var result = new HashMap<String, Object>();
            result.put(JsonLd.TYPE_KEY, type);
            if (id != null) {
                result.put(JsonLd.ID_KEY, id);
            }
            result.put(Fmt.DISPLAY, display.stream().map(Decorated::asJsonLd).toList());

            super.asJsonLd(result);
            return result;
        }
    }

    public final static class DecoratedProperty extends Decorated {
        String key;
        List<Decorated> values; // TODO handle single value separately?

        public DecoratedProperty(String key, List<Decorated> values) {
            this.key = key;
            this.values = values;
        }

        @Override
        StringBuilder printTo(StringBuilder s) {
            before(s);
            values.forEach(decorated -> decorated.printTo(s));
            after(s);
            return s;
        }

        public Object asJsonLd() {
            var result = new HashMap<String, Object>();
            result.put(key, unwrapSingle(values.stream().map(Decorated::asJsonLd).toList()));
            super.asJsonLd(result);
            return result;
        }

        @Override
        public String toString() {
            return "DecoratedProperty{" +
                    "key='" + key + '\'' +
                    ", values=" + values +
                    '}';
        }
    }

    public final static class DecoratedLiteral extends Decorated {
        Object value;

        public DecoratedLiteral(Object value) {
            this.value = value;
        }

        @Override
        StringBuilder printTo(StringBuilder s) {
            before(s);
            s.append(value);
            after(s);
            return s;
        }

        public Object asJsonLd() {
            var result = new HashMap<String, Object>();
            super.asJsonLd(result);
            if (result.isEmpty()) {
                return unwrapSingle(value);
            } else {
                result.put(JsonLd.VALUE_KEY, unwrapSingle(value));
                return result;
            }
        }
    }

    public final static class DecoratedTransliterated extends Decorated {
        // TODO
        private static final String T_START = " ’";
        private static final String T_END = "’";
        private static final String T_BETWEEN = ", ";

        Map<LangCode, Decorated> scripts;

        public DecoratedTransliterated(Map<LangCode, Decorated> scripts) {
            this.scripts = scripts;
        }

        public DecoratedTransliterated(LanguageContainer container) {
            // TODO order
            scripts = new HashMap<>();
            for (var e : container.languages.entrySet()) {
                // TODO MULTIPLE
                var v = new DecoratedLiteral(String.join(" ", e.getValue()));
                scripts.put(e.getKey(), v);
            }
        }

        @Override
        StringBuilder printTo(StringBuilder s) {
            var codes = scripts.keySet().stream().sorted(ORIGINAL_SCRIPT_FIRST).toList();
            before(s);
            if (!codes.isEmpty()) {
                var originalScript = scripts.get(codes.getFirst()).printTo(new StringBuilder()).toString();
                var isRtl = Unicode.guessScript(originalScript).map(Unicode::isRtl).orElse(false);
                if (isRtl) {
                    s.append(Unicode.RIGHT_TO_LEFT_ISOLATE);
                }
                s.append(originalScript);
                if (isRtl) {
                    s.append(Unicode.POP_DIRECTIONAL_ISOLATE);
                }
            }
            if (codes.size() > 1) {
                s.append(T_START);
                for (int i = 0 ; i < codes.size() ; i++) {
                    if (i > 0) {
                        scripts.get(codes.get(i)).printTo(s);
                        if (i < codes.size() - 2) {
                            s.append(T_BETWEEN);
                        }
                    }
                }
                s.append(T_END);
            }
            after(s);
            return s;
        }

        public Object asJsonLd() {
            var result = new HashMap<String, Object>();

            scripts.forEach((code, decorated) -> {
                result.put(code.code,  decorated.asJsonLd());
            });

            return Map.of(Fmt.SCRIPTS, result);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getUnsafe(Map<?, ?> m, Object key, T defaultTo) {
        return m.containsKey(key)
                ? (T) m.get(key)
                : defaultTo;
    }

    private static Object unwrapSingle(Object value) {
        if (value instanceof List<?> list && list.size() == 1) {
            return list.getFirst();
        }
        return value;
    }

    private static Object first(Object value) {
        if (value instanceof List<?> list) {
            if (!list.isEmpty()) {
                return list.getFirst();
            } else {
               return null;
            }
        }
        return value;
    }

    private static Map<?, ?> asMap(Object o) {
        if (o instanceof Map<?, ?> m) {
            return m;
        }
        return new HashMap<>();
    }

    private static <T, U> List<U> mapWithIndex(List<T> list, BiFunction<T, Integer, U> mapper) {
        var result = new ArrayList<U>(list.size());
        for (int ix = 0; ix < list.size(); ix++) {
            result.add(mapper.apply(list.get(ix), ix));
        }
        return result;
    }
}
