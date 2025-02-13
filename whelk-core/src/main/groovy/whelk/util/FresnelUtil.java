package whelk.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.JsonLd;
import whelk.JsonLd.Rdfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static whelk.util.FresnelUtil.LangCode.ORIGINAL_SCRIPT_FIRST;

// https://www.w3.org/2005/04/fresnel-info/manual/

// TODO fallback style for things that fall outside the class hierarchy?
// TODO defer language selection?
// TODO bad data - blank nodes without type?

public class FresnelUtil {

    public enum LensLevel {
        Full(List.of("full", "cards")),
        Card(List.of("cards")),
        Chip(List.of("chips")),
        Token(List.of("tokens", "chips"));

        final List<String> groups;

        LensLevel(List<String> groups) {
            this.groups = groups;
        }
    }

    // https://www.w3.org/2005/04/fresnel-info/manual/
    static class Fresnel {
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

    private static final Logger logger = LogManager.getLogger(FresnelUtil.class);

    //TODO load from display.jsonld
    private static final Map<?, ?> DEFAULT_LENS = Map.of(
            JsonLd.TYPE_KEY, Fresnel.Lens,
            Fresnel.showProperties, List.of(
                    Map.of(Fresnel.alternateProperties, List.of(
                            // TODO this is the expanded form with xByLang like in JsonLd
                            "prefLabel", "prefLabelByLang", "label", "labelByLang", "name", "nameByLang", "@id"
                    ))
            )
    );

    JsonLd jsonLd;
    Formats formats;

    FresnelUtil(JsonLd jsonLd) {
        this.jsonLd = jsonLd;
        this.formats = new Formats(getUnsafe(jsonLd.displayData, "formatters", null));
    }

    public Lensed applyLens(Object thing, LensLevel lens) {
        // TODO
        if (!isTypedNode(thing)) {
            throw new IllegalArgumentException("Thing is not typed node: " + thing);
        }

        return (Lensed) applyLens(thing, lens, null);
    }

    public Decorated format(Lensed lensed, LangCode locale) {
        var f = new Formatter(locale);
        return switch(lensed) {
            case Node n -> f.displayDecorate(n);
            case TransliteratedNode n -> f.displayDecorate(n);
        };
    }

    private Object applyLens(
            Object value,
            LensLevel lensLevel,
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
                n.add(script, (Node) applyLens(thing, lensLevel, script));
            }
            return n;
        }

        var result = new Node(lensLevel, selectedLang);
        var lens = findLens(thing, lensLevel);

        @SuppressWarnings("unchecked")
        var showProperties = (List<Object>) lens.get(Fresnel.showProperties);
        showProperties = Stream.concat(Stream.of(JsonLd.TYPE_KEY, JsonLd.ID_KEY), showProperties.stream()).toList();
        for (var p : showProperties) {
            if (JsonLd.isAlternateProperties(p)) {
                for (var alternative : alternatives(p)) {
                    if (JsonLd.isAlternateRangeRestriction(alternative)) {
                        // can never be language container
                        var r = asRangeRestriction(alternative);
                        var k = r.subPropertyOf;
                        @SuppressWarnings("unchecked")
                        var v = ((List<Object>) JsonLd.asList(thing.get(k)))
                                .stream()
                                .filter(n -> isTypedNode(n) && r.range.equals(asMap(n).get(JsonLd.TYPE_KEY)))
                                .toList();

                        if (!v.isEmpty()) {
                            result.pick(Map.of(k, v), new PropertyKey(k));
                            break;
                        }
                    }
                    else if (alternative instanceof List<?> list) {
                        // expanded lang alias, i.e. ["x", "xByLang"]
                        var found = false;
                        for (var k : list) {
                            if (has(thing, (String) k)) {
                                result.pick(thing, new PropertyKey((String) k));
                                found = true;
                            }
                        }
                        if (found) {
                            break;
                        }
                    }
                    else if (alternative instanceof String k) {
                        if (has(thing, k)) {
                            result.pick(thing, new PropertyKey(k));
                            break;
                        }
                    }
                }
            }
            else if (isInverseProperty(p)) {
                // never language container
                var i = asInverseProperty(p);
                if (thing.get(JsonLd.REVERSE_KEY) instanceof Map<?, ?> r && r.containsKey(i.name)) {
                    var v = r.get(i.name);
                    result.pick(Map.of(i.inverseName, v), new PropertyKey(i.inverseName));
                }
            }
            else if (p instanceof String k) {
                if (has(thing, k)) {
                    result.pick(thing, new PropertyKey(k));
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

    class PropertyKey {
        String name;

        public PropertyKey(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        boolean isLangAlias() {
            return jsonLd.langContainerAliasInverted.containsKey(name);
        }

        public String langAliasFor() {
            return (String) jsonLd.langContainerAliasInverted.get(name);
        }

        boolean isTypeVocabTerm() {
            return jsonLd.isVocabTerm(name);
        }
    }

    // FIXME naming
    public sealed abstract class Lensed permits Node, TransliteratedNode {}

    public final class Node extends Lensed {
        record Property(String name, Object value) {}

        LangCode selectedLang;
        String id;
        String type;
        List<Property> orderedProps = new ArrayList<>();
        LensLevel nextLensLevel;

        Node(LensLevel lensLevel, LangCode selectedLang) {
            this.nextLensLevel = subLens(lensLevel);
            this.selectedLang = selectedLang;
        }

        void pick(Map<?, ?> thing, PropertyKey p) {
            // TODO JsonLd class expands lang container aliases. Do we want that?

            if (Rdfs.RDF_TYPE.equals(p.name)) {
                var type = (String) first(thing.get(JsonLd.TYPE_KEY)); // TODO how to handle multiple types?
                orderedProps.add(new Property(p.name, mapVocabTerm(type)));
            }
            else if (!p.isLangAlias() && thing.containsKey(p.name)) {
                Object value = thing.get(p.name);
                if (JsonLd.TYPE_KEY.equals(p.name)) {
                    type = (String) first(value); // TODO how to handle multiple types?
                }
                else if (JsonLd.ID_KEY.equals(p.name)) {
                    id = (String) value;
                }
                else if (p.isTypeVocabTerm()) {
                    if (value instanceof List<?> list) {
                        orderedProps.add(new Property(p.name, list.stream().map(this::mapVocabTerm).toList()));
                    }
                    else {
                        orderedProps.add(new Property(p.name, mapVocabTerm(value)));
                    }
                }
                else {
                    if (value instanceof List<?> list) {
                        var values = list.stream().map(v -> applyLens(v, nextLensLevel, selectedLang)).toList();
                        orderedProps.add(new Property(p.name, values));
                    }
                    else {
                        orderedProps.add(new Property(p.name, applyLens(value, nextLensLevel, selectedLang)));
                    }
                }
            }
            else if (p.isLangAlias() && thing.containsKey(p.name)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> langContainer = (Map<String, Object>) thing.get(p.name);
                if (selectedLang != null) {
                    // TODO should we remember here that these are script alts?
                    if(langContainer.containsKey(selectedLang.code)) {
                        orderedProps.add(new Property(p.langAliasFor(), langContainer.get(selectedLang.code)));
                    }
                } else {
                    orderedProps.add(new Property(p.langAliasFor(), new LanguageContainer(langContainer)));
                }
            }
        }

        private Object mapVocabTerm(Object value) {
            if (value instanceof String s) {
                var def = jsonLd.vocabIndex.get(s);
                return applyLens(def != null ? def : s, nextLensLevel, selectedLang);
            } else {
                // bad data
                return applyLens(value, nextLensLevel, selectedLang);
            }
        }
    }

    public final class TransliteratedNode extends Lensed {
        Map<LangCode, Node> transliterations = new HashMap<>();
        void add(LangCode langCode, Node node) {
            transliterations.put(langCode, node);
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

        public boolean isTransliterated() {
            return languages.keySet().stream().anyMatch(LangCode::isTransliterated);
        }
    }

    private boolean has(Map<?, ?> thing, String key) {
        if (Rdfs.RDF_TYPE.equals(key)) {
            return thing.containsKey(JsonLd.TYPE_KEY);
        }
        return thing.containsKey(key);
    }

    private Map<?, ?> findLens(Map<?,?> thing, LensLevel lensLevel) {
        for (var groupName : lensLevel.groups) {
            @SuppressWarnings("unchecked")
            var group = ((Map<String, Map<String,Object>>) jsonLd.displayData.get("lensGroups")).get(groupName);
            @SuppressWarnings("unchecked")
            var lens = (Map<String, Object>) jsonLd.getLensFor(thing, group);
            if (lens != null) {
                return  lens;
            }
        }

        return DEFAULT_LENS;
    }

    private LensLevel subLens(LensLevel lensLevel) {
        return switch (lensLevel) {
            case Full -> LensLevel.Card;
            case Card -> LensLevel.Chip;
            case Chip, Token -> LensLevel.Token;
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> alternatives(Object alternateProperties) {
        return (List<Object>) ((Map<String, Object>) alternateProperties).get(JsonLd.ALTERNATE_PROPERTIES);
    }

    private record RangeRestriction(String subPropertyOf, String range) {}

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
                            && (new PropertyKey(s)).isLangAlias()
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
        return jsonLd.isSubClassOf((String) thing.get(JsonLd.TYPE_KEY), Base.StructuredValue);
    }

    private boolean isIdentity(Map<?,?> thing) {
        return jsonLd.isSubClassOf((String) thing.get(JsonLd.TYPE_KEY), Base.Identity);
    }

    private boolean isInverseProperty(Object showProperty) {
        return showProperty instanceof Map && ((Map<?, ?>) showProperty).containsKey("inverseOf");
    }

    private record InverseProperty(String name, String inverseName) {}

    private InverseProperty asInverseProperty(Object showProperty) {
        String p = (String) ((Map<?, ?>) showProperty).get("inverseOf");
        return new InverseProperty(p, jsonLd.getInverseProperty(p));
    }

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
                        : formatValues(pickLanguage(lang), className, propertyName);
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

        private List<String> pickLanguage(LanguageContainer value) {
            // TODO
            // TODO handle missing
            return value.languages.get(this.locale);
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

    private static <T, U> List<U> mapWithIndex(List<T> list, BiFunction<T, Integer, U> mapper) {
        var result = new ArrayList<U>(list.size());
        for (int ix = 0; ix < list.size(); ix++) {
            result.add(mapper.apply(list.get(ix), ix));
        }
        return result;
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

        // TODO refactor? JsonLd is only needed for displayType()
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
                scripts.get(codes.getFirst()).printTo(s);
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
            /*
            super.serialize(result);
            if (result.isEmpty()) {
                return unwrapSingle(value);
            } else {
                result.put(JsonLd.VALUE_KEY, unwrapSingle(value));
                return result;
            }

             */
            return result;
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
}
