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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static whelk.util.FresnelUtil.LangCode.NO_LANG;
import static whelk.util.FresnelUtil.LangCode.ORIGINAL_SCRIPT_FIRST;
import static whelk.util.FresnelUtil.Options.NO_FALLBACK;
import static whelk.util.FresnelUtil.Options.TAKE_FIRST_SHOW_PROPERTY;

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
        SearchChip(List.of("search-chips", "chips")),
        SearchToken(List.of("search-tokens"));

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
        public static String fslselector = "fresnel:fslselector";
        public static String group = "fresnel:group";
        public static String mergeProperties = "fresnel:mergeProperties";
        public static String propertyFormat = "fresnel:propertyFormat";
        public static String propertyFormatDomain = "fresnel:propertyFormatDomain";
        public static String propertyStyle = "fresnel:propertyStyle";
        public static String resourceFormat = "fresnel:resourceFormat";
        public static String resourceStyle = "fresnel:resourceStyle";
        public static String super_ = "fresnel:super";
        public static String use = "fresnel:use";
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
        TAKE_ALL_ALTERNATE,
        TAKE_FIRST_SHOW_PROPERTY,
        NO_FALLBACK
    }

    private enum FallbackLens {
        //TODO load from display.jsonld
        DEFAULT(Map.of(
                JsonLd.TYPE_KEY, Fresnel.Lens,
                Fresnel.showProperties, List.of(
                        Map.of(Fresnel.alternateProperties, List.of(
                                // TODO this is the expanded form with xByLang like in JsonLd
                                "prefLabel", "prefLabelByLang", "label", "labelByLang", "name", "nameByLang", "@id"
                        ))
                )
        )),
        EMPTY(Map.of(
                JsonLd.TYPE_KEY, Fresnel.Lens,
                Fresnel.showProperties, List.of()
        ));

        private final Map<String, Object> lens;

        FallbackLens(Map<String, Object> lens) {
            this.lens = lens;
        }
    }

    private record DerivedCacheKey(Object types, DerivedLensGroup lens) {}
    private record LensCacheKey(Object types, LensGroupName lensGroupName, FallbackLens fallbackLens) {}

    private static final Logger logger = LogManager.getLogger(FresnelUtil.class);

    JsonLd jsonLd;
    List<LangCode> fallbackLocales;
    Formats formats;

    private final Map<DerivedCacheKey, Lens> derivedLensCache = new ConcurrentHashMap<>();
    private final Map<LensCacheKey, Lens> lensCache = new ConcurrentHashMap<>();

    private static final LensGroupName searchKeyLens = LensGroupName.SearchToken;

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

    public Lensed applyLens(Object thing, DerivedLensGroup derived) {
        return applyLens(thing, derived, Options.DEFAULT);
    }

    public Lensed applyLens(Object thing, DerivedLensGroup derived, Options options) {
        // TODO
        if (!(thing instanceof Map<?, ?> t)) {
            throw new IllegalArgumentException("Thing is not typed node: " + thing);
        }

        var types = t.get(JsonLd.TYPE_KEY);
        var derivedLens = derivedLensCache.computeIfAbsent(new DerivedCacheKey(types, derived), k -> {
            var base = findLens(t, derived.base);
            var minus = derived.minus.stream().map(l -> findLens(t, l)).toList();
            return base.minus(minus, derived);
        });

        return (Lensed) applyLens(thing, derivedLens, options, null);
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
                    var label = format(applyLens(value, LensGroupName.Chip), locale).asString();
                    node.put(JsonLd.Platform.COMPUTED_LABEL, label);
                    // TODO Check if structured value and don't compute for sub-nodes?
                } catch (Exception e) {
                    logger.warn("Error computing label for {}: {}", data, e, e);
                }
            }

            return DocumentUtil.NOP;
        });
    }

    public List<?> fslSelect(Map<?, ?> thing, String fslSelector) {
        return new FslPath(fslSelector).getValues(thing);
    }

    private Object applyLens(Object value, LensGroupName lensGroupName, LangCode selectedLang) {
        return applyLens(value, lensGroupName, Options.DEFAULT, selectedLang);
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
        var lens = findLens(thing, lensGroupName, options.equals(Options.NO_FALLBACK) ? FallbackLens.EMPTY : FallbackLens.DEFAULT);

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

        var result = new Node(lens, selectedLang);

        var showProperties = options == Options.TAKE_FIRST_SHOW_PROPERTY && !lens.showProperties().isEmpty()
                ? lens.showProperties().subList(0, 1)
                : lens.showProperties();

        Stream.concat(Stream.of(new FslPath(JsonLd.TYPE_KEY), new FslPath(JsonLd.ID_KEY)), showProperties.stream())
                .forEach(sp -> result.select(thing, sp, Options.TAKE_ALL_ALTERNATE.equals(options)));

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

    // FIXME naming
    public sealed abstract class Lensed permits Node, TransliteratedNode {
        public String asString() {
            return printTo(new StringBuilder()).toString();
        }

        public Map<String, String> byLang() {
            return asLangMap(byLang(new LinkedHashMap<>()));
        }

        public Map<String, String> byScript() {
            return asLangMap(byScript(new LinkedHashMap<>()));
        }

        public abstract Map<String, Object> getThingForIndex();

        public abstract boolean isEmpty();

        protected abstract StringBuilder printTo(StringBuilder s);

        protected abstract Map<LangCode, StringBuilder> byLang(Map<LangCode, StringBuilder> stringsByLang);

        protected abstract Map<LangCode, StringBuilder> byScript(Map<LangCode, StringBuilder> stringsByLang);

        private Map<String, String> asLangMap(Map<LangCode, StringBuilder> stringsByLang) {
            Map<String, String> result = new LinkedHashMap<>();
            stringsByLang.forEach((lang, s) -> {
                if (!lang.equals(NO_LANG)) {
                    result.put(lang.code(), s.toString());
                }
            });
            return result;
        }
    }

    public final class Node extends Lensed {
        record Selected(FslPath selector, Object value) {}

        LangCode selectedLang;
        String id;
        String type;
        List<Selected> orderedSelection = new ArrayList<>();
        Lens lens;

        Node(Lens lens, LangCode selectedLang) {
            this.lens = lens;
            this.selectedLang = selectedLang;
        }

        void select(Map<?, ?> thing, ShowProperty showProperty, boolean takeAllAlternate) {
            switch (showProperty) {
                case AlternateProperties a -> {
                    for (var alternative : a.alternatives()) {
                        if (alternative instanceof FslPath fslPath) {
                            boolean selected = select(thing, fslPath, takeAllAlternate);
                            if (selected && !takeAllAlternate) {
                                break;
                            }
                        }
                    }
                }
                case FslPath fslPath -> select(thing, fslPath, takeAllAlternate);
                case Unrecognized ignored -> {}
                case MergeProperties mergeProperties -> {
                    Node n = new Node(lens, selectedLang);
                    for (var m : mergeProperties.merge()) {
                        n.select(thing, m, takeAllAlternate);
                    }
                    var values = n.orderedSelection.stream().map(Selected::value).flatMap(this::asStream).toList();
                    if (!values.isEmpty()) {
                        this.orderedSelection.add(new Selected(mergeProperties.use(), values));
                    }
                }
            }
        }

        private boolean select(Map<?, ?> thing, FslPath fslPath, boolean takeAllAlternate) {
            PropertyKey p = fslPath.getEndArcStep().asPropertyKey();
            List<?> values = fslPath.getValues(thing);

            if (values.isEmpty()) {
                return false;
            }

            if (Rdfs.RDF_TYPE.equals(p.name)) {
                var type = (String) values.getFirst(); // TODO how to handle multiple types?
                orderedSelection.add(new Selected(fslPath, mapVocabTerm(type)));
            }
            else if (JsonLd.TYPE_KEY.equals(p.name)) {
                if (!fslPath.isArcOnly()) {
                    // TODO: What should be set as the type when we have a path that traverses several nodes?
                    throw new RuntimeException("");
                }
                type = (String) values.getFirst(); // TODO how to handle multiple types?
            }
            else if (JsonLd.ID_KEY.equals(p.name)) {
                if (!fslPath.isArcOnly()) {
                    throw new RuntimeException("");
                }
                id = (String) values.getFirst();
            }
            else if (p.isTypeVocabTerm()) {
                values = values.stream().map(this::mapVocabTerm).toList();
                orderedSelection.add(new Selected(fslPath, values));
            }
            else {
                values = values.stream()
                        .map(v -> {
                            if (v instanceof LanguageContainer l && selectedLang != null) {
                                // TODO should we remember here that these are script alts?
                                return l.languages.get(selectedLang);
                            }
                            if (fslPath.isIntegralProperty()) {
                                Options options = takeAllAlternate ? Options.TAKE_ALL_ALTERNATE : Options.DEFAULT;
                                if (lens.lensGroup() instanceof DerivedLensGroup d) {
                                    List<LensGroupName> handled = d.minus().stream()
                                            .filter(l -> findLens(thing, l).showProperties().contains(fslPath))
                                            .toList();
                                    if (!handled.isEmpty()) {
                                        // The integral thing may have already been handled by a deducted lens
                                        // Thus we need to continue with a derived lens to avoid repetition at this level
                                        return applyLens(v, new DerivedLensGroup(d.base(), handled, d.subLens()), options);
                                    }
                                }
                                return applyLens(v, lens.base(), options, selectedLang);
                            }
                            return applyLens(v, lens.subLens(), selectedLang);
                        })
                        .toList();
                orderedSelection.add(new Selected(fslPath, values));
            }

            return true;
        }

        @Override
        public Map<String, Object> getThingForIndex() {
            return buildThingForIndex();
        }

        @Override
        public boolean isEmpty() {
            return orderedSelection.isEmpty();
        }

        @Override
        protected StringBuilder printTo(StringBuilder s) {
            orderedSelection.forEach(p -> printTo(s, p.value));
            return s;
        }

        @Override
        public Map<LangCode, StringBuilder> byLang(Map<LangCode, StringBuilder> stringsByLang) {
            orderedSelection.forEach(p -> byLang(stringsByLang, p.value()));
            return stringsByLang;
        }

        @Override
        protected Map<LangCode, StringBuilder> byScript(Map<LangCode, StringBuilder> stringsByLang) {
            orderedSelection.forEach(p -> byScript(stringsByLang, p.value()));
            return stringsByLang;
        }

        private Map<String, Object> buildThingForIndex() {
            Map<String, Object> thing = new LinkedHashMap<>();
            if (id != null) {
                thing.put(JsonLd.ID_KEY, id);
            }
            if (type != null) {
                // TODO: Only for certain entitites? Should be included in lens definition if wanted?
                thing.put(JsonLd.TYPE_KEY, type);
            }
            orderedSelection.forEach(p -> insert(thing, p.selector(), p.value()));
            if (type != null) {
                var _str = buildSearchStr(thing);
                if (!_str.isEmpty()) {
                    thing.put(JsonLd.SEARCH_KEY, _str.size() == 1 ? _str.getFirst() : _str);
                }
            }
            return thing;
        }

        private List<String> buildSearchStr(Map<String, Object> thing) {
            var lensedForSearchStr = applyLens(thing, searchKeyLens, NO_FALLBACK);
            if (lensedForSearchStr.isEmpty()) {
                return List.of();
            }
            var byLang = lensedForSearchStr.byLang();
            if (!byLang.isEmpty()) {
                return jsonLd.locales.stream().map(byLang::get).filter(Objects::nonNull).toList();
            }
            var byScript = lensedForSearchStr.byScript();
            if (!byScript.isEmpty()) {
                return (List<String>) byScript.values();
            }
            return List.of(lensedForSearchStr.asString());
        }

        @SuppressWarnings("unchecked")
        private void insert(Map<String, Object> thing, FslPath selector, Object value) {
            List<String> steps = selector.asJsonPath();
            String key = steps.removeFirst();

            while (!steps.isEmpty()) {
                switch (thing.get(key)) {
                    case Map<?, ?> m -> thing = (Map<String, Object>) m;
                    case List<?> l -> {
                        Map<String, Object> child = new LinkedHashMap<>();
                        thing.put(key, Stream.concat(l.stream(), Stream.of(child)).toList());
                        thing = child;
                    }
                    case null -> {
                        Map<String, Object> child = new LinkedHashMap<>();
                        thing.put(key, child);
                        thing = child;
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + thing.get(key));
                }
                key = steps.removeFirst();
            }

            insert(thing, key, value);
        }

        private void insert(Map<String, Object> thing, String key, Object value) {
            switch (value) {
                case Collection<?> c -> c.forEach(v -> insert(thing, key, v));
                case LanguageContainer l -> insert(thing, (String) jsonLd.langContainerAlias.get(key), l.asLangMap(jsonLd.locales));
                case TransliteratedNode t -> insert(thing, key, t.transliterations.values().stream().map(Node::buildThingForIndex).toList());
                case Node n -> {
                    if (jsonLd.isVocabTerm(key) && n.id != null) {
                        insert(thing, key, jsonLd.toTermKey(n.id));
                    } else {
                        insert(thing, key, n.buildThingForIndex());
                    }
                }
                default -> {
                    List<Object> values = Stream.concat(asStream(thing.get(key)), Stream.of(value)).distinct().toList();
                    thing.put(key, values.size() == 1 ? values.getFirst() : values);
                }
            }
        }

        private Stream<?> asStream(Object o) {
            return o instanceof List<?> l ? l.stream() : Stream.ofNullable(o);
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

        private void byLang(Map<LangCode, StringBuilder> stringsByLangTag, Object value) {
            StringBuilder noLang = stringsByLangTag.computeIfAbsent(NO_LANG, k -> new StringBuilder());
            switch (value) {
                case Collection<?> c -> c.forEach(v -> byLang(stringsByLangTag, v));
                case LanguageContainer l -> {
                    if (!l.isTransliterated()) {
                        l.languages.forEach((lang, v) ->
                                printTo(stringsByLangTag.computeIfAbsent(lang, k -> new StringBuilder(noLang.toString())), v)
                        );
                    } else {
                        byLang(stringsByLangTag, l.languages.values());
                    }
                }
                case Lensed l -> l.byLang(stringsByLangTag);
                default -> stringsByLangTag.values().forEach(s -> printTo(s, value));
            }
        }

        private void byScript(Map<LangCode, StringBuilder> stringsByLangTag, Object value) {
            StringBuilder noLang = stringsByLangTag.computeIfAbsent(NO_LANG, k -> new StringBuilder());
            switch (value) {
                case Collection<?> c -> c.forEach(v -> byScript(stringsByLangTag, v));
                case LanguageContainer l -> {
                    if (l.isTransliterated()) {
                        l.languages.forEach((lang, v) ->
                                printTo(stringsByLangTag.computeIfAbsent(lang, k -> new StringBuilder(noLang.toString())), v)
                        );
                    } else {
                        byScript(stringsByLangTag, l.languages.values());
                    }
                }
                case Lensed l -> l.byScript(stringsByLangTag);
                default -> stringsByLangTag.values().forEach(s -> printTo(s, value));
            }
        }

        private Object mapVocabTerm(Object value) {
            if (value instanceof String s) {
                var def = jsonLd.vocabIndex.get(s);
                return applyLens(def != null ? def : s, lens.subLens(), selectedLang);
            } else {
                // bad data
                return applyLens(value, lens.subLens(), selectedLang);
            }
        }
    }

    public final class TransliteratedNode extends Lensed {
        Map<LangCode, Node> transliterations = new HashMap<>();
        void add(LangCode langCode, Node node) {
            transliterations.put(langCode, node);
        }

        @Override
        public Map<String, Object> getThingForIndex() {
            // FIXME
            throw new UnsupportedOperationException("");
        }

        @Override
        public boolean isEmpty() {
            return transliterations.values().stream().allMatch(Node::isEmpty);
        }

        @Override
        protected StringBuilder printTo(StringBuilder s) {
            if(!s.isEmpty() && s.charAt(s.length() - 1) != ' ') {
                s.append(" ");
            }
            transliterations.values().forEach(node -> node.printTo(s));
            return s;
        }

        @Override
        protected Map<LangCode, StringBuilder> byLang(Map<LangCode, StringBuilder> stringsByLang) {
            transliterations.values().forEach(node -> node.byLang(stringsByLang));
            return stringsByLang;
        }

        @Override
        protected Map<LangCode, StringBuilder> byScript(Map<LangCode, StringBuilder> stringsByLang) {
            transliterations.forEach((lang, n) -> {
                var noLang = new StringBuilder(stringsByLang.containsKey(NO_LANG) ? stringsByLang.get(NO_LANG).toString() : "");
                var s = stringsByLang.computeIfAbsent(lang, l -> noLang);
                n.printTo(s);
            });
            return stringsByLang;
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

        public Map<String, List<String>> asLangMap(List<String> locales) {
            Map<String, List<String>> langMap = new LinkedHashMap<>();
            if (isTransliterated()) {
                languages.forEach((langCode, values) -> langMap.put(langCode.code(), values));
            } else {
                for (String locale : locales) {
                    for (LangCode lang : languages.keySet()) {
                        if (locale.equals(lang.code())) {
                            langMap.put(lang.code(), languages.get(lang));
                            break;
                        }
                    }
                }
            }
            return langMap;
        }
    }

    private Lens findLens(Map<?,?> thing, LensGroupName lensGroupName) {
        return findLens(thing, lensGroupName, FallbackLens.DEFAULT);
    }

    private Lens findLens(Map<?,?> thing, LensGroupName lensGroupName, FallbackLens fallbackLens) {
        var types = thing.get(JsonLd.TYPE_KEY);
        var cacheKey = new LensCacheKey(types, lensGroupName, fallbackLens);

        return lensCache.computeIfAbsent(cacheKey, k -> {
            for (var groupName : lensGroupName.groups) {
                @SuppressWarnings("unchecked")
                var group = ((Map<String, Map<String,Object>>) jsonLd.displayData.get("lensGroups")).get(groupName);
                @SuppressWarnings("unchecked")
                var lens = (Map<String, Object>) jsonLd.getLensFor(thing, group);
                if (lens != null) {
                    return new Lens(lens, lensGroupName);
                }
            }

            return new Lens(fallbackLens.lens, lensGroupName);
        });
    }

    public class Lens {
        private final LensGroup lensGroup;
        private final List<ShowProperty> showProperties;

        public Lens(Map<String, Object> lensDefinition, LensGroupName lensGroupName) {
            this.lensGroup = new DefinedLensGroup(lensGroupName, FresnelUtil.subLens(lensGroupName));

            @SuppressWarnings("unchecked")
            var showProperties = (List<Object>) lensDefinition.get(Fresnel.showProperties);
            this.showProperties = parseShowProperties(showProperties);
        }

        private Lens(List<ShowProperty> showProperties, LensGroup lensGroup) {
            this.lensGroup = lensGroup;
            this.showProperties = showProperties;
        }

        LensGroup lensGroup() {
            return lensGroup;
        }

        LensGroupName base() {
            return lensGroup.base();
        }

        LensGroupName subLens() {
            return lensGroup.subLens();
        }

        List<ShowProperty> showProperties() {
            return showProperties;
        }

        private List<ShowProperty> parseShowProperties(List<Object> showProperties) {
            return showProperties.stream()
                    .map(p -> {
                        if (JsonLd.isAlternateProperties(p)) {
                            return parseAlternateProperties(p);
                        }
                        if (isMergeProperties(p)) {
                            return parseMergeProperties(p);
                        }
                        if (p instanceof List<?> list) {
                            // expanded lang alias, i.e. ["x", "xByLang"] inside alternateProperties
                            // TODO remove expansion in jsonLd?
                            if (list.size() == 2) {
                                return new FslPath((String) list.getFirst());
                            }
                        }
                        if (isInverseProperty(p)) {
                            return parseInverseProperty(p);
                        }
                        if (JsonLd.isAlternateRangeRestriction(p)) {
                            return parseRangeRestriction(p);
                        }
                        if (isFslSelector(p)) {
                            return parseFslSelector(p);
                        }
                        if (p instanceof String k) {
                            if (!jsonLd.langContainerAliasInverted.containsKey(k)) {
                                return new FslPath(k);
                            }
                            // ignore langContainer aliases expanded by jsonld
                            return null;
                        }
                        return new Unrecognized();
                    })
                    .filter(Objects::nonNull)
                    .toList();
        }

        @SuppressWarnings("unchecked")
        private AlternateProperties parseAlternateProperties(Object alternateProperties) {
            var alternatives = (List<Object>) ((Map<String, Object>) alternateProperties).get(Fresnel.alternateProperties);
            return new AlternateProperties(parseShowProperties(alternatives));
        }

        @SuppressWarnings("unchecked")
        private MergeProperties parseMergeProperties(Object mergeProperties) {
            var m = (Map<String, Object>) mergeProperties;
            var merge = (List<Object>) m.get(Fresnel.mergeProperties);
            var use = new FslPath((String) m.get(Fresnel.use));
            return new MergeProperties(parseShowProperties(merge), use);
        }

        private boolean isMergeProperties(Object showProperty) {
            return showProperty instanceof Map<?, ?> m && m.containsKey(Fresnel.mergeProperties) && m.containsKey(Fresnel.use);
        }

        private boolean isInverseProperty(Object showProperty) {
            return showProperty instanceof Map && ((Map<?, ?>) showProperty).containsKey("inverseOf");
        }

        private FslPath parseInverseProperty(Object showProperty) {
            String p = (String) ((Map<?, ?>) showProperty).get("inverseOf");
            return new FslPath(FslPath.IN + p);
        }

        @SuppressWarnings("unchecked")
        private FslPath parseRangeRestriction(Object showProperty) {
            Map<String, String> r = (Map<String, String>) showProperty;
            // TODO: The correct interpretation of this would be to match all subclasses, i.e. prefix the range class with a ^,
            //  however we can't do this at the moment since we depend on subclasses *not* being matched for constructions like e.g.
            //  {"subPropertyOf": "hasTitle", "range": "Title"}
//            return new FslPath(r.get(Rdfs.SUB_PROPERTY_OF) + '[' + FslPath.SUB + r.get(Rdfs.RANGE) + ']');
            return new FslPath(r.get(Rdfs.SUB_PROPERTY_OF) + '[' + r.get(Rdfs.RANGE) + ']');
        }

        private boolean isFslSelector(Object showProperty) {
            return showProperty instanceof Map<?, ?> m && Fresnel.fslselector.equals(m.get(JsonLd.TYPE_KEY));
        }

        private FslPath parseFslSelector(Object showProperty) {
            return new FslPath((String) ((Map<?, ?>) showProperty).get(JsonLd.VALUE_KEY));
        }

        // TODO
        Lens minus(Collection<Lens> minus, DerivedLensGroup derived) {
            var keep = new ArrayList<>(showProperties);

            for (var m : minus) {
                for (var sp : m.showProperties()) {
                    if (sp instanceof FslPath f && f.isIntegralProperty()) {
                        continue;
                    }
                    keep.remove(sp);
                }
            }

            return new Lens(keep, derived);
        }
    }

    public sealed interface LensGroup permits DefinedLensGroup, DerivedLensGroup {
        LensGroupName base();
        LensGroupName subLens();
    }

    public record DefinedLensGroup(LensGroupName base, LensGroupName subLens) implements LensGroup {}
    public record DerivedLensGroup(LensGroupName base, List<LensGroupName> minus, LensGroupName subLens) implements LensGroup {}

    private static LensGroupName subLens(LensGroupName lensGroupName) {
        return switch (lensGroupName) {
            case Full -> LensGroupName.Card;
            case Card -> LensGroupName.Chip;
            case Chip, Token -> LensGroupName.Token;

            case SearchCard, SearchChip -> LensGroupName.SearchChip;
            case SearchToken -> LensGroupName.SearchToken;
        };
    }

    private sealed interface ShowProperty permits AlternateProperties, FslPath, MergeProperties, Unrecognized {
    }

    private record AlternateProperties(List<ShowProperty> alternatives) implements ShowProperty {
    }

    private record MergeProperties(List<ShowProperty> merge, FslPath use) implements ShowProperty {
    }

    private record Unrecognized() implements ShowProperty {
    }

    private final class FslPath implements ShowProperty {
        private static final String IN = "in::";
        private static final String SUB = "^";

        private final String path;

        FslPath(String path) {
            this.path = path;
        }

        List<Object> getValues(Map<?, ?> sourceEntity) {
            return isArcOnly()
                    ? getSoleArcStep().getValues(sourceEntity)
                    : getValues(sourceEntity, new ArrayList<>(List.of(path.split("/"))));
        }

        ArcStep getEndArcStep() {
            return isArcOnly()
                    ? getSoleArcStep()
                    : new ArcStep(path.substring(path.lastIndexOf("/") + 1));
        }

        ArcStep getSoleArcStep() {
            return new ArcStep(path);
        }

        List<String> asJsonPath() {
            String[] steps = path.split("/");
            List<String> jsonPath = new ArrayList<>();
            for (int i = 0; i < steps.length; i += 2) {
                jsonPath.addAll(new ArcStep(steps[i]).asJsonPath());
            }
            return jsonPath;
        }

        boolean isArcOnly() {
            return !path.contains("/");
        }

        public boolean isIntegralProperty() {
            return isArcOnly() && getSoleArcStep().asPropertyKey().isIntegral();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof FslPath other && path.equals(other.path);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(path);
        }

        private List<Object> getValues(Map<?, ?> currentEntity, List<String> pathRemainder) {
            if (pathRemainder.isEmpty()) {
                return List.of();
            }

            if (pathRemainder.size() == 1) {
                return new ArcStep(pathRemainder.getFirst()).getValues(currentEntity);
            }

            ArcStep nextArcStep = new ArcStep(pathRemainder.removeFirst());
            NodeStep nextNodeStep = new NodeStep(pathRemainder.removeFirst());

            return nextArcStep.getValues(currentEntity).stream()
                    .filter(nextNodeStep::isCompatible)
                    .map(Map.class::cast)
                    .map(m -> getValues(m, pathRemainder))
                    .flatMap(List::stream)
                    .toList();
        }

        private sealed abstract class LocationStep permits ArcStep, NodeStep {}

        private final class NodeStep extends LocationStep {
            private final List<String> allowedTypes = new ArrayList<>();

            NodeStep(String nodeStep) {
                init(nodeStep);
            }

            boolean isCompatible(Object node) {
                return node instanceof Map<?, ?> m && (!restrictTypes() || isAllowedType(m, allowedTypes));
            }

            private void init(String nodeStep) {
                if (nodeStep.equals(Fresnel.WILD_PROPERTY)) {
                    return;
                }
                if (nodeStep.startsWith(SUB)) {
                    nodeStep = nodeStep.substring(SUB.length());
                    allowedTypes.add(nodeStep);
                    allowedTypes.addAll(jsonLd.getSubClasses(nodeStep));
                } else {
                    allowedTypes.add(nodeStep);
                }
            }

            private boolean restrictTypes() {
                return !allowedTypes.isEmpty();
            }
        }

        private final class ArcStep extends LocationStep {
            private boolean reverse = false;
            private final List<String> allowedTypes = new ArrayList<>();
            private final List<PropertyKey> candidateKeys = new ArrayList<>();

            ArcStep(String arcStep) {
                init(arcStep);
            }

            List<Object> getValues(Map<?, ?> entity) {
                return candidateKeys.stream()
                        .flatMap(p -> getValues(entity, p).stream())
                        .filter(v -> !restrictTypes() || (v instanceof Map<?,?> m && isAllowedType(m, allowedTypes)))
                        .toList();
            }

            PropertyKey asPropertyKey() {
                PropertyKey baseProp = candidateKeys.getFirst();
                if (reverse) {
                    var inverse = jsonLd.getInverseProperty(baseProp.name());
                    return new PropertyKey(inverse != null ? inverse : JsonLd.REVERSE_KEY + "." + baseProp.name());
                }
                return baseProp;
            }

            List<String> asJsonPath() {
                PropertyKey baseProp = candidateKeys.getFirst();
                return reverse ? List.of(JsonLd.REVERSE_KEY, baseProp.name()) : List.of(baseProp.name());
            }

            private List<Object> getValues(Map<?, ?> m, PropertyKey p) {
                if (reverse) {
                    m = (Map<?, ?>) DocumentUtil.getAtPath(m, List.of(JsonLd.REVERSE_KEY), Map.of());
                }
                String pName = Rdfs.RDF_TYPE.equals(p.name()) ? JsonLd.TYPE_KEY : p.name();
                @SuppressWarnings("unchecked")
                List<Object> v = JsonLd.asList(m.get(pName));
                if (p.hasLangAlias() && m.containsKey(p.langAlias())) {
                    v.add(new LanguageContainer((Map<?, ?>) m.get(p.langAlias())));
                }
                return v;
            }

            private boolean restrictTypes() {
                return !allowedTypes.isEmpty();
            }

            private void init(String arcStep) {
                if (arcStep.startsWith(IN)) {
                    reverse = true;
                    arcStep = arcStep.substring(IN.length());
                }

                if (arcStep.matches(".+\\[.+]")) {
                    String allowedType = arcStep.substring(arcStep.indexOf('[') + 1, arcStep.indexOf(']'));
                    arcStep = arcStep.substring(0, arcStep.indexOf('['));
                    if (allowedType.startsWith(SUB)) {
                        allowedType = allowedType.substring(SUB.length());
                        allowedTypes.add(allowedType);
                        allowedTypes.addAll(jsonLd.getSubClasses(allowedType));
                    } else {
                        allowedTypes.add(allowedType);
                    }
                }

                if (arcStep.startsWith(SUB)) {
                    PropertyKey k = new PropertyKey(arcStep.substring(SUB.length()));
                    candidateKeys.add(k);
                    jsonLd.getSubProperties(k.name).stream().map(PropertyKey::new).forEach(candidateKeys::add);
                } else {
                    candidateKeys.add(new PropertyKey(arcStep));
                }
            }
        }

        private boolean isAllowedType(Map<?, ?> entity, List<String> allowedTypes) {
            return allowedTypes.stream().anyMatch(JsonLd.asList(entity.get(JsonLd.TYPE_KEY))::contains);
        }
    }

    private class PropertyKey {
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

        boolean isIntegral() {
            return jsonLd.isIntegral(name);
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

    public record LangCode(String code) {
        public static final LangCode NO_LANG = new LangCode("");

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
            result.display = formatProperties(node.orderedSelection, node.type);
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

        private Decorated formatLanguageContainer(LanguageContainer lang, String className, String propertyName) {
            var picked = lang.pick(locale, fallbackLocales);
            var literal = new DecoratedLiteral(String.join(" ", picked));
            formats.valueStyle(className, propertyName).apply(literal);
            return literal;
        }

        private List<Decorated> formatProperties(List<Node.Selected> selected, String className) {
            return mapWithIndex(selected,
                    (p, ix) -> formatProperty(p, className, ix == 0, ix == selected.size() - 1)
            );
        }

        private Decorated formatProperty(Node.Selected selected, String className, boolean isFirst, boolean isLast) {
            FslPath fslPath = selected.selector();
            if (!fslPath.isArcOnly()) {
                // TODO
                throw new UnsupportedOperationException("Formatting with multi-step FSL path is not supported");
            }

            String pName = fslPath.getSoleArcStep().asPropertyKey().name();
            Object v = selected.value();
            var result = new DecoratedProperty(pName, formatValues(v, className, pName));

            formats.propertyStyle(className, pName).apply(result);
            // TODO implement _style sort() ?
            formats.propertyDetails(className, pName).apply(result, isFirst, isLast);
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
                case LanguageContainer lang -> formatLanguageContainer(lang, className, propertyName);
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
