package whelk.util;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import whelk.Document;
import whelk.JsonLd;
import whelk.JsonLd.Rdfs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.RECORD_TYPE;
import static whelk.JsonLd.REVERSE_KEY;
import static whelk.JsonLd.THING_KEY;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.asList;

// https://www.w3.org/2005/04/fresnel-info/manual/

// TODO handle subPropertyOf
//   -- https://www.w3.org/2005/04/fresnel-info/fsl/#rdfsowl
//   -- https://github.com/libris/definitions/blob/41b0ac7b7089974dc1d1c41f221c038f1353df75/source/vocab/display.jsonld#L171
// TODO fallback style for things that fall outside the class hierarchy?
// TODO defer language selection?
// TODO bad data - blank nodes without type?

public class FresnelUtil {
    public static final String CARDS = "cards";
    public static final String CHIPS = "chips";
    public static final String FULL = "full";
    public static final String SEARCH_CARDS = "search-cards";
    public static final String SEARCH_CHIPS = "search-chips";
    public static final String SEARCH_TOKENS = "search-tokens";
    public static final String TOKENS = "tokens";

    public record LensGroupChain(List<String> chain) {
        public LensGroupChain(String group) {
            this(List.of(group));
        }
    }

    public static final LensGroupChain CARD_CHAIN = new LensGroupChain(List.of(CARDS));
    public static final LensGroupChain CHIP_CHAIN = new LensGroupChain(List.of(CHIPS));
    public static final LensGroupChain SEARCH_CARD_CHAIN = new LensGroupChain(List.of(SEARCH_CARDS, CARDS, SEARCH_CHIPS, CHIPS, TOKENS));
    public static final LensGroupChain SEARCH_CHIP_CHAIN = new LensGroupChain(List.of(SEARCH_CHIPS, CHIPS, TOKENS));
    public static final LensGroupChain SEARCH_TOKEN_CHAIN = new LensGroupChain(SEARCH_TOKENS);
    public static final LensGroupChain TOKEN_CHAIN = new LensGroupChain(List.of(TOKENS, CHIPS));

    public static final class Lenses {
        public static final Lens SEARCH_CARD = new Lens(SEARCH_CARD_CHAIN);
        public static final Lens SEARCH_CHIP = new Lens(SEARCH_CHIP_CHAIN);
        public static final Lens SEARCH_TOKEN = new Lens(SEARCH_TOKEN_CHAIN);
        public static final Lens TOKEN = new Lens(TOKEN_CHAIN);
    }

    public static final class NestedLenses {
        public static final Lens CHIP_TO_TOKEN = new Lens(CHIP_CHAIN, Lenses.TOKEN);
        public static final Lens CARD_TO_CHIP_TO_TOKEN = new Lens(CARD_CHAIN, CHIP_TO_TOKEN);
        public static final Lens SEARCH_CARD_TO_SEARCH_CHIP = new Lens(SEARCH_CARD_CHAIN, Lenses.SEARCH_CHIP);
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
        public static String property = "fresnel:property";
        public static String propertyFormat = "fresnel:propertyFormat";
        public static String propertyFormatDomain = "fresnel:propertyFormatDomain";
        public static String propertyStyle = "fresnel:propertyStyle";
        public static String resourceFormat = "fresnel:resourceFormat";
        public static String resourceStyle = "fresnel:resourceStyle";
        public static String subLens = "fresnel:subLens";
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
        TAKE_ALL_ALTERNATE,
        TRACK_ORIGINAL,
        SKIP_MAP_VOCAB_TERMS,
        NO_FALLBACK
    }

    private enum FallbackLens {
        //TODO load from display.jsonld
        DEFAULT(Map.of(
                TYPE_KEY, Fresnel.Lens,
                Fresnel.showProperties, List.of(
                        Map.of(Fresnel.alternateProperties, List.of(
                                // TODO this is the expanded form with xByLang like in JsonLd
                                "prefLabel", "prefLabelByLang", "label", "labelByLang", "name", "nameByLang", "@id"
                        ))
                )
        )),
        EMPTY(Map.of(
                TYPE_KEY, Fresnel.Lens,
                Fresnel.showProperties, List.of()
        ));

        private final Map<String, Object> lens;

        FallbackLens(Map<String, Object> lens) {
            this.lens = lens;
        }
    }

    private record DerivedLensCacheKey(Object types, LensGroupChain lensGroupChain, List<LensGroupChain> minus) {}
    private record LensCacheKey(Object types, LensGroupChain lensGroupChain) {}

    private static final Logger logger = LogManager.getLogger(FresnelUtil.class);

    JsonLd jsonLd;
    List<LangCode> fallbackLocales;
    Formats formats;

    private final Map<DerivedLensCacheKey, List<ShowProperty>> derivedLensCache = new ConcurrentHashMap<>();
    private final Map<LensCacheKey, List<ShowProperty>> lensCache = new ConcurrentHashMap<>();

    private static final String TMP_ID = "_id";

    public FresnelUtil(JsonLd jsonLd) {
        this.jsonLd = jsonLd;
        this.fallbackLocales = jsonLd.locales.stream().map(LangCode::new).toList();
        this.formats = new Formats(getUnsafe(jsonLd.displayData, "formatters", null));
    }

    public Map<String, Object> getLensedThing(Object thing, Lens lens) {
        return getLensedThing(thing, lens, List.of());
    }

    public Map<String, Object> getLensedThing(Object thing, Lens lens, Collection<String> preserveLinks) {
        var options = List.of(Options.TAKE_ALL_ALTERNATE, Options.TRACK_ORIGINAL, Options.SKIP_MAP_VOCAB_TERMS);
        return new AppliedLens(thing, lens, preserveLinks, options).getThing();
    }

    public Lensed applyLens(Object thing, Lens lens) {
        return applyLens(thing, lens, List.of());
    }

    public Lensed applyLens(Object thing, Lens lens, Collection<Options> options) {
        // TODO
        if (!isTypedNode(thing)) {
            throw new IllegalArgumentException("Thing is not typed node: " + thing);
        }

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
            if (value instanceof Map node && node.containsKey(TYPE_KEY)) {
                try {
                    var label = format(applyLens(value, NestedLenses.CHIP_TO_TOKEN), locale).asString();
                    node.put(JsonLd.Platform.COMPUTED_LABEL, label);
                    // TODO Check if structured value and don't compute for sub-nodes?
                } catch (Exception e) {
                    logger.warn("Error computing label for {}: {}", data, e, e);
                }
            }

            return DocumentUtil.NOP;
        });
    }

    public List<?> fslSelect(Map<String, Object> thing, String fslSelector) {
        return new FslPath(fslSelector).select(thing)
                .stream()
                .map(Node.Selected::getFlatValues)
                .flatMap(List::stream)
                .toList();
    }

    private Object applyLens(
            Object value,
            Lens lens,
            Collection<Options> options,
            LangCode selectedLang
    ) {
        if (!(value instanceof Map<?, ?>)) {
            // literal
            return value;
        }

        var thing = asMap(value);

        List<LangCode> scripts = selectedLang == null ? scriptAlternatives(thing) : Collections.emptyList();
        if (!scripts.isEmpty()) {
            TransliteratedNode n = new TransliteratedNode();
            for (var script : scripts) {
                n.add(script, (Node) applyLens(thing, lens, options, script));
            }
            return n;
        }

        var result = new Node(lens, selectedLang);

        List<ShowProperty> showProperties = loadShowProperties(thing, lens, options);

        result.select(thing, showProperties, options);

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

    private class AppliedLens {
        Collection<String> preserveLinks;
        Lensed lensed;
        Map<String, List<Node>> nodeTmpIdMap;

        Map<String, Object> thing;

        private static final Lens searchKeyLens = Lenses.SEARCH_TOKEN;

        private AppliedLens(Object thing, Lens lens, Collection<String> preserveLinks, Collection<Options> options) {
            this.preserveLinks = preserveLinks;
            this.lensed = applyLens(getCopyWithTmpIds(thing), lens, options);
            this.nodeTmpIdMap = buildNodeTmpIdMap(lensed);
        }

        public Map<String, Object> getThing() {
            if (thing == null) {
                this.thing = getThing(lensed);
            }
            return thing;
        }

        public void restoreLinks() {
            // TODO
        }

        private Map<String, Object> getThing(Lensed lensed) {
            Map<String, Object> thing = switch (lensed) {
                case Node n -> merge(getShared(n));
                case TransliteratedNode t -> merge(t.transliterations.values());
            };
            if (thing.containsKey(TYPE_KEY)) {
                var _str = buildSearchStr(thing);
                if (!_str.isEmpty()) {
                    thing.put(JsonLd.SEARCH_KEY, unwrapSingle(_str));
                }
            }
            return thing;
        }

        // FIXME: Naming, explain
        private List<Node> getShared(Node n) {
            return nodeTmpIdMap.getOrDefault(getTmpId(n), List.of());
        }

        private Map<String, Object> merge(Collection<Node> nodes) {
            Map<String, Object> mergedThing = new LinkedHashMap<>();
            for (Node n : nodes) {
                buildThing(n).forEach((k, v) -> insert(mergedThing, k, v));
            }
            return mergedThing;
        }

        private Map<String, Object> buildThing(Node n) {
            if (n instanceof LanguageContainer l) {
                return l.filteredLangMap(fallbackLocales);
            }

            Map<String, Object> lensedThing = new LinkedHashMap<>();

            if (n.id != null) {
                lensedThing.put(ID_KEY, n.id);
            }
            if (n.thing.containsKey(TYPE_KEY)) {
                lensedThing.put(TYPE_KEY, n.thing.get(TYPE_KEY));
            }
            if (RECORD_TYPE.equals(n.type)) {
                var mainEntityId = getUnsafe(n.thing, THING_KEY, Map.of()).get(ID_KEY);
                if (mainEntityId != null) {
                    // Always keep mainEntity link
                    lensedThing.put(THING_KEY, new HashMap<>(Map.of(ID_KEY, mainEntityId)));
                }
            }

            n.orderedSelection.forEach(s -> insert(lensedThing, s));
//            if (!preserveLinks.isEmpty()) {
//                restoreLinks(lensedThing, thing, preserveLinks);
//            }

            // Redundant
            lensedThing.remove(Rdfs.RDF_TYPE);

            return lensedThing;
        }

        private void insert(Map<String, Object> thing, Node.Selected s) {
            for (var value : s.values()) {
                var v = value instanceof Lensed l ? getThing(l) : value;
                if (s.pKey() instanceof InversePropertyKey ipk) {
                    insert(asMap(thing.computeIfAbsent(REVERSE_KEY, k -> new HashMap<>())), ipk.inverseOf(), v);
                } else if (s.pKey().hasLangAlias()) {
                    insert(thing, value instanceof LanguageContainer ? s.pKey().langAlias() : s.pKey().name(), v);
                } else {
                    insert(thing, s.pKey().name(), v);
                }
            }
        }

        private void insert(Map<String, Object> thing, String key, Object value) {
            List<Object> uniqueValues = Stream.concat(asStream(thing.get(key)), asStream(value)).distinct().collect(Collectors.toList());
            thing.put(key, unwrapSingle(uniqueValues));
        }

        private static void restoreLinks(Map<String, Object> lensedThing, Map<?, ?> originalThing, Collection<String> preserveLinks) {
            originalThing.forEach((k, v) -> {
                var key = (String) k;
                if (!lensedThing.containsKey(key)) {
                    Object links = JsonLd.retainLinks(v, preserveLinks);
                    if (!ObjectUtils.isEmpty(links)) {
                        DocumentUtil.traverse(List.of(links), (value, path) -> {
                            if (value instanceof Map<?, ?> m && JsonLd.isLink(m)) {
                                // Use a temporary ID key so these links are skipped during framing
                                lensedThing.put(key, Map.of("_id", m.get(ID_KEY)));
                                return new DocumentUtil.Replace(Map.of("_id", m.get(ID_KEY)));
                            }
                            return new DocumentUtil.Nop();
                        });
                        lensedThing.put(key, links);
                    }
                }
            });
        }

        private List<String> buildSearchStr(Map<String, Object> thing) {
            var lensedForSearchStr = applyLens(thing, searchKeyLens, List.of(Options.NO_FALLBACK));
            if (lensedForSearchStr.isEmpty()) {
                return List.of();
            }
            var byLang = lensedForSearchStr.byLang();
            if (!byLang.isEmpty()) {
                return new ArrayList<>(byLang.values());
            }
            var byScript = lensedForSearchStr.byScript();
            if (!byScript.isEmpty()) {
                return new ArrayList<>(byScript.values());
            }
            var asString = lensedForSearchStr.asString();
            if (!asString.isEmpty()) {
                return List.of(asString);
            }
            return List.of();
        }

        private String getTmpId(Node n) {
            return (String) n.thing.get(TMP_ID);
        }

        private Map<String, List<Node>> buildNodeTmpIdMap(Lensed lensed) {
            Map<String, List<Node>> map = new HashMap<>();
            populateNodeIdMap(map, lensed);
            return map;
        }

        private void populateNodeIdMap(Map<String, List<Node>> map, Object o) {
            switch (o) {
                case Collection<?> c -> c.forEach(elem -> populateNodeIdMap(map, elem));
                case TransliteratedNode t -> t.transliterations.values().forEach(elem -> populateNodeIdMap(map, elem));
                case Node n -> {
                    if (n.thing.containsKey(TMP_ID)) {
                        map.computeIfAbsent((String) n.thing.get(TMP_ID), k -> new ArrayList<>()).add(n);
                    }
                    n.orderedSelection.forEach(s -> populateNodeIdMap(map, s.values()));
                }
                default -> {
                }
            }
        }

        private static Object getCopyWithTmpIds(Object thing) {
            var copy = Document.deepCopy(thing);
            AtomicInteger i = new AtomicInteger();
            DocumentUtil.traverse(copy, (value, path) -> {
                if (value instanceof Map<?, ?>) {
                    Map<String, Object> m = asMap(value);
                    m.put("_id", Integer.toString(i.addAndGet(1)));
                }
                return new DocumentUtil.Nop();
            });
            return copy;
        }
    }

    // FIXME naming
    public sealed abstract class Lensed permits Node, TransliteratedNode {
        public String asString() {
            return printTo(new StringBuilder()).toString();
        }

        public Map<String, String> byLang() {
            return cleanLangMap(byLang(new LinkedHashMap<>()));
        }

        public Map<String, String> byScript() {
            return cleanLangMap(byScript(new LinkedHashMap<>()));
        }

        public abstract boolean isEmpty();

        protected abstract StringBuilder printTo(StringBuilder s);

        protected abstract Map<LangCode, StringBuilder> byLang(Map<LangCode, StringBuilder> stringsByLang);

        protected abstract Map<LangCode, StringBuilder> byScript(Map<LangCode, StringBuilder> stringsByLang);

        private Map<String, String> cleanLangMap(Map<LangCode, StringBuilder> stringsByLang) {
            Map<String, String> result = new LinkedHashMap<>();
            stringsByLang.forEach((lang, s) -> {
                if (!lang.equals(LangCode.NO_LANG)) {
                    result.put(lang.code(), s.toString());
                }
            });
            return result;
        }
    }

    public sealed class Node extends Lensed permits IntermediateNode, LanguageContainer {
        record Selected(PropertyKey pKey, List<?> values) {
            List<Object> getFlatValues() {
                return getFlatValues(values);
            }

            private List<Object> getFlatValues(Object v) {
                return switch (v) {
                    case Collection<?> c -> c.stream().flatMap(o -> getFlatValues(o).stream()).toList();
                    case IntermediateNode in -> getFlatValues(in.selected());
                    case Selected s -> getFlatValues(s.values());
                    // TODO: LanguageContainer?
                    default -> List.of(v);
                };
            }
        }

        LangCode selectedLang;
        String id;
        String type;
        List<Selected> orderedSelection = new ArrayList<>();
        Lens lens;
        Map<String, Object> thing;

        Node() {}

        Node(LangCode selectedLang) {
            this.lens = Lens.newEmpty();
            this.selectedLang = selectedLang;
        }

        Node(Lens lens, LangCode selectedLang) {
            this.lens = lens;
            this.selectedLang = selectedLang;
        }

        void select(Map<String, Object> thing, List<ShowProperty> showProperties, Collection<Options> options) {
            Stream.concat(Stream.of(new FslPath(TYPE_KEY), new FslPath(ID_KEY)), showProperties.stream())
                    .forEach(sp -> select(thing, sp, options));
        }

        private void select(Map<String, Object> thing, ShowProperty showProperty, Collection<Options> options) {
            if (options.contains(Options.TRACK_ORIGINAL)) {
                this.thing = thing;
            }
            switch (showProperty) {
                case AlternateProperties a -> {
                    for (var alternative : a.alternatives()) {
                        if (alternative instanceof FslPath fslPath) {
                            boolean selected = select(thing, fslPath, options);
                            if (selected && !options.contains(Options.TAKE_ALL_ALTERNATE)) {
                                break;
                            }
                        }
                    }
                }
                case FslPath fslPath -> select(thing, fslPath, options);
                case Unrecognized ignored -> {}
                case MergeProperties mergeProperties -> {
                    Node n = new Node(selectedLang);
                    for (var m : mergeProperties.merge()) {
                        n.select(thing, m, options);
                    }
                    var values = n.orderedSelection.stream().map(Selected::getFlatValues).flatMap(FresnelUtil::asStream).toList();
                    if (!values.isEmpty()) {
                        this.orderedSelection.add(new Selected(mergeProperties.use(), values));
                    }
                }
                case PropertyDescription pd -> {
                    var values = new ArrayList<>();
                    for (Object o : asList(thing.get(pd.property().name()))) {
                        if (o instanceof Map<?, ?> m) {
                            Node n = new Node(selectedLang);
                            n.select(asMap(m), pd.subLens(), options);
                            if (!n.isEmpty()) {
                                values.add(n);
                            }
                        }
                    }
                    if (!values.isEmpty()) {
                        this.orderedSelection.add(new Selected(pd.property(), values));
                    }
                }
            }
        }

        private boolean select(Map<String, Object> thing, FslPath fslPath, Collection<Options> options) {
            List<Selected> selection = new ArrayList<>();

            for (Selected s : fslPath.select(thing)) {
                var p = s.pKey();

                var nextLensGroup = getNextLensLevel(lens, thing, fslPath, options, p.isIntegral());

                var values = s.values()
                        .stream()
                        .map(o -> build(o, nextLensGroup, p, options))
                        .filter(Objects::nonNull)
                        .toList();

                if (values.isEmpty()) {
                    continue;
                }

                if (Rdfs.RDF_TYPE.equals(p.name())) {
                    var type = (String) values.getFirst(); // TODO how to handle multiple types?
                    selection.add(new Selected(p, List.of(mapVocabTerm(type, options))));
                }
                else if (TYPE_KEY.equals(p.name())) {
                    type = (String) values.getFirst(); // TODO how to handle multiple types?
                }
                else if (ID_KEY.equals(p.name())) {
                    id = (String) values.getFirst();
                } else if (p.isTypeVocabTerm()) {
                    values = values.stream().map(value -> mapVocabTerm(value, options)).toList();
                    selection.add(new Selected(p, values));
                } else {
                    selection.add(new Selected(p, values));
                }
            }

            if (selection.isEmpty()) {
                return false;
            }

            orderedSelection.addAll(selection);

            return true;
        }

        private Object build(Object o, Lens l, PropertyKey p, Collection<Options> opts) {
            if (o instanceof IntermediateNode in) {
                var deepLensed = build(in, l, opts);
                return deepLensed.isEmpty() ? null : deepLensed;
            }
            if (o instanceof Map<?,?> m) {
                return p.hasLangAlias()
                        ? asLangContainer(m, selectedLang)
                        : applyLens(m, l, opts, selectedLang);
            }
            return o;
        }

        private IntermediateNode build(IntermediateNode n, Lens lens, Collection<Options> options) {
            n.orderedSelection = n.selected().stream()
                    .map(s -> {
                        var nextLensGroup = s.pKey().isIntegral() ? lens : lens.next();
                        var values = s.values().stream()
                                .map(v -> build(v, nextLensGroup, s.pKey(), options))
                                .toList();
                        return new Selected(s.pKey(), values);
                    }).toList();
            return n;
        }

        private LanguageContainer asLangContainer(Map<?, ?> m, LangCode selectedLang) {
            if (selectedLang != null && !m.containsKey(selectedLang.code())) {
                return null;
            }
            return new LanguageContainer(asMap(m), selectedLang);
        }

        private Lens getNextLensLevel(Lens current, Map<String, Object> thing, FslPath fslPath, Collection<Options> options, boolean integral) {
            if (integral) {
                if (current.isDerived()) {
                    List<LensGroupChain> handled = current.minus().stream()
                            .filter(l -> loadShowProperties(thing, l, options).contains(fslPath))
                            .toList();
                    // FIXME: Clarify comment
                    // The integral thing may have already been handled by a deducted lens
                    // Thus we need to continue with a derived lens to avoid repetition at this level
                    return new Lens(current.lensGroupChain(), current.subLens(), handled);
                }
                return current;
            }
            return current.next();
        }

        @Override
        public boolean isEmpty() {
            return orderedSelection.isEmpty();
        }

        @Override
        protected StringBuilder printTo(StringBuilder s) {
            orderedSelection.forEach(p -> printTo(s, p.values()));
            return s;
        }

        @Override
        protected Map<LangCode, StringBuilder> byLang(Map<LangCode, StringBuilder> stringsByLang) {
            orderedSelection.forEach(p -> byLang(stringsByLang, p.values()));
            return stringsByLang;
        }

        @Override
        protected Map<LangCode, StringBuilder> byScript(Map<LangCode, StringBuilder> stringsByLang) {
            orderedSelection.forEach(p -> byScript(stringsByLang, p.values()));
            return stringsByLang;
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
                case Lensed l -> l.printTo(s);
                default -> s.append(value);
            }
        }

        private void byLang(Map<LangCode, StringBuilder> stringsByLangTag, Object value) {
            stringsByLangTag.putIfAbsent(LangCode.NO_LANG, new StringBuilder());
            switch (value) {
                case Collection<?> c -> c.forEach(v -> byLang(stringsByLangTag, v));
                case Lensed l -> l.byLang(stringsByLangTag);
                default -> stringsByLangTag.values().forEach(s -> printTo(s, value));
            }
        }

        private void byScript(Map<LangCode, StringBuilder> stringsByLangTag, Object value) {
            stringsByLangTag.putIfAbsent(LangCode.NO_LANG, new StringBuilder());
            switch (value) {
                case Collection<?> c -> c.forEach(v -> byScript(stringsByLangTag, v));
                case Lensed l -> l.byScript(stringsByLangTag);
                default -> stringsByLangTag.values().forEach(s -> printTo(s, value));
            }
        }

        private Object mapVocabTerm(Object value, Collection<Options> options) {
            if (options.contains(Options.SKIP_MAP_VOCAB_TERMS)) {
                return value;
            } else if (value instanceof String s) {
                var def = jsonLd.vocabIndex.get(s);
                return applyLens(def != null ? def : s, lens.next(), options, selectedLang);
            } else {
                // bad data
                return applyLens(value, lens.next(), options, selectedLang);
            }
        }
    }

    public final class TransliteratedNode extends Lensed {
        Map<LangCode, Node> transliterations = new HashMap<>();
        void add(LangCode langCode, Node node) {
            transliterations.put(langCode, node);
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
                var noLang = new StringBuilder(stringsByLang.containsKey(LangCode.NO_LANG)
                        ? stringsByLang.get(LangCode.NO_LANG).toString()
                        : "");
                var s = stringsByLang.computeIfAbsent(lang, l -> noLang);
                n.printTo(s);
            });
            return stringsByLang;
        }
    }

    public final class LanguageContainer extends Node {
        public LanguageContainer(Map<String, Object> container) {
            this.thing = container;
        }

        public LanguageContainer(Map<String, Object> container, LangCode selectedLang) {
            this.thing = container;
            this.selectedLang = selectedLang;
        }

        public List<LangCode> languages() {
            return selectedLang != null
                    ? List.of(selectedLang)
                    : thing.keySet().stream().filter(Predicate.not(TMP_ID::equals)).map(LangCode::new).toList();
        }

        public List<String> get(LangCode langCode) {
            return asStringList(thing.get(langCode.code()));
        }

        public List<String> pick(LangCode langCode, List<LangCode> fallbackLocales) {
            if (isTransliterated()) {
                return get(selectedLang);
            }

            var values = get(langCode);

            if (!values.isEmpty()) {
                return values;
            }

            for (LangCode fallbackLocale : fallbackLocales) {
                values = get(fallbackLocale);
                if (!values.isEmpty()) {
                    return values;
                }
            }

            // random lang
            return asStringList(thing.values().iterator().next());
        }

        public boolean isTransliterated() {
            return thing.keySet().stream().anyMatch(LangCode::isTransliterated);
        }

        public Map<String, Object> filteredLangMap(List<LangCode> fallbackLocales) {
            return (selectedLang != null ? Stream.of(selectedLang) : fallbackLocales.stream())
                    .map(LangCode::code)
                    .filter(thing::containsKey)
                    .collect(Collectors.toMap(Function.identity(), thing::get));
        }

        @Override
        protected StringBuilder printTo(StringBuilder s) {
            languages().forEach(l -> super.printTo(s, get(l)));
            return s;
        }

        @Override
        protected Map<LangCode, StringBuilder> byLang(Map<LangCode, StringBuilder> stringsByLang) {
            if (!isTransliterated()) {
                StringBuilder noLang = stringsByLang.computeIfAbsent(LangCode.NO_LANG, l -> new StringBuilder());
                languages().forEach(l -> super.printTo(stringsByLang.computeIfAbsent(l, k -> new StringBuilder(noLang.toString())), get(l)));
            } else {
                languages().forEach(l -> super.byLang(stringsByLang, get(l)));
            }
            return stringsByLang;
        }

        @Override
        protected Map<LangCode, StringBuilder> byScript(Map<LangCode, StringBuilder> stringsByLang) {
            if (isTransliterated()) {
                StringBuilder noLang = stringsByLang.computeIfAbsent(LangCode.NO_LANG, l -> new StringBuilder());
                languages().forEach(l -> super.printTo(stringsByLang.computeIfAbsent(l, k -> new StringBuilder(noLang.toString())), get(l)));
            } else {
                languages().forEach(l -> super.byScript(stringsByLang, get(l)));
            }
            return stringsByLang;
        }

        @SuppressWarnings("unchecked")
        private List<String> asStringList(Object o) {
            return (List<String>) asList(o);
        }
    }

    private final class IntermediateNode extends Node {
        IntermediateNode(List<Node.Selected> selected, Map<String, Object> thing) {
            this.orderedSelection = selected;
            this.thing = thing;
        }

        List<Node.Selected> selected() {
            return orderedSelection;
        }

        @Override
        public boolean isEmpty() {
            return orderedSelection.stream().allMatch(s -> s.getFlatValues().isEmpty());
        }
    }

    private List<ShowProperty> loadShowProperties(Map<?,?> thing, Lens lens, Collection<Options> options) {
        var types = thing.get(TYPE_KEY);

        if (lens.isDerived()) {
            var cacheKey = new DerivedLensCacheKey(types, lens.lensGroupChain(), lens.minus());
            return derivedLensCache.computeIfAbsent(cacheKey, k -> {
                var keep = new ArrayList<>(loadShowProperties(thing, lens.lensGroupChain(), options));
                var remove = lens.minus().stream()
                        .map(l -> loadShowProperties(thing, l, options))
                        .flatMap(List::stream)
                        .distinct()
                        .filter(sp -> !(sp instanceof FslPath f && f.isIntegralProperty()))
                        .toList();
                keep.removeAll(remove);
                return keep;
            });
        }

        return loadShowProperties(thing, lens.lensGroupChain(), options);
    }

    private List<ShowProperty> loadShowProperties(Map<?,?> thing, LensGroupChain lensGroupChain, Collection<Options> options) {
        var types = thing.get(TYPE_KEY);
        var cacheKey = new LensCacheKey(types, lensGroupChain);

        var showProperties = lensCache.computeIfAbsent(cacheKey, k -> {
            for (var lensGroupName : lensGroupChain.chain()) {
                var group = (Map<?, ?>) getUnsafe(jsonLd.displayData, "lensGroups", Map.of()).get(lensGroupName);
                var lensDefinition = asMap(jsonLd.getLensFor(thing, group));
                if (!lensDefinition.isEmpty()) {
                    // FIXME
                    if (!"Resource".equals(lensDefinition.get("classLensDomain")) && !"StructuredValue".equals(lensDefinition.get("classLensDomain"))) {
                        return new ShowPropertyParser().parse(lensDefinition);
                    }
                }
            }
            return List.of();
        });

        if (!showProperties.isEmpty()) {
            return showProperties;
        }

        var fallbackLens = options.contains(Options.NO_FALLBACK) ? FallbackLens.EMPTY : FallbackLens.DEFAULT;
        return new ShowPropertyParser().parse(fallbackLens.lens);
    }

    public static class Lens {
        private final LensGroupChain lensGroupChain;
        private final Lens subLens;
        private final List<LensGroupChain> minus;

        public Lens(LensGroupChain lensGroupChain) {
            this(lensGroupChain, null);
        }

        public Lens(LensGroupChain lensGroupChain, Lens subLens) {
            this(lensGroupChain, subLens, null);
        }

        public Lens(LensGroupChain lensGroupChain, Lens subLens, List<LensGroupChain> minus) {
            this.lensGroupChain = lensGroupChain;
            this.subLens = subLens;
            this.minus = minus;
        }

        Lens next() {
            return subLens != null ? subLens : this;
        }

        LensGroupChain lensGroupChain() {
            return lensGroupChain;
        }

        Lens subLens() {
            return subLens;
        }

        List<LensGroupChain> minus() {
            return minus != null ? minus : List.of();
        }

        static Lens newEmpty() {
            return new Lens(new LensGroupChain(List.of()));
        }

        boolean isDerived() {
            return !minus().isEmpty();
        }

        boolean isNested() {
            return subLens != null;
        }
    }

    private class ShowPropertyParser {
        private List<ShowProperty> parse(Map<String, Object> lensDefinition) {
            return parse((List<?>) lensDefinition.get(Fresnel.showProperties));
        }

        private List<ShowProperty> parse(List<?> showProperties) {
            return showProperties.stream()
                    .map(p -> {
                        if (JsonLd.isAlternateProperties(p)) {
                            return parseAlternateProperties(p);
                        }
                        if (isMergeProperties(p)) {
                            return parseMergeProperties(p);
                        }
                        if (isPropertyDescription(p)) {
                            return parsePropertyDescription(p);
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
            return new AlternateProperties(parse(alternatives));
        }

        @SuppressWarnings("unchecked")
        private MergeProperties parseMergeProperties(Object mergeProperties) {
            var m = asMap(mergeProperties);
            var merge = (List<Object>) m.get(Fresnel.mergeProperties);
            var use = new PropertyKey((String) m.get(Fresnel.use));
            return new MergeProperties(parse(merge), use);
        }

        private boolean isPropertyDescription(Object showProperty) {
            return showProperty instanceof Map<?, ?> m && m.containsKey(Fresnel.property) && m.containsKey(Fresnel.subLens);
        }

        private PropertyDescription parsePropertyDescription(Object propertyDescription) {
            var m = asMap(propertyDescription);
            var p = (String) m.get(Fresnel.property);
            var subLens = parse(asMap(m.get(Fresnel.subLens)));
            return new PropertyDescription(new PropertyKey(p), subLens);
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
            return showProperty instanceof Map<?, ?> m && Fresnel.fslselector.equals(m.get(TYPE_KEY));
        }

        private FslPath parseFslSelector(Object showProperty) {
            return new FslPath((String) ((Map<?, ?>) showProperty).get(JsonLd.VALUE_KEY));
        }
    }

    private sealed interface ShowProperty permits AlternateProperties, FslPath, MergeProperties, PropertyDescription, Unrecognized {
    }

    private record AlternateProperties(List<ShowProperty> alternatives) implements ShowProperty {
    }

    private record MergeProperties(List<ShowProperty> merge, PropertyKey use) implements ShowProperty {
    }

    private record PropertyDescription(PropertyKey property, List<ShowProperty> subLens) implements ShowProperty {
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

        List<Node.Selected> select(Map<String, Object> sourceEntity) {
            return select(sourceEntity, new ArrayList<>(List.of(path.split("/"))));
        }

        boolean isIntegralProperty() {
            return new ArcStep(path).asPropertyKey().isIntegral();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof FslPath other && path.equals(other.path);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(path);
        }

        private List<Node.Selected> select(Map<String, Object> currentEntity, List<String> pathRemainder) {
            if (pathRemainder.isEmpty()) {
                return List.of();
            }

            if (pathRemainder.size() == 1) {
                return new ArcStep(pathRemainder.getFirst()).select(currentEntity);
            }

            ArcStep nextArcStep = new ArcStep(pathRemainder.removeFirst());
            NodeStep nextNodeStep = new NodeStep(pathRemainder.removeFirst());

            return nextArcStep.select(currentEntity, nextNodeStep.allowedTypes()).stream()
                    .map(s -> {
                        var values = s.values().stream()
                                .map(v -> {
                                    var m = asMap(v);
                                    return new IntermediateNode(select(m, pathRemainder), m);
                                })
                                .toList();
                        return new Node.Selected(s.pKey(), values);
                    })
                    .toList();
        }


        private sealed abstract class LocationStep permits ArcStep, NodeStep {}

        private final class NodeStep extends LocationStep {
            private final List<String> allowedTypes = new ArrayList<>();

            NodeStep(String nodeStep) {
                init(nodeStep);
            }

            List<String> allowedTypes() {
                return allowedTypes;
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
        }

        private final class ArcStep extends LocationStep {
            private boolean reverse = false;
            private final List<String> allowedTypes = new ArrayList<>();
            private final List<String> candidateKeys = new ArrayList<>();

            ArcStep(String arcStep) {
                init(arcStep);
            }

            List<Node.Selected> select(Map<?, ?> entity) {
                return select(entity, allowedTypes);
            }

            List<Node.Selected> select(Map<?, ?> entity, List<String> allowedTypes) {
                return candidateKeys.stream()
                        .map(p -> select(entity, p, allowedTypes))
                        .filter(Objects::nonNull)
                        .toList();
            }

            PropertyKey asPropertyKey() {
                return asPropertyKey(candidateKeys.getFirst());
            }

            private PropertyKey asPropertyKey(String p) {
                if (reverse) {
                    var inverse = jsonLd.getInverseProperty(p);
                    return new InversePropertyKey(inverse, p);
                }
                return new PropertyKey(p);
            }

            private Node.Selected select(Map<?, ?> m, String p, List<String> allowedTypes) {
                if (reverse) {
                    m = (Map<?, ?>) DocumentUtil.getAtPath(m, List.of(JsonLd.REVERSE_KEY), Map.of());
                }
                String pName = Rdfs.RDF_TYPE.equals(p) ? TYPE_KEY : p;

                List<Object> values = new ArrayList<>();

                for (var o : JsonLd.asList(m.get(pName))) {
                    if (isAllowedType(o, allowedTypes)) {
                        values.add(o);
                    }
                }

                if (jsonLd.langContainerAlias.containsKey(p)) {
                    var langAlias = (String) jsonLd.langContainerAlias.get(p);
                    if (m.containsKey(langAlias)) {
                        values.add(m.get(langAlias));
                    }
                }

                return values.isEmpty() ? null : new Node.Selected(asPropertyKey(p), values);
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
                    String k = arcStep.substring(SUB.length());
                    candidateKeys.add(k);
                    candidateKeys.addAll(jsonLd.getSubProperties(k));
                } else {
                    candidateKeys.add(arcStep);
                }
            }
        }

        private boolean isAllowedType(Object o, List<String> allowedTypes) {
            return allowedTypes.isEmpty()
                    || !(o instanceof Map<?,?> m)
                    || allowedTypes.stream().anyMatch(JsonLd.asList(m.get(TYPE_KEY))::contains);
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
            s.append(name());
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
            return thing.entrySet().stream()
                    .filter(e -> e.getKey() instanceof String s
                            && jsonLd.langContainerAliasInverted.containsKey(s))
                    .map(e -> new LanguageContainer(asMap(e.getValue())))
                    .filter(LanguageContainer::isTransliterated)
                    .map(LanguageContainer::languages)
                    .flatMap(List::stream)
                    .distinct()
                    .toList();
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

        public static boolean isTransliterated(String code) {
            return code.contains("-t-");
        }

        private boolean isTransliterated() {
             return isTransliterated(code);
        }
    }

    private class InversePropertyKey extends PropertyKey {
        private final String inverseOf;

        public InversePropertyKey(String name, String inverseOf) {
            super(name);
            this.inverseOf = inverseOf;
        }

        @Override
        public String name() {
            return name != null ? name : JsonLd.REVERSE_KEY + "." + inverseOf;
        }

        public String inverseOf() {
            return inverseOf;
        }
    }

    private boolean isTypedNode(Object o) {
        return o instanceof Map && ((Map<?, ?>) o).containsKey(TYPE_KEY);
    }

    // TODO handle multiple types=
    private String firstType(Map<?,?> thing) {
        var types = thing.get(TYPE_KEY);
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
            String pName = selected.pKey().name();
            List<?> v = selected.values();

            if (v.stream().anyMatch(IntermediateNode.class::isInstance)) {
                // TODO
                throw new UnsupportedOperationException("Formatting with multi-step FSL path is not supported");
            }

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
                return formatValues(lang.pick(locale, fallbackLocales), className, propertyName);
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
                if (!fd.getKey().equals(f.get(ID_KEY))) {
                    logger.warn("Mismatch in format id: {} {}", fd.getKey(), f.get(ID_KEY));
                }
                if (!Fresnel.Format.equals(f.get(TYPE_KEY))) {
                    logger.warn("Unknown type, skipping {}", f.get(TYPE_KEY));
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
            result.put(TYPE_KEY, type);
            if (id != null) {
                result.put(ID_KEY, id);
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
            for (var l : container.languages()) {
                // TODO MULTIPLE
                var v = new DecoratedLiteral(String.join(" ", container.get(l)));
                scripts.put(l, v);
            }
        }

        @Override
        StringBuilder printTo(StringBuilder s) {
            var codes = scripts.keySet().stream().sorted(LangCode.ORIGINAL_SCRIPT_FIRST).toList();
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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        if (o instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
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

    private static Stream<?> asStream(Object o) {
        return o instanceof Collection<?> c ? c.stream() : Stream.ofNullable(o);
    }
}
