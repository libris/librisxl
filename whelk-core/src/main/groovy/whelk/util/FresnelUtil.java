package whelk.util;

import whelk.JsonLd;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class FresnelUtil {

    // full = full, card ?
    // card = card
    // chip = chip
    // token = token, chip
    JsonLd jsonLd;

    //TODO
    Map<?, ?> DEFAULT_LENS = Map.of(
            JsonLd.TYPE_KEY, Fresnel.Lens,
            Fresnel.showProperties, List.of(
                    Map.of(Fresnel.alternateProperties, List.of(
                            "prefLabel", "label", "name", "@id"
                    ))
            )
    );

    enum LensLevel {
        Full(List.of("full", "cards")),
        Card(List.of("cards")),
        Chip(List.of("chips")),
        Token(List.of("tokens", "chips"));

        final List<String> groups;

        LensLevel(List<String> groups) {
            this.groups = groups;
        }
    }

    FresnelUtil(JsonLd jsonLd) {
        this.jsonLd = jsonLd;
    }

    Object applyLens(Object thing, String lensGroupName) {

    }

    private Object applyLens(
            Object value,
            LensLevel lensLevel,
            LangCode langAndScript
    ) {
        if (!(value instanceof Map<?, ?> thing)) {
            // literal
            return value;
        }

        List<LangCode> scripts = langAndScript == null ? Collections.emptyList() : scriptAlternatives(thing);
        if (!scripts.isEmpty()) {
            langAndScript = scripts.getFirst();
        }

        var result = new HashMap<String, Object>();
        var lens = findLens(thing, lensLevel);

        @SuppressWarnings("unchecked")
        var showProperties = (List<Object>) lens.get(Fresnel.showProperties);
        for (var p : Stream.concat(Stream.of(JsonLd.TYPE_KEY, JsonLd.ID_KEY), showProperties.stream()).toList()) {
            if (JsonLd.isAlternateProperties(p)) {

            }
            else if (isInverseProperty(p)) {
                // TODO
            }
            else if (p instanceof String k){
                if (has(thing, k)) {
                    pick(result, thing, k);
                }
            }
        }


        return result;
    }

    class Result {
        String type;
        List<Map<?, ?>> props = new ArrayList();
    }

    boolean has(Map<?, ?> thing, String key) {
        return thing.containsKey(key)
                || (jsonLd.langContainerAlias.containsKey(key) && thing.containsKey(jsonLd.langContainerAlias.get(key)));
    }

    void pick(Map<?, ?> result, Map<?, ?> thing, String key) {
        if (key in )
    }

    Map<?, ?> findLens(Map<?,?> thing, LensLevel lensLevel) {
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

    LensLevel subLens(LensLevel lensLevel) {
        return switch (lensLevel) {
            case Full -> LensLevel.Card;
            case Card -> LensLevel.Chip;
            case Chip, Token -> LensLevel.Token;
        };
    }





    interface PropertyDescription {

    }

    //record Literal(Object o) { }
    //record

    class SimpleProperty implements PropertyDescription {}
    class AlternateProperties implements PropertyDescription {}
    class RangeRestriction implements PropertyDescription {}



    class Lens {
        String group;
        String classLensDomain;
        List<PropertyDescription> showProperties;
    }

    public FresnelUtil(Map displayData) {

    }

    public class DisplayDecorated {}

    public class LensedOrdered {}

    @FunctionalInterface
    interface Ack<T> {
        void ack(T result, String propertyName, Object value);
    }

    @FunctionalInterface
    interface SubLensSelector {
        String select(String lensType, String propertyName);
    }

    List<LangCode> scriptAlternatives(Map<?,?> thing) {
        var shouldBeGrouped = isTypedNode(thing) && (isStructuredValue(thing) || isIdentity(thing));

        // TODO
        return Collections.emptyList();
    }

    boolean isStructuredValue(Map<?,?> thing) {
        return jsonLd.isSubClassOf((String) thing.get(JsonLd.TYPE_KEY), Base.StructuredValue);
    }

    boolean isIdentity(Map<?,?> thing) {
        return jsonLd.isSubClassOf((String) thing.get(JsonLd.TYPE_KEY), Base.Identity);
    }

    boolean isInverseProperty(Object showProperty) {
        return showProperty instanceof Map && ((Map<?, ?>) showProperty).containsKey("inverseOf");
    }

    record LangCode(String code) {}

/*
    public DisplayDecorated format() {

    }
*/
    private boolean isTypedNode(Object o) {
        return o instanceof Map && ((Map<?, ?>) o).containsKey(JsonLd.TYPE_KEY);
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
    }

    // https://github.com/libris/definitions/blob/develop/source/vocab/base.ttl
    static class Base {
        public static String Resource = "Resource";
        public static String StructuredValue = "StructuredValue";
        public static String Identity = "Identity";
    }

    static class Fmt {
        public static String DISPLAY = "_display";
        public static String PROPS = "_props";
        public static String CONTENT_AFTER = "_contentAfter";
        public static String CONTENT_BEFORE = "_contentBefore";
        public static String STYLE = "_style";
        public static String LABEL = "_label";
    }
}
