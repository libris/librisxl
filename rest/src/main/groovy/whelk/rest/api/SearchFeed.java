package whelk.rest.api;

import whelk.Document;
import whelk.JsonLd;
import whelk.search2.AppParams;
import whelk.search2.Operator;
import whelk.util.FresnelUtil;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.REVERSE_KEY;
import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.asList;
import static whelk.JsonLd.looksLikeIri;

public class SearchFeed {
    private static final String ATOM_NS = "http://www.w3.org/2005/Atom";
    private static final String XHTML_NS = "http://www.w3.org/1999/xhtml";

    private static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newInstance();

    private static final Set<String> SKIP_KEYS =
            Set.of(ID_KEY, REVERSE_KEY, "meta", "reverseLinks", "_categoryByCollection");

    private final JsonLd jsonld;
    private final FresnelUtil fresnelUtil;
    private final List<String> locales;

    public SearchFeed(JsonLd jsonld, FresnelUtil fresnelUtil, List<String> locales) {
        this.jsonld = jsonld;
        this.fresnelUtil = fresnelUtil;
        this.locales = locales;
    }

    public String represent(String feedId, Object searchResults) {
        Map<String, Object> results = asMap(searchResults);
        List<Map<String, Object>> items = itemsOf(results);

        List<Instant> timestamps = items.stream()
                .map(SearchFeed::modifiedOf)
                .filter(Objects::nonNull)
                .map(Document::parseTimestamp)
                .sorted()
                .toList();
        String lastMod = Document.formatTimeStamp(timestamps.isEmpty() ? Instant.now() : timestamps.getLast());
        String feedTitle = buildTitle(results);

        StringWriter out = new StringWriter();
        try {
            XMLStreamWriter w = XML_OUTPUT_FACTORY.createXMLStreamWriter(out);

            w.setDefaultNamespace(ATOM_NS);
            w.writeStartElement(ATOM_NS, "feed");
            w.writeDefaultNamespace(ATOM_NS);

            writeElement(w, "title", feedTitle);
            writeElement(w, "id", feedId);
            w.writeStartElement(ATOM_NS, "author");
            writeElement(w, "name", "Libris");
            w.writeEndElement();
            writeLink(w, "self", str(results.get(ID_KEY)));
            for (String rel : List.of("next", "prev", "first", "last")) {
                Map<String, Object> ref = asMap(results.get(rel));
                if (ref != null) {
                    writeLink(w, rel, str(ref.get(ID_KEY)));
                }
            }
            writeElement(w, "updated", lastMod);

            for (Map<String, Object> item : items) {
                String itemId = str(item.get(ID_KEY));
                w.writeStartElement(ATOM_NS, "entry");
                writeElement(w, "id", itemId);
                w.writeStartElement(ATOM_NS, "link");
                w.writeAttribute("rel", "alternate");
                w.writeAttribute("type", "text/html");
                if (itemId != null) {
                    w.writeAttribute("href", itemId);
                }
                w.writeEndElement();
                writeElement(w, "updated", modifiedOf(item));
                writeElement(w, "title", toChipString(item));
                w.writeStartElement(ATOM_NS, "summary");
                w.writeAttribute("type", "xhtml");
                writeEntryCard(w, item);
                w.writeEndElement();
                w.writeStartElement(ATOM_NS, "content");
                w.writeAttribute("src", itemId);
                w.writeEndElement();
                w.writeEndElement();
            }

            w.writeEndElement();
            w.writeEndDocument();
            w.flush();
        } catch (XMLStreamException e) {
            throw new RuntimeException("Failed to build Atom feed", e);
        }

        return out.toString();
    }

    String buildTitle(Map<String, Object> searchResults) {
        String title = getByLang(asMap(searchResults.get("titleByLang")));

        Map<String, Object> search = asMap(searchResults.get("search"));
        List<String> params = new ArrayList<>();
        if (search != null) {
            for (Object m : asList(search.get("mapping"))) {
                String s = searchMappingToString(asMap(m));
                if (s != null) {
                    params.add(s);
                }
            }
        }

        return params.isEmpty() ? title : title + " | " + String.join(" ", params);
    }

    private void writeEntryCard(XMLStreamWriter w, Map<String, Object> item) throws XMLStreamException {
        w.setDefaultNamespace(XHTML_NS);
        w.writeStartElement(XHTML_NS, "div");
        w.writeDefaultNamespace(XHTML_NS);

        Map<String, Object> meta = asMap(item.get("meta"));
        for (Object note : asList(meta != null ? meta.get("hasChangeNote") : null)) {
            w.writeStartElement(XHTML_NS, "p");
            writeElement(w, XHTML_NS, "b", toChipString(note));
            w.writeEndElement();
        }

        if (item.get(TYPE_KEY) != null) {
            Map<String, Object> sorted = fresnelUtil.mapThroughLens(
                    item, FresnelUtil.NestedLenses.CARD_TO_CHIP_TO_TOKEN, List.of(), List.of());

            for (Map.Entry<String, Object> kv : sorted.entrySet()) {
                w.writeStartElement(XHTML_NS, "div");
                if (!SKIP_KEYS.contains(kv.getKey())) {
                    String label = getLabelFor(kv.getKey());
                    List<String> values = new ArrayList<>();
                    for (Object v : asList(kv.getValue())) {
                        String s = toValueString(v);
                        if (s != null && !s.isEmpty()) {
                            values.add(s);
                        }
                    }

                    if (label != null && !label.isEmpty() && !values.isEmpty()) {
                        w.writeStartElement(XHTML_NS, "span");
                        w.writeAttribute("style", "display: block; font-size: 0.75rem; margin-top: 0.5rem;");
                        writeElement(w, XHTML_NS, "span", label);
                        // fallback when no CSS support
                        w.writeStartElement(XHTML_NS, "span");
                        w.writeAttribute("style", "display: none");
                        w.writeCharacters(": ");
                        w.writeEndElement();
                        w.writeEndElement();

                        w.writeStartElement(XHTML_NS, "span");
                        for (int i = 0; i < values.size(); i++) {
                            w.writeStartElement(XHTML_NS, "span");
                            w.writeAttribute("style", "display: block");
                            if (i > 0) {
                                // fallback when no CSS support
                                w.writeStartElement(XHTML_NS, "span");
                                w.writeAttribute("style", "display: none");
                                w.writeCharacters(", ");
                                w.writeEndElement();
                            }
                            writeElement(w, XHTML_NS, "span", values.get(i));
                            w.writeEndElement();
                        }
                        w.writeEndElement();
                    }
                }
                w.writeEndElement();
            }
        }

        w.writeEndElement();
        w.setDefaultNamespace(ATOM_NS);
    }

    String toChipString(Object item) {
        return fresnelUtil.asFormattedString(item, FresnelUtil.NestedLenses.CHIP_TO_TOKEN, locales.getFirst());
    }

    String toValueString(Object item) {
        if (item instanceof Map<?, ?> m) {
            if (m.get(TYPE_KEY) != null) {
                return toChipString(item);
            }
            if (m.get(ID_KEY) != null) {
                return uriSlug(String.valueOf(m.get(ID_KEY)));
            }
        }

        return String.valueOf(item);
    }

    static String uriSlug(String s) {
        String[] parts = s.split("/");
        return parts[parts.length - 1];
    }

    String getLabelFor(String key) {
        String lookup = TYPE_KEY.equals(key) ? "rdf:type" : key;
        Map<String, Object> term = asMap(jsonld.vocabIndex.get(lookup));
        if (term != null) {
            Map<String, Object> byLang = asMap(term.get("labelByLang"));
            if (byLang != null) {
                String s = getByLang(byLang);
                if (s != null && !s.isEmpty()) {
                    return s.substring(0, 1).toUpperCase() + s.substring(1);
                }
            }
        }
        return key;
    }

    String getByLang(Map<String, Object> byLang) {
        if (byLang == null) {
            return null;
        }

        for (String lang : locales) {
            Object o = byLang.get(lang);
            if (o instanceof String s) {
                return s;
            } else if (o instanceof List<?> l && !l.isEmpty()) {
                return String.valueOf(l.getFirst());
            }
        }
        for (Object value : byLang.values()) {
            return (String) value;
        }
        return null;
    }

    String searchMappingToString(Map<String, Object> m) {
        if (AppParams.DEFAULT_SITE_FILTERS.equals(m.get("variable"))) {
            return null;
        }

        Object object = m.get("object");
        if (isTruthy(object)) {
            Map<String, Object> predicate = asMap(m.get("predicate"));
            if (predicate != null && isTruthy(predicate.get("label"))) {
                String l = (String) predicate.get("label");
                l = looksLikeIri(l) ? uriSlug(l) : l;
                return "& " + l + "=" + toValueString(object);
            }
            return toValueString(object);
        }

        Object value = m.get("value");
        if (isTruthy(value) && !(value instanceof Boolean)) {
            return toValueString(value);
        }

        for (Operator o : Operator.values()) {
            Object term = m.get(o.termKey);
            if (isTruthy(term)) {
                Map<String, Object> property = asMap(m.get("property"));
                if (property != null
                        && ("https://id.kb.se/vocab/textQuery".equals(property.get(ID_KEY))
                        || asList(property.get("category")).contains(Map.of(ID_KEY, "https://id.kb.se/vocab/impliedByObject")))) {
                    return toValueString(term);
                }

                return o.format(toValueString(m.get("property")), toValueString(term));
            }
        }

        if (isTruthy(m.get("and"))) {
            return "(" + joinMappings(m.get("and"), " AND ") + ")";
        }
        if (isTruthy(m.get("or"))) {
            return "(" + joinMappings(m.get("or"), " OR ") + ")";
        }
        if (isTruthy(m.get("not"))) {
            return "NOT " + searchMappingToString(asMap(m.get("not")));
        }

        return null;
    }

    private String joinMappings(Object mappings, String separator) {
        List<String> parts = new ArrayList<>();
        for (Object m : asList(mappings)) {
            parts.add(String.valueOf(searchMappingToString(asMap(m))));
        }
        return String.join(separator, parts);
    }

    private static String modifiedOf(Map<String, Object> item) {
        Map<String, Object> meta = item != null ? asMap(item.get("meta")) : null;
        return meta != null ? str(meta.get("modified")) : null;
    }

    private static List<Map<String, Object>> itemsOf(Map<String, Object> searchResults) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (Object item : asList(searchResults.get("items"))) {
            items.add(asMap(item));
        }
        return items;
    }

    /**
     * Mimics Groovy truth, which the original Groovy implementation relied on
     */
    private static boolean isTruthy(Object o) {
        return switch (o) {
            case null -> false;
            case String s -> !s.isEmpty();
            case Collection<?> c -> !c.isEmpty();
            case Map<?, ?> m -> !m.isEmpty();
            case Boolean b -> b;
            default -> true;
        };
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return o instanceof Map ? (Map<String, Object>) o : null;
    }

    private static String str(Object o) {
        return o != null ? String.valueOf(o) : null;
    }

    private static void writeElement(XMLStreamWriter w, String name, String text) throws XMLStreamException {
        writeElement(w, ATOM_NS, name, text);
    }

    private static void writeElement(XMLStreamWriter w, String ns, String name, String text) throws XMLStreamException {
        w.writeStartElement(ns, name);
        if (text != null) {
            w.writeCharacters(text);
        }
        w.writeEndElement();
    }

    private static void writeLink(XMLStreamWriter w, String rel, String href) throws XMLStreamException {
        w.writeStartElement(ATOM_NS, "link");
        w.writeAttribute("rel", rel);
        if (href != null) {
            w.writeAttribute("href", href);
        }
        w.writeEndElement();
    }
}
