package whelk.xlql;

import whelk.JsonLd;
import whelk.Whelk;

import java.util.*;

public class Disambiguate {
    private JsonLd jsonLd;

    // :category :heuristicIdentifier too broad...?
    private Set<String> notatingProps = new HashSet<>(Arrays.asList("label", "prefLabel", "altLabel", "code", "librisQueryCode"));
    private Map<String, String> propertyAliasMappings;
    // TODO: Handle ambiguous aliases
    private Map<String, Set<String>> ambiguousPropertyAliases;

    Disambiguate(Whelk whelk) {
        this.jsonLd = whelk.getJsonld();
        loadPropertyMappings(whelk);
    }

    public String mapToKbvProperty(String alias) {
        return propertyAliasMappings.get(alias);
    }

    private void loadPropertyMappings(Whelk whelk) {
        this.propertyAliasMappings = new TreeMap<>();
        this.ambiguousPropertyAliases = new TreeMap<>();

        Map<String, Map> vocab = jsonLd.getVocabIndex();

        // Hardcoding these for now...
        addMapping("type", "@type");
        addMapping("typ", "@type");
        addMapping("rdf:type", "@type");

        for (String termKey : vocab.keySet()) {
            Map termDefinition = vocab.get(termKey);
            if (isKbvTerm(termDefinition) && isProperty(termDefinition)) {
                addMapping(termKey, termKey);
                addMappings(termDefinition, termKey);
                if (termDefinition.containsKey("equivalentProperty")) {
                    List<Map> equivProperty = (List<Map>) termDefinition.get("equivalentProperty");
                    for (Map ep : equivProperty) {
                        String equivPropId = (String) ep.get(JsonLd.getID_KEY());
                        String equivPropKey = jsonLd.toTermKey(equivPropId);
                        if (!vocab.containsKey(equivPropKey)) {
                            Map equivPropData = whelk.loadData(equivPropKey);
                            if (equivPropData == null) {
                                addMapping(equivPropId, termKey);
                                addMapping(toPrefixed(equivPropId), termKey);
                            } else {
                                List graph = (List) equivPropData.get(JsonLd.getGRAPH_KEY());
                                Map equivPropDefinition = (Map) graph.get(1);
                                addMappings(equivPropDefinition, termKey);
                            }
                        }
                    }
                }
            }
        }
    }

    private void addMappings(Map fromTermData, String toTermKey) {
        String fromTermId = (String) fromTermData.get(JsonLd.getID_KEY());
        addMapping(fromTermId, toTermKey);
        addMapping(toPrefixed(fromTermId), toTermKey);
        for (String prop : notatingProps) {
            if (fromTermData.containsKey(prop)) {
                addMapping((String) fromTermData.get(prop), toTermKey);
            }
            String alias = (String) jsonLd.getLangContainerAlias().get(prop);
            if (fromTermData.containsKey(alias)) {
                Map byLang = (Map) fromTermData.get(alias);
                for (String lang : jsonLd.getLocales()) {
                    List values = JsonLd.asList(byLang.get(lang));
                    values.forEach(v -> addMapping((String) v, toTermKey));
                }
            }
        }
    }

    private void addMapping(String from, String to) {
        from = from.toLowerCase();
        if (ambiguousPropertyAliases.containsKey(from)) {
            ambiguousPropertyAliases.get(from).add(to);
        } else if (propertyAliasMappings.containsKey(from)) {
            if (propertyAliasMappings.get(from).equals(to)) {
                return;
            }
            ambiguousPropertyAliases.put(from, new HashSet<>(Arrays.asList(to, propertyAliasMappings.remove(from))));
        } else {
            propertyAliasMappings.put(from, to);
        }
    }

    public static boolean isKbvTerm(Map termDefinition) {
        Map definedBy = (Map) termDefinition.get("isDefinedBy");
        return definedBy != null && definedBy.get("@id").equals("https://id.kb.se/vocab/");
    }

    public static boolean isProperty(Map termDefinition) {
        Object type = termDefinition.get(JsonLd.getTYPE_KEY());
        // TODO: type subClassOf rdf:Property?
        return "ObjectProperty".equals(type) || "DatatypeProperty".equals(type);
    }

    public static String toPrefixed(String iri) {
        // TODO: get prefix mappings from context
        Map<String, String> nsToPrefix = new HashMap<>();
        nsToPrefix.put("https://id.kb.se/vocab/", "kbv:");
        nsToPrefix.put("http://id.loc.gov/ontologies/bibframe/", "bf:");
        nsToPrefix.put("http://purl.org/dc/terms/", "dc:");
        nsToPrefix.put("http://schema.org/", "sdo:");
        nsToPrefix.put("https://id.kb.se/term/sao/", "sao:");

        for (String ns : nsToPrefix.keySet()) {
            if (iri.startsWith(ns)) {
                return iri.replace(ns, nsToPrefix.get(ns));
            }
        }

        return iri;
    }

    public static String expandPrefixed(String s) {
        if (!s.contains(":")) {
            return s;
        }
        // TODO: get prefix mappings from context
        Map<String, String> nsToPrefix = new HashMap<>();
        nsToPrefix.put("https://id.kb.se/vocab/", "kbv:");
        nsToPrefix.put("http://id.loc.gov/ontologies/bibframe/", "bf:");
        nsToPrefix.put("http://purl.org/dc/terms/", "dc:");
        nsToPrefix.put("http://schema.org/", "sdo:");
        nsToPrefix.put("https://id.kb.se/term/sao/", "sao:");

        for (String ns : nsToPrefix.keySet()) {
            String prefix = nsToPrefix.get(ns);
            if (s.startsWith(prefix)) {
                return s.replace(prefix, ns);
            }
        }

        return s;
    }
}
