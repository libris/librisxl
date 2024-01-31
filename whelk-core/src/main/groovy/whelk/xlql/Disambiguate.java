package whelk.xlql;

import whelk.JsonLd;
import whelk.Whelk;

import java.util.*;

// TODO: Disambiguate values too (not only properties)
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

    public Object toDisambiguatedTree(Object ast) throws BadQueryException {
        if (ast instanceof Ast.And) {
            List<Object> operands = new ArrayList<>();
            for (Object o : ((Ast.And) ast).operands()) {
                operands.add(toDisambiguatedTree(o));
            }
            return new Ast.And(operands);
        }
        if (ast instanceof Ast.Or) {
            List<Object> operands = new ArrayList<>();
            for (Object o : ((Ast.Or) ast).operands()) {
                operands.add(toDisambiguatedTree(o));
            }
            return new Ast.Or(operands);
        }
        if (ast instanceof Ast.Not) {
            return new Ast.Not(toDisambiguatedTree(((Ast.Not) ast).operand()));
        }
        if (ast instanceof Ast.Like) {
            return new Ast.Like(toDisambiguatedTree(((Ast.Like) ast).operand()));
        }
        if (ast instanceof Ast.CodeEquals) {
            Ast.CodeEquals ce = (Ast.CodeEquals) ast;
            String kbvProperty = mapToKbvProperty(ce.code());
            if (kbvProperty == null) {
                throw new BadQueryException("Unrecognized property alias: " + ce.code());
            }
            return new Ast.CodeEquals(kbvProperty, ce.operand());
        }
        if (ast instanceof Ast.NotCodeEquals) {
            Ast.NotCodeEquals nce = (Ast.NotCodeEquals) ast;
            String kbvProperty = mapToKbvProperty(nce.code());
            if (kbvProperty == null) {
                throw new BadQueryException("Unrecognized property alias: " + nce.code());
            }
            return new Ast.NotCodeEquals(kbvProperty, nce.operand());
        }
        if (ast instanceof Ast.CodeLesserGreaterThan) {
            Ast.CodeLesserGreaterThan clgt = (Ast.CodeLesserGreaterThan) ast;
            String kbvProperty = mapToKbvProperty(clgt.code());
            if (kbvProperty == null) {
                throw new BadQueryException("Unrecognized property alias: " + clgt.code());
            }
            return new Ast.CodeLesserGreaterThan(kbvProperty, clgt.operator(), clgt.operand());
        }
        // String
        return ast;
    }

    public String mapToKbvProperty(String alias) {
        return propertyAliasMappings.get(alias.toLowerCase());
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
        return isObjectProperty(termDefinition) || isDatatypeProperty(termDefinition);
    }

    public static boolean isObjectProperty(Map termDefinition) {
        Object type = termDefinition.get(JsonLd.getTYPE_KEY());
        return "ObjectProperty".equals(type);
    }

    public static boolean isDatatypeProperty(Map termDefinition) {
        Object type = termDefinition.get(JsonLd.getTYPE_KEY());
        return "DatatypeProperty".equals(type);
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
