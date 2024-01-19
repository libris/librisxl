package whelk.xlql;

import whelk.JsonLd;
import whelk.Whelk;

import java.util.*;
import java.util.stream.Collectors;

public class Elastify {
    // For testing
    public record ElastifiedField(String searchKey, String value) {}
    private JsonLd jsonLd;
    private Disambiguate disambiguate;
    private Map<String, String> domainByProperty;
    private Set<String> adminMetadataSubtypes;
    private Set<String> workSubtypes;
    private Set<String> instanceSubtypes;
    private Set<String> instanceCompatibleTypes;

    private static final String UNKNOWN = "Unknown";

    Elastify(Whelk whelk) {
        this.jsonLd = whelk.getJsonld();
        this.disambiguate = new Disambiguate(whelk);
        this.domainByProperty = loadDomainByProperty(whelk);
    }

    public Object reduceToAndOrTree(Object ast) {
        // For when grouping is possible in Elastic...
//        Set<String> givenProperties = collectGivenProperties(ast);
//        Set<String> givenTypes = givenProperties.contains(JsonLd.getTYPE_KEY()) ? collectGivenTypes(ast) : Collections.emptySet();
//        Set<String> domains = givenProperties.stream()
//                .map(p -> domainByProperty.getOrDefault(p, UNKNOWN))
//                .collect(Collectors.toSet());
//
//        Set<String> givenInstanceTypes = intersect(getInstanceSubtypes(), givenTypes);
//        Set<String> instanceCompatibleDomains = intersect(getInstanceCompatibleTypes(), domains);
//
//        boolean searchInstances = !givenInstanceTypes.isEmpty() || !instanceCompatibleDomains.isEmpty() || domains.contains(UNKNOWN);
        return reduceToAndOrTree(ast, false, false);
    }

    private Object reduceToAndOrTree(Object node, boolean searchInstances, boolean negate) {
        if (node instanceof Ast.And) {
            List<Object> operands = ((Ast.And) node).operands()
                    .stream()
                    .map(o -> reduceToAndOrTree(o, searchInstances, negate))
                    .toList();
            return negate ? new Ast.Or(operands) : new Ast.And(operands);
        } else if (node instanceof Ast.Or) {
            List<Object> operands = ((Ast.Or) node).operands()
                    .stream()
                    .map(o -> reduceToAndOrTree(o, searchInstances, negate))
                    .toList();
            return negate ? new Ast.And(operands) : new Ast.Or(operands);
        } else if (node instanceof Ast.Not) {
            return reduceToAndOrTree(((Ast.Not) node).operand(), searchInstances, !negate);
        }
//        else if (node instanceof Ast.Like) {
//            return new Ast.Like(collectSearchPathTree(((Ast.Like) node).operand(), searchInstances));
//        }
        else if (node instanceof Ast.Comp) {
            return elastifyCodeNode((Ast.Comp) node, searchInstances, negate);
        }
        return node;
    }

    private Object elastifyCodeNode(Ast.Comp node, boolean searchInstances, boolean negate) {
        String code = node.code();
        // TODO: Maybe not always String...
        String value = Disambiguate.expandPrefixed((String) node.operand()) ;
        Optional<String> operator = node instanceof Ast.CodeLesserGreaterThan
                ? Optional.of(((Ast.CodeLesserGreaterThan) node).operator())
                : Optional.empty();


        String property = disambiguate.mapToKbvProperty(code.toLowerCase());
        if (property == null) {
            // TODO: Handle bad code?
            return value;
        }

        Field field = new Field(property, value, operator, negate);
        field.expandChainAxiom(jsonLd);
        if (getAdminMetadataSubtypes().contains(getDomain(property))) {
            field.prependMeta();
        }
        if (JsonLd.looksLikeIri(value)) {
            field.appendId();
        }

        // TODO: More alternate paths...
        //  If value is a literal then label, code, prefLabel etc. (or just _str?)
        //  If value is a link then e.g. exactMatch
        if (searchInstances) {
            List<ElastifiedField> alternatePaths = collectAlternateFields(field).stream()
                    .map(f -> new ElastifiedField(f.buildSearchKey(), f.value))
                    .toList();
            return new Ast.Or(Collections.singletonList(alternatePaths));
        } else {
            return new ElastifiedField(field.buildSearchKey(), field.value);
        }
    }

    private List<Field> collectAlternateFields(Field field) {
        String domain = domainByProperty.get(field.property);

        boolean isInstanceBound = domain != null && instanceSubtypes.contains(domain);
        boolean isWorkBound = domain != null && workSubtypes.contains(domain);

        List<Field> fields = new ArrayList<>(Arrays.asList(field));

        if (isInstanceBound) {
            Field copy = field.copy();
            copy.setInstanceToWorkPath();
            fields.add(copy);
        } else if (isWorkBound) {
            Field copy = field.copy();
            copy.setWorkToInstancePath();
            fields.add(copy);
        } else {
            Field copy1 = field.copy();
            copy1.setInstanceToWorkPath();
            fields.add(copy1);
            Field copy2 = field.copy();
            copy2.setWorkToInstancePath();
            fields.add(copy2);
        }

        return fields;
    }

    public Set<String> collectGivenProperties(Object ast) {
        return collectGivenPropertyAliases(ast).stream()
                .map(String::toLowerCase)
                .map(disambiguate::mapToKbvProperty)
                // TODO: Abort if any bad alias?
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    private static Set<String> collectGivenPropertyAliases(Object ast) {
        Set<String> aliases = new HashSet<>();
        collectGivenPropertyAliases(ast, aliases);
        return aliases;
    }

    public Set<String> collectGivenTypes(Object ast) {
        Set<String> givenTypes = new HashSet<>();
        collectGivenTypes(ast, givenTypes);
        return givenTypes;
    }

    private static void collectGivenPropertyAliases(Object ast, Set<String> properties) {
        if (ast instanceof Ast.And) {
            ((Ast.And) ast).operands().forEach(o -> collectGivenPropertyAliases(o, properties));
        } else if (ast instanceof Ast.Or) {
            ((Ast.Or) ast).operands().forEach(o -> collectGivenPropertyAliases(o, properties));
        } else if (ast instanceof Ast.Not) {
            collectGivenPropertyAliases(((Ast.Not) ast).operand(), properties);
        } else if (ast instanceof Ast.Like) {
            collectGivenPropertyAliases(((Ast.Like) ast).operand(), properties);
        } else if (ast instanceof Ast.CodeEquals) {
            properties.add(((Ast.CodeEquals) ast).code());
        } else if (ast instanceof Ast.CodeLesserGreaterThan) {
            properties.add(((Ast.CodeLesserGreaterThan) ast).code());
        }
    }

    private void collectGivenTypes(Object ast, Set<String> types) {
        if (ast instanceof Ast.And) {
            ((Ast.And) ast).operands().forEach(o -> collectGivenTypes(o, types));
        } else if (ast instanceof Ast.Or) {
            ((Ast.Or) ast).operands().forEach(o -> collectGivenTypes(o, types));
        } else if (ast instanceof Ast.Not) {
            collectGivenTypes(((Ast.Not) ast).operand(), types);
        } else if (ast instanceof Ast.Like) {
            collectGivenTypes(((Ast.Like) ast).operand(), types);
        } else if (ast instanceof Ast.CodeEquals) {
            Ast.CodeEquals codeEquals = (Ast.CodeEquals) ast;
            String code = codeEquals.code();
            String property = disambiguate.mapToKbvProperty(code);
            if (JsonLd.getTYPE_KEY().equals(property)) {
                // TODO: Handle non-String operand?
                types.add((String) codeEquals.operand());
            }
        } else if (ast instanceof Ast.CodeLesserGreaterThan) {
            Ast.CodeLesserGreaterThan codeLesserGreaterThan = (Ast.CodeLesserGreaterThan) ast;
            String code = codeLesserGreaterThan.code();
            String property = disambiguate.mapToKbvProperty(code);
            if (JsonLd.getTYPE_KEY().equals(property)) {
                types.add(codeLesserGreaterThan.operand());
            }
        }
    }

    public String getDomain(String property) {
        return domainByProperty.get(property);
    }

    private Map<String, String> loadDomainByProperty(Whelk whelk) {
        Map<String, String> domainByProperty = new TreeMap<>();
        jsonLd.getVocabIndex().entrySet()
                .stream()
                .filter(e -> disambiguate.isKbvTerm(e.getValue()) && disambiguate.isProperty(e.getValue()))
                .forEach(e -> findDomain(e.getValue(), whelk)
                        .ifPresent(domain -> domainByProperty.put(jsonLd.toTermKey(e.getKey()), jsonLd.toTermKey(domain)))
                );
        return domainByProperty;
    }

    // TODO: BFS + review order
    private Optional<String> findDomain(Map propDefinition, Whelk whelk) {
        if (propDefinition.containsKey("domain")) {
            return Optional.of(propDefinition.get("domain"))
                    .map(d -> ((List) d).get(0))
                    .map(link -> (String) ((Map) link).get(JsonLd.getID_KEY()));
        } else if (propDefinition.containsKey("subPropertyOf")) {
            List<Map> subProperty = (List<Map>) propDefinition.get("subPropertyOf");
            for (Map sp : subProperty) {
                Optional<Map> spDefinition = getDefinition(sp, whelk);
                if (spDefinition.isPresent()) {
                    Optional<String> domain = findDomain(spDefinition.get(), whelk);
                    if (domain.isPresent()) {
                        return domain;
                    }
                }
            }
        } else if (propDefinition.containsKey("equivalentProperty")) {
            List<Map> equivProperty = (List<Map>) propDefinition.get("equivalentProperty");
            for (Map ep : equivProperty) {
                Optional<Map> epDefinition = getDefinition(ep, whelk);
                if (epDefinition.isPresent()) {
                    Optional<String> domain = findDomain(epDefinition.get(), whelk);
                    if (domain.isPresent()) {
                        return domain;
                    }
                }
            }
        } else if (propDefinition.containsKey("propertyChainAxiom")) {
            List pca = (List) propDefinition.get("propertyChainAxiom");
            Map first = (Map) pca.get(0);
            Optional<Map> pcaDefinition = getDefinition(first, whelk);
            if (pcaDefinition.isPresent()) {
                return findDomain(pcaDefinition.get(), whelk);
            }
        }

        return Optional.empty();
    }

    Optional<Map> getDefinition(Map object, Whelk whelk) {
        if (!object.containsKey(JsonLd.getID_KEY())) {
            return Optional.of(object);
        }
        String propId = (String) object.get(JsonLd.getID_KEY());
        String propKey = jsonLd.toTermKey(propId);
        Optional<Map> propDefinition = Optional.ofNullable(jsonLd.getVocabIndex().get(propKey));
        return propDefinition.isPresent()
                ? propDefinition
                : Optional.ofNullable(whelk.loadData(propId))
                .map(data -> data.get(JsonLd.getGRAPH_KEY()))
                .map(graph -> (Map) ((List) graph).get(1));
    }

    private Set<String> intersect(Collection<String> a, Collection<String> b) {
        return a.stream().filter(x -> b.contains(x)).collect(Collectors.toSet());
    }

    private Set<String> getInstanceCompatibleTypes() {
        if (instanceCompatibleTypes == null) {
            this.instanceCompatibleTypes = new HashSet<>();
            List<String> superClasses = new ArrayList<>();
            jsonLd.getSuperClasses("Instance", superClasses);
            instanceCompatibleTypes.addAll(superClasses);
            instanceCompatibleTypes.addAll(getInstanceSubtypes());
            instanceCompatibleTypes.add("Instance");
        }
        return instanceCompatibleTypes;
    }

    private Set<String> getWorkSubtypes() {
        if (workSubtypes == null) {
            this.workSubtypes = jsonLd.getSubClasses("Work");
            workSubtypes.add("Work");
        }
        return workSubtypes;
    }

    private Set<String> getInstanceSubtypes() {
        if (instanceSubtypes == null) {
            this.instanceSubtypes = jsonLd.getSubClasses("Instance");
            instanceSubtypes.add("Instance");
        }
        return instanceSubtypes;
    }

    private Set<String> getAdminMetadataSubtypes() {
        if (adminMetadataSubtypes == null) {
            this.adminMetadataSubtypes = jsonLd.getSubClasses("AdminMetadata");
            adminMetadataSubtypes.add("AdminMetadata");
        }
        return adminMetadataSubtypes;
    }
}
