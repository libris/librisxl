package whelk.xlql;

import whelk.JsonLd;
import whelk.Whelk;

import java.util.*;
import java.util.stream.Collectors;

public class QueryTreeBuilder {
    public record And(List<Object> conjuncts) {
    }

    public record Or(List<Object> disjuncts) {
    }

    public record FreeText(String value, Operator operator) {
    }

    public record Field(Path path, Operator operator, String value) {
    }

    public record SimpleField(String property, Operator operator, String value) {
    }

    public enum Operator {
        EQUALS,
        NOT_EQUALS,
        GREATER_THAN_OR_EQUAL,
        GREATER_THAN,
        LESS_THAN_OR_EQUAL,
        LESS_THAN;
    }

    private JsonLd jsonLd;
    private Disambiguate disambiguate;
    private Map<String, String> domainByProperty;
    private Set<String> adminMetadataSubtypes;
    private Set<String> workSubtypes;
    private Set<String> instanceSubtypes;
    private Set<String> instanceCompatibleTypes;

    private static final String UNKNOWN = "Unknown";

    public QueryTreeBuilder(Whelk whelk) {
        this.jsonLd = whelk.getJsonld();
        this.disambiguate = new Disambiguate(whelk);
        this.domainByProperty = loadDomainByProperty(whelk);
    }

    public Object toQueryTree(Object disambiguatedAst) {
        return astToQt(disambiguatedAst);
    }

    public Object toSimpleQueryTree(Object disambiguatedAst) {
        return astToSimpleQt(disambiguatedAst);
    }

    public Object toDisambiguatedAst(String queryString) throws BadQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        Object ast = Ast.buildFrom(parseTree);
        // TODO: Review necessity of flattenCodes/flattenNegations
        ast = Analysis.flattenCodes(ast);
        ast = Analysis.flattenNegations(ast);
        return disambiguate.toDisambiguatedTree(ast);
    }

    private Object astToQt(Object ast) {
        Set<String> givenProperties = collectGivenProperties(ast);
        Set<String> givenTypes = givenProperties.contains(JsonLd.getTYPE_KEY()) ? collectGivenTypes(ast) : Collections.emptySet();
        Set<String> domains = givenProperties.stream()
                .map(p -> domainByProperty.getOrDefault(p, UNKNOWN))
                .collect(Collectors.toSet());

        Set<String> givenInstanceTypes = intersect(getInstanceSubtypes(), givenTypes);
        Set<String> instanceCompatibleDomains = intersect(getInstanceCompatibleTypes(), domains);

        boolean searchInstances = !givenInstanceTypes.isEmpty() || !instanceCompatibleDomains.isEmpty() || domains.contains(UNKNOWN);

//        return astToQt(ast, searchInstances, false);
        return astToQt(ast, false, false);
    }

    private Object astToSimpleQt(Object ast) {
        return astToQt(ast, false, true);
    }

    private Object astToQt(Object node, boolean searchInstances, boolean simpleTree) {
        // Assuming flattened codes and negations
        if (node instanceof Ast.And) {
            List<Object> conjuncts = new ArrayList<>();
            for (Object o : ((Ast.And) node).operands()) {
                conjuncts.add(astToQt(o, searchInstances, simpleTree));
            }
            return new And(conjuncts);
        } else if (node instanceof Ast.Or) {
            List<Object> disjuncts = new ArrayList<>();
            for (Object o : ((Ast.Or) node).operands()) {
                disjuncts.add(astToQt(o, searchInstances, simpleTree));
            }
            return new Or(disjuncts);
        }
//        else if (node instanceof Ast.Like) {
//            return new Ast.Like(collectSearchPathTree(((Ast.Like) node).operand(), searchInstances));
//        }
        else if (node instanceof Ast.Comp) {
            Ast.Comp compNode = (Ast.Comp) node;
            return simpleTree
                    ? new SimpleField(compNode.code(), getOperator(compNode), (String) compNode.operand())
                    : compNodeToField(compNode, searchInstances);
        } else if (node instanceof Ast.Not) {
            String value = (String) ((Ast.Not) node).operand();
            return new FreeText(value, Operator.NOT_EQUALS);
        }
        return new FreeText((String) node, Operator.EQUALS);
    }

    private Object compNodeToField(Ast.Comp node, boolean searchInstances) {
        String property = node.code();
        String value = (String) node.operand();
        Operator operator = getOperator(node);

        Path path = new Path(property);
        path.expandChainAxiom(jsonLd);
        if (getAdminMetadataSubtypes().contains(getDomain(property))) {
            path.prependMeta();
        }

        List<Object> searchFields = new ArrayList<>(List.of(new Field(path, operator, value)));

        // TODO: @type not in vocab, needs special handling
        Map propertyDefinition = jsonLd.getVocabIndex().get(property);
        if (Disambiguate.isObjectProperty(propertyDefinition)) {
            String expanded = Disambiguate.expandPrefixed(value);
            /*
            Add ._str or .@id as an alternative paths but keep the "normal" path since sometimes the value of ObjectProperty
            is a string, e.g. issuanceType: "Serial" or encodingLevel: "marc:FullLevel".
            (Can we skip either path with better disambiguation?)
             */
            if (JsonLd.looksLikeIri(expanded)) {
                Path copy = path.copy();
                copy.appendId();
                searchFields.add(new Field(copy, operator, expanded));
            } else {
                Path copy = path.copy();
                copy.appendUnderscoreStr();
                searchFields.add(new Field(copy, operator, value));
            }
        }

        if (searchInstances) {
            List<Field> instanceVsWorkFields = new ArrayList<>();
            for (Object sf : searchFields) {
                Field field = (Field) sf;
                collectInstanceVsWorkPaths(field.path()).stream()
                        .map(p -> new Field(p, field.operator(), field.value()))
                        .forEach(f -> instanceVsWorkFields.add(f));
            }
            searchFields.addAll(instanceVsWorkFields);
        }

        if (searchFields.size() == 1) {
            return searchFields.get(0);
        }
        return operator == Operator.NOT_EQUALS ? new And(searchFields) : new Or(searchFields);
    }

    private Operator getOperator(Ast.Comp node) throws RuntimeException {
        if (node instanceof Ast.CodeLesserGreaterThan) {
            String operator = ((Ast.CodeLesserGreaterThan) node).operator();
            if (">".equals(operator)) {
                return Operator.GREATER_THAN;
            } else if (">=".equals(operator)) {
                return Operator.GREATER_THAN_OR_EQUAL;
            } else if ("<".equals(operator)) {
                return Operator.LESS_THAN;
            } else if ("<=".equals(operator)) {
                return Operator.LESS_THAN_OR_EQUAL;
            }
        } else if (node instanceof Ast.NotCodeEquals) {
            return Operator.NOT_EQUALS;
        } else if (node instanceof Ast.CodeEquals) {
            return Operator.EQUALS;
        }
        throw new RuntimeException("Unable to decide operator for AST node with code: " + node.code()); // Shouldn't be reachable
    }

    private List<Path> collectInstanceVsWorkPaths(Path p) {
        String domain = domainByProperty.get(p.property);

        boolean isInstanceBound = domain != null && getInstanceSubtypes().contains(domain);
        boolean isWorkBound = domain != null && getWorkSubtypes().contains(domain);

        List<Path> paths = new ArrayList<>();

        if (isInstanceBound) {
            Path copy = p.copy();
            copy.setWorkToInstancePath();
            paths.add(copy);
        } else if (isWorkBound) {
            Path copy = p.copy();
            copy.setInstanceToWorkPath();
            paths.add(copy);
        } else {
            Path copy1 = p.copy();
            copy1.setInstanceToWorkPath();
            paths.add(copy1);
            Path copy2 = p.copy();
            copy2.setWorkToInstancePath();
            paths.add(copy2);
        }

        return paths;
    }

    public static Set<String> collectGivenProperties(Object ast) {
        return collectGivenProperties(ast, new HashSet<>());
    }

    public static Set<String> collectGivenTypes(Object ast) {
        Set<String> givenTypes = new HashSet<>();
        collectGivenTypes(ast, givenTypes);
        return givenTypes;
    }

    private static Set<String> collectGivenProperties(Object ast, Set<String> properties) {
        if (ast instanceof Ast.And) {
            ((Ast.And) ast).operands().forEach(o -> collectGivenProperties(o, properties));
        } else if (ast instanceof Ast.Or) {
            ((Ast.Or) ast).operands().forEach(o -> collectGivenProperties(o, properties));
        } else if (ast instanceof Ast.Not) {
            collectGivenProperties(((Ast.Not) ast).operand(), properties);
        } else if (ast instanceof Ast.Like) {
            collectGivenProperties(((Ast.Like) ast).operand(), properties);
        } else if (ast instanceof Ast.CodeEquals) {
            properties.add(((Ast.CodeEquals) ast).code());
        } else if (ast instanceof Ast.CodeLesserGreaterThan) {
            properties.add(((Ast.CodeLesserGreaterThan) ast).code());
        }
        return properties;
    }

    private static Set<String> collectGivenTypes(Object ast, Set<String> types) {
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
            String property = codeEquals.code();
            if (JsonLd.getTYPE_KEY().equals(property)) {
                // Assume operand is String
                types.add((String) codeEquals.operand());
            }
        } else if (ast instanceof Ast.CodeLesserGreaterThan) {
            Ast.CodeLesserGreaterThan codeLesserGreaterThan = (Ast.CodeLesserGreaterThan) ast;
            String property = codeLesserGreaterThan.code();
            if (JsonLd.getTYPE_KEY().equals(property)) {
                types.add(codeLesserGreaterThan.operand());
            }
        }
        return types;
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
