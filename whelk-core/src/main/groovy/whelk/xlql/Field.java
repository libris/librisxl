package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;

public class Field {
    public String property;
    public String value;
    public Optional<String> operator;
    public boolean negate = false;

    public List<String> path;

    private enum ElasticOperator {
        AND("and-"),
        OR("or-"),
        NOT("not-"),
        GREATER_THAN_OR_EQUAL("min-"),
        GREATER_THAN("minEx-"),
        LESS_THAN_OR_EQUAL("max-"),
        LESS_THAN("maxEx-");

        String prefix;

        private ElasticOperator(String prefix) {
            this.prefix = prefix;
        }
    }

    Map<String, ElasticOperator> operatorMappings;
    Map<ElasticOperator, ElasticOperator> operatorOpposites;

    Field(String property, String value, Optional<String> operator, boolean negate) {
        this.property = property;
        this.path = new ArrayList<>(Arrays.asList(property));
        this.value = value;
        this.operator = operator;
        this.negate = negate;
    }

    Field(String property, String value, Optional<String> operator, List<String> path, boolean negate) {
        this.property = property;
        this.path = new ArrayList<>(path);
        this.value = value;
        this.operator = operator;
        this.negate = negate;
    }

    Field(Field field) {
        this(field.property, field.value, field.operator, field.path, field.negate);
    }

    String buildSearchKey() {
        String pathStr = toString(path);
        if (operator.isPresent() && negate) {
            ElasticOperator eop = getOperatorMappings().get(operator.get());
            ElasticOperator reverseComp = getOperatorOpposites().get(eop);
            return reverseComp.prefix + pathStr;
        } else if (operator.isPresent()) {
            return getOperatorMappings().get(operator.get()).prefix + pathStr;
        } else if (negate) {
            return ElasticOperator.NOT.prefix + pathStr;
        }
        return pathStr;
    }

    Field copy() {
        return new Field(this);
    }

    public void prependMeta() {
        path.add(0, JsonLd.getRECORD_KEY());
    }

    public void appendId() {
        path.add(JsonLd.getID_KEY());
    }

    public void setWorkToInstancePath() {
        path.set(0, JsonLd.getWORK_KEY());
        path.set(0, JsonLd.getREVERSE_KEY());
    }

    public void setInstanceToWorkPath() {
        path.set(0, JsonLd.getWORK_KEY());
        path.set(0, JsonLd.getREVERSE_KEY());
    }

    // TODO: Handle owl:Restriction
    public void expandChainAxiom(JsonLd jsonLd) {
        List<String> extended = new ArrayList<>();

        for (String p : path) {
            Map<String, Object> termDefinition = jsonLd.getVocabIndex().get(p);
            if (!termDefinition.containsKey("propertyChainAxiom")) {
                extended.add(p);
                continue;
            }
            List<Map> pca = (List<Map>) termDefinition.get("propertyChainAxiom");
            for (Map prop : pca) {
                boolean added = false;
                if (prop.containsKey(JsonLd.getID_KEY())) {
                    String propId = (String) prop.get(JsonLd.getID_KEY());
                    added = extended.add(jsonLd.toTermKey(propId));
                } else if (prop.containsKey("subPropertyOf")) {
                    List superProp = (List) prop.get("subPropertyOf");
                    if (superProp.size() == 1) {
                        Map superPropLink = (Map) superProp.get(0);
                        if (superPropLink.containsKey(JsonLd.getID_KEY())) {
                            String superPropId = (String) superPropLink.get(JsonLd.getID_KEY());
                            added = extended.add(jsonLd.toTermKey(superPropId));
                        }
                    }
                }
                if (!added) {
                    System.out.println("Failed to expand chain axiom for property " + p);
                    return;
                }
            }
        }

        this.path = extended;
    }

    static String toString(List<String> p) {
        return p.stream().collect(Collectors.joining("."));
    }

    Map<String, ElasticOperator> getOperatorMappings() {
        if (operatorMappings == null) {
            Map<String, ElasticOperator> m = new HashMap<>();
            m.put(">=", ElasticOperator.GREATER_THAN_OR_EQUAL);
            m.put(">", ElasticOperator.GREATER_THAN);
            m.put("<=", ElasticOperator.LESS_THAN_OR_EQUAL);
            m.put("<", ElasticOperator.LESS_THAN);
            this.operatorMappings = m;
        }
        return operatorMappings;
    }

    Map<ElasticOperator, ElasticOperator> getOperatorOpposites() {
        if (operatorOpposites == null) {
            Map<ElasticOperator, ElasticOperator> m = new HashMap<>();
            m.put(ElasticOperator.LESS_THAN, ElasticOperator.GREATER_THAN_OR_EQUAL);
            m.put(ElasticOperator.GREATER_THAN_OR_EQUAL, ElasticOperator.LESS_THAN);
            m.put(ElasticOperator.GREATER_THAN, ElasticOperator.LESS_THAN_OR_EQUAL);
            m.put(ElasticOperator.LESS_THAN_OR_EQUAL, ElasticOperator.GREATER_THAN);
            this.operatorOpposites = m;
        }
        return operatorOpposites;
    }
}