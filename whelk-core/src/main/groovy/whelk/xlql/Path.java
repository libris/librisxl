package whelk.xlql;

import whelk.JsonLd;

import java.util.*;
import java.util.stream.Collectors;

public class Path {
    public String property;
    public List<String> path;

    Path(String property) {
        this.property = property;
        this.path = new ArrayList<>(Arrays.asList(property));
    }

    Path(String property, List<String> path) {
        this.property = property;
        this.path = new ArrayList<>(path);
    }

    Path(Path field) {
        this(field.property, field.path);
    }

    Path copy() {
        return new Path(this);
    }

    public void prependMeta() {
        path.add(0, JsonLd.getRECORD_KEY());
    }

    public void appendId() {
        path.add(JsonLd.getID_KEY());
    }

    public void appendUnderscoreStr() {
        path.add(JsonLd.getSEARCH_KEY());
    }

    public void setWorkToInstancePath() {
        path.add(0, JsonLd.getWORK_KEY());
        path.add(0, JsonLd.getREVERSE_KEY());
    }

    public void setInstanceToWorkPath() {
        path.add(0, JsonLd.getWORK_KEY());
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

    public String stringify() {
        return path.stream().collect(Collectors.joining("."));
    }
}