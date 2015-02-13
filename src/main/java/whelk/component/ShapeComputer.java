package whelk.component;

import whelk.plugin.Plugin;

public interface ShapeComputer extends Plugin {
    public String calculateTypeFromIdentifier(String identifier);
    public String toElasticId(String whelkIdentifier);
    public String fromElasticId(String elasticId);
}
