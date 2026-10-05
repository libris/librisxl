package whelk.converter;

import java.util.Map;

import trld.jsonld.Compaction;
import trld.jsonld.Expansion;
import trld.jsonld.Flattening;

class JsonLdShapes {
    public static Object reCompact(Object data, Map context) {
      return reCompact(data, context, null);
    }

    public static Object reCompact(Object data, Map context, String baseIri) {
        var expanded = expand(data, baseIri);
        return Compaction.compact(context, expanded);
    }

    public static Object reCompactWithEmbeddedBlanks(Object data, Map context) {
      return reCompactWithEmbeddedBlanks(data, context, null);
    }

    public static Object reCompactWithEmbeddedBlanks(Object data, Map context, String baseIri) {
        var expanded = expand(data, baseIri);
        var flattened = flatten(expanded);
        var compacted = Compaction.compact(context, flattened);
        EmbedBlanks.embedBlanks(compacted);
        return compacted;
    }

    protected static Object expand(Object data) {
      return expand(data, null);
    }

    protected static Object expand(Object data, String baseIri) {
        return Expansion.expand(data, baseIri);
    }

    protected static Object flatten(Object data) {
        return Flattening.flatten(data);
    }
}
