package whelk.converter;

import java.util.Map;

import trld.jsonld.Compaction;
import trld.jsonld.Expansion;
import trld.jsonld.Flattening;

class JsonLdShapes {
    static Object expand(Object data) {
      return expand(data, null);
    }

    public static Object expand(Object data, String baseIri) {
        return Expansion.expand(data, baseIri);
    }

    public static Object flatten(Object data) {
        return Flattening.flatten(data);
    }

    public static Object compact(Object data, Map context) {
      return compact(data, context, null);
    }

    public static Object compact(Object data, Map context, String baseIri) {
        var expanded = expand(data, baseIri);
        var flattened = flatten(expanded);
        return Compaction.compact(context, flattened);
    }

    public static Object compactWithEmbeddedBlanks(Object data, Map context) {
      return compactWithEmbeddedBlanks(data, context, null);
    }

    public static Object compactWithEmbeddedBlanks(Object data, Map context, String baseIri) {
        var compacted = compact(data, context, baseIri);
        EmbedBlanks.embedBlanks(compacted);
        return compacted;
    }
}
