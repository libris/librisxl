package whelk.converter;

import java.util.Map;

import trld.jsonld.Compaction;
import trld.jsonld.Expansion;
import trld.jsonld.Flattening;

public class JsonLdShapes {
    public static Object reCompact(Object data, Map context) {
      return reCompact(data, context, null);
    }

    public static Object reCompact(Object data, Map context, String baseIri) {
      return reCompact(data, context, baseIri, null);
    }

    public static Object reCompact(Object data, Map context, String baseIri, String compactBaseIri) {
        var expanded = expand(data, baseIri);
        return Compaction.compact(context, expanded, compactBaseIri);
    }

    public static Object reCompactWithEmbeddedBlanks(Object data, Map context) {
      return reCompactWithEmbeddedBlanks(data, context, null);
    }

    public static Object reCompactWithEmbeddedBlanks(Object data, Map context, String baseIri) {
      return reCompactWithEmbeddedBlanks(data, context, baseIri, null);
    }

    public static Object reCompactWithEmbeddedBlanks(Object data, Map context, String baseIri, String compactBaseIri) {
        var expanded = expand(data, baseIri);
        var flattened = flatten(expanded);
        var compacted = Compaction.compact(context, flattened, compactBaseIri);
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
