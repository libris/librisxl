package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.EsMappings;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public sealed interface Selector permits Path, PathElement {
    String queryKey();
    String esField();

    List<? extends PathElement> path();

    List<Selector> getAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes);
    Selector withPrependedMetaProperty(JsonLd jsonLd);

    boolean isValid();
    boolean isType();

    boolean isObjectProperty();

    boolean mayAppearOnType(String type, JsonLd jsonLd);
    boolean appearsOnType(String type, JsonLd jsonLd);
    boolean indirectlyAppearsOnType(String type, JsonLd jsonLd);
    boolean appearsOnlyOnRecord(JsonLd jsonLd);

    Map<String, Object> definition();

    List<String> domain();
    List<String> range();

    default Optional<String> getEsNestedStem(EsMappings esMappings) {
        String esField = esField();
        if (esMappings.isNestedTypeField(esField)) {
            return Optional.of(esField);
        }
        return esMappings.getNestedTypeFields().stream().filter(esField::startsWith).findFirst();
    }
}
