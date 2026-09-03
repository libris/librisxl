package whelk.search2.querytree.selector;

import whelk.JsonLd;
import whelk.search2.QueryUtil;

import java.util.List;
import java.util.Map;

public sealed interface Selector permits Path, PathElement {
    String queryKey();
    String esField();

    List<? extends PathElement> path();

    boolean isValid();
    boolean isType();
    boolean isComposite();

    boolean isObjectProperty();
    boolean isLdSetContainer();

    boolean mayAppearOnType(String type, JsonLd jsonLd);
    boolean appearsOnType(String type, JsonLd jsonLd);
    boolean indirectlyAppearsOnType(String type, JsonLd jsonLd);

    Map<String, Object> definition();

    List<String> domain();
    List<String> range();

    default String formattedQueryKey() {
        var k = queryKey();
        return k.contains(":") && !QueryUtil.isQuoted(k)
                ? QueryUtil.quote(k)
                : k;
    }
}
