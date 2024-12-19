package whelk.search2.querytree;

import java.util.Map;

public interface PropertyLike {
    String name();
    Map<String, Object> definition();
}
