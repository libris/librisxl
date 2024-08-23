package whelk.search2;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public record Entity(String id, String rdfType, Collection<String> superclasses) {
    public List<String> superclassesIncludingSelf() {
        return Stream.concat(superclasses.stream(), Stream.of(rdfType)).toList();
    }
}
