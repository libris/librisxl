package whelk

import whelk.component.PostgreSQLComponent

class Relations {
    public static final List<String> BROADER_RELATIONS = ['broader', 'broadMatch', 'exactMatch']

    PostgreSQLComponent storage

    Relations(PostgreSQLComponent storage) {
        this.storage = storage
    }

    boolean isImpliedBy(String broaderIri, String narrowerIri) {
        return isReachable(narrowerIri, broaderIri, BROADER_RELATIONS)
    }

    Set<String> followReverseBroader(String iri) {
        return followReverse(iri, BROADER_RELATIONS)
    }

    Set<String> getBy(String iri, List<String> relations) {
        Set<String> result = new HashSet<>()
        relations.each { result.addAll(storage.getByRelation(iri, it)) }
        return result
    }

    Set<String> getByReverse(String iri, List<String> relations) {
        Set<String> result = new HashSet<>()
        relations.each { result.addAll(storage.getByReverseRelation(iri, it)) }
        return result
    }

    private boolean isReachable(String fromIri, String toIri, List<String> relations) {
        Set<String> visited = []
        List<String> stack = [fromIri]

        while (!stack.isEmpty()) {
            String iri = stack.pop()
            for (String relation : relations) {
                Set<String> dependencies = new HashSet<>(storage.getByRelation(iri, relation))
                if (dependencies.contains(toIri)) {
                    return true
                }
                dependencies.removeAll(visited)
                visited.addAll(dependencies)
                stack.addAll(dependencies)
            }
        }

        return false
    }

    private Set<String> followReverse(String iri, List<String> relations) {
        Set<String> iris = []
        List<String> stack = [iri]

        while (!stack.isEmpty()) {
            String id = stack.pop()
            for (String relation : relations ) {
                Set<String> dependers = new HashSet<>(storage.getByReverseRelation(id, relation))
                dependers.removeAll(iris)
                stack.addAll(dependers)
                iris.addAll(dependers)
            }
        }

        return iris
    }
}
