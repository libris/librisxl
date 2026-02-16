package whelk

import whelk.component.PostgreSQLComponent

class Relations {
    public static final List<String> BROADER_RELATIONS = ['broader', 'broadMatch', 'exactMatch', 'locatedIn']

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

    Set<String> followBroader(String iri) {
        return follow(iri, BROADER_RELATIONS)
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
    
    Map<String, Long> getReverseCountByRelation(String iri) {
        storage.getIncomingLinkCountByRelation(iri)
    }

    private boolean isReachable(String fromIri, String toIri, List<String> relations) {
        Set<String> visited = []
        List<String> stack = [fromIri]

        while (!stack.isEmpty()) {
            String iri = stack.pop()
            for (String relation : relations) {
                Set<String> dependencies = new HashSet<>(storage.getByRelation(iri, relation))
                if (isSymmetric(relation)) {
                    dependencies.addAll(storage.getByReverseRelation(iri, relation))
                }
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
            
            Set<String> dependers = new HashSet<>()
            for (String relation : relations) {
                dependers.addAll(storage.getByReverseRelation(id, relation))
                if (isSymmetric(relation)) {
                    dependers.addAll(storage.getByRelation(id, relation))
                }
            }
            
            dependers.removeAll(iris)
            stack.addAll(dependers)
            iris.addAll(dependers)
        }

        return iris
    }

    private Set<String> follow(String iri, List<String> relations) {
        Set<String> iris = []
        List<String> stack = [iri]

        while (!stack.isEmpty()) {
            String id = stack.pop()

            Set<String> dependencies = new HashSet<>()
            for (String relation : relations) {
                dependencies.addAll(storage.getByRelation(id, relation))
                if (isSymmetric(relation)) {
                    dependencies.addAll(storage.getByReverseRelation(id, relation))
                }
            }

            dependencies.removeAll(iris)
            stack.addAll(dependencies)
            iris.addAll(dependencies)
        }

        return iris
    }
    
    private static boolean isSymmetric(String relation) {
        // FIXME get from vocab
        relation == "exactMatch"
    }
}
