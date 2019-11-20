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

    Set<String> findInverseBroaderRelations(String iri) {
        return getNestedDependers(iri, BROADER_RELATIONS)
    }

    private boolean isReachable(String fromIri, String toIri, List<String> relations) {
        Set<String> visited = []
        List<String> stack = [fromIri]

        while (!stack.isEmpty()) {
            String iri = stack.pop()
            for (String relation : relations) {
                Set<String> dependencies = new HashSet<>(storage.getDependenciesOfTypeByIri(iri, relation))
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

    private Set<String> getNestedDependers(String iri, List<String> relations) {
        Set<String> iris = []
        List<String> stack = [iri]

        while (!stack.isEmpty()) {
            String id = stack.pop()
            for (String relation : relations ) {
                Set<String> dependers = new HashSet<>(storage.getDependersOfTypeByIri(id, relation))
                dependers.removeAll(iris)
                stack.addAll(dependers)
                iris.addAll(dependers)
            }
        }

        return iris
    }
}
