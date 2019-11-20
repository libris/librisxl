package whelk


import whelk.component.PostgreSQLComponent

class Relations {
    public static final List<String> BROADER_RELATIONS = ['broader', 'broadMatch', 'exactMatch']

    PostgreSQLComponent storage

    Relations(PostgreSQLComponent storage) {
        this.storage = storage
    }

    boolean isImpliedBy(String broaderIri, String narrowerIri) {
        Set<String> visited = []
        List<String> stack = [narrowerIri]

        while (!stack.isEmpty()) {
            String iri = stack.pop()
            for (String relation : BROADER_RELATIONS) {
                Set<String> dependencies = new HashSet<>(storage.getDependenciesOfTypeByIri(iri, relation))
                if (dependencies.contains(broaderIri)) {
                    return true
                }
                dependencies.removeAll(visited)
                visited.addAll(dependencies)
                stack.addAll(dependencies)
            }
        }

        return false
    }

    Set<String> findInverseBroaderRelations(String iri) {
        Set<String> iris = []
        List<String> stack = [iri]

        while (!stack.isEmpty()) {
            String id = stack.pop()
            for (String relation : BROADER_RELATIONS ) {
                Set<String> dependers = new HashSet<>(storage.getDependersOfTypeByIri(id, relation))
                dependers.removeAll(iris)
                stack.addAll(dependers)
                iris.addAll(dependers)
            }
        }

        return iris
    }
}
