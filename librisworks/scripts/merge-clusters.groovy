import datatool.util.DisjointSets

mergeClusters(new File(System.getProperty('clusters')))

void mergeClusters(File clusters) throws FileNotFoundException {
    DisjointSets<String> sets = new DisjointSets<>()

    clusters.eachLine {
        sets.addSet(Arrays.asList(it.split(/[\t ]+/)))
    }

    sets.iterateAllSets(new DisjointSets.SetVisitor<String>() {
        boolean first = true
        @Override
        void nextElement(String e) {
            if(!first)
                print('\t')
            print(e)
            first = false
        }

        @Override
        void closeSet() {
            println()
            first = true
        }
    })
}