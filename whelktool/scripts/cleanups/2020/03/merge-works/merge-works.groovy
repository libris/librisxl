import datatool.util.DocumentComparator
import datatool.util.Statistics
import java.util.concurrent.ConcurrentLinkedQueue
import whelk.Document

String dir = System.getProperty('clustersDir')
mergeWorks(new File(dir, 'clusters-merged.tsv'))



void mergeWorks(File input) throws FileNotFoundException {
    Statistics stats = new Statistics()

    //MergeWorks m = new MergeWorks()
    stats.printOnShutdown()
    def results = []

    input.eachLine() {
        List<String> cluster = Arrays.asList(it.split(/[\t ]+/))
        List<List<Document>> result = mergeWorks(cluster)
        stats.increment(String.format("Cluster size %03d", cluster.size()) , String.format("Num works %03d", result.size()))

        results.addAll(result)
    }

}

List<List<Document>> mergeWorks(Collection<String> ids) {
    Queue<Document> docs = new ConcurrentLinkedQueue<>()
    selectByIds(ids, { bib ->
        docs.add(bib.doc)
    }, 100, true)

    List<List<Document>> works = []
    for (Document doc : docs) {
        boolean match = false
        for (List<Document> workGroup : works) {
            if (sameWork(workGroup.first(), doc)) {
                workGroup.add(doc)
                match = true
                break;
            }
        }
        if (!match) {
            works.add([doc])
        }
    }

    return works
}

boolean sameWork(Document a, Document b) {
    return new DocumentComparator().isEqual(getWork(a), getWork(b))
}

Map getWork(Document d) {
    Map instance = d.data['@graph'][1]
    Map work = d.data['@graph'][2]

    if(!work['hasTitle']) {
        work['hasTitle'] = instance['hasTitle']
    }

    work.remove('@id')
    return work
}