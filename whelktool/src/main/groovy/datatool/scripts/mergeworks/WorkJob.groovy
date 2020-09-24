package datatool.scripts.mergeworks


import datatool.util.DocumentComparator
import org.apache.commons.lang3.StringUtils
import org.codehaus.jackson.map.ObjectMapper
import org.skyscreamer.jsonassert.JSONCompare
import org.skyscreamer.jsonassert.JSONCompareMode
import org.skyscreamer.jsonassert.JSONCompareResult
import se.kb.libris.Normalizers
import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd
import whelk.Whelk
import whelk.exception.WhelkRuntimeException
import whelk.util.DocumentUtil
import whelk.util.LegacyIntegrationTools
import whelk.util.Statistics
import whelk.util.Unicode

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

import static datatool.scripts.mergeworks.FieldStatus.COMPATIBLE
import static datatool.scripts.mergeworks.FieldStatus.DIFF
import static datatool.scripts.mergeworks.FieldStatus.EQUAL
import static datatool.scripts.mergeworks.Util.partition

class WorkJob {
    private static Set<String> IGNORED_SUBTITLES = WorkJob.class.getClassLoader()
            .getResourceAsStream('merge-works/ignored-subtitles.txt')
            .readLines().grep().collect(this.&normalize) as Set

    private String CSS = WorkJob.class.getClassLoader()
            .getResourceAsStream('merge-works/table.css').getText("UTF-8")

    Whelk whelk
    Statistics statistics
    File clusters

    AtomicInteger clusterId = new AtomicInteger()

    ObjectMapper mapper = new ObjectMapper()

    String changedIn = "xl"
    String changedBy = "SEK"
    boolean dryRun = true
    boolean skipIndex = false
    boolean loud = false

    WorkJob(File clusters) {
        this.clusters = clusters

        this.whelk = Whelk.createLoadedSearchWhelk('secret', true)
        this.statistics = new Statistics()
    }

    Closure filter = { Doc doc ->
        (doc.isText()
                && doc.isMonograph()
                && !doc.hasPart()
                && (doc.encodingLevel() == 'marc:MinimalLevel' || doc.encodingLevel() == 'marc:FullLevel'))
    }

    void show(List<List<String>> diffPaths = []) {
        println("""<html><head>
                    <meta charset="UTF-8">
                    <style>$CSS</style>
                    </head><body>""")
        run({ cluster ->
            return {
                try {
                    Collection<Collection<Doc>> docs = titleClusters(cluster)
                            .sort { a, b -> a.first().instanceDisplayTitle() <=> b.first().instanceDisplayTitle() }

                    if (docs.isEmpty() || docs.size() == 1 && docs.first().size() == 1) {
                        return
                    }

                    println(docs
                            .collect { it.sort { a, b -> a.getWork()['@type'] <=> b.getWork()['@type'] } }
                            .collect { clusterTable(it, diffPaths) }
                            .join('') + "<hr/><br/>\n")
                }
                catch (Exception e) {
                    System.err.println(e.getMessage())
                    e.printStackTrace(System.err)
                }
            }
        })
        println("""</body""")
        println('</html>')
    }

    Document makeWork(Map workData) {
        workData['@id'] = "TEMPID#it"
        Document d = new Document([
                "@graph": [
                        [
                                "@id"       : "TEMPID",
                                "@type"     : "Record",
                                "mainEntity": ["@id": "TEMPID#it"],
                        ],
                        workData
                ]
        ])

        d.setGenerationDate(new Date())
        d.setGenerationProcess('https://libris.kb.se/sys/merge-works')
        d.deepReplaceId(Document.BASE_URI.toString() + IdGenerator.generate())
        return d
    }

    void show2(List<List<String>> diffPaths = []) {
        println("""<html><head>
                    <meta charset="UTF-8">
                    <style>$CSS</style>
                    </head><body>""")
        run({ cluster ->
            return {
                try {
                    println(works(cluster).collect {[new Doc2(whelk, it.work)] + it.derivedFrom }
                            .collect { clusterTable(it, diffPaths) }
                            .join('') + "<hr/><br/>\n")
                }
                catch (Exception e) {
                    System.err.println(e.getMessage())
                    e.printStackTrace(System.err)
                }
            }
        })
        println("""</body""")
        println('</html>')
    }

    void merge() {
        def s = statistics.printOnShutdown()
        run({ cluster ->
            return {
                works(cluster).each {
                    s.increment('num derivedFrom', "${it.derivedFrom.size()}")
                    store(it)
                }
            }
        })
    }

    class MergedWork {
        Document work
        Collection<Doc> derivedFrom
    }

    void store(MergedWork work) {
        if (!dryRun) {
            if (!whelk.createDocument(work.work, changedIn, changedBy,
                    LegacyIntegrationTools.determineLegacyCollection(work.work, whelk.getJsonld()), false, !skipIndex)) {
                throw new WhelkRuntimeException("Could not store new work: ${work.work.shortId}")
            }

            String workIri = work.work.thingIdentifiers.first()

            work.derivedFrom
                    .collect { it.doc }
                    .each {
                        def sum = it.checksum
                        it.data[JsonLd.GRAPH_KEY][1]['instanceOf'] = [(JsonLd.ID_KEY): workIri]
                        whelk.storeAtomicUpdate(it, !loud, changedIn, changedBy, sum, !skipIndex)
                    }
        }
    }

    Collection<MergedWork> works(List<String> cluster) {
        def titleClusters = loadDocs(cluster)
                .findAll(filter)
                .with { partitionByTitle(it) }
                .findAll {it.size() > 1 }

        def works = []
        titleClusters.each {titleCluster ->
            WorkComparator c = new WorkComparator(WorkComparator.allFields(titleCluster))

            works.addAll(partition(titleCluster, { Doc a, Doc b -> c.sameWork(a, b)})
                    .findAll {it.size() > 1 }
                    .collect{new MergedWork(work: makeWork(c.merge(it)), derivedFrom: it)})
        }

        return works
    }

    void subTitles() {
        statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                String titles = cluster.collect(whelk.&getDocument).collect {
                    getPathSafe(it.data, ['@graph', 1, 'hasTitle', 0, 'subtitle'])
                }.grep().join('\n')

                if (!titles.isBlank()) {
                    println(titles + '\n')
                }
            }
        })
    }



    void splitClustersByWorks() {
        statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                statistics.increment('TOTAL', 'TOTAL CLUSTERS')
                List<List<Document>> result = mergeWorks(cluster)
                //statistics.increment(String.format("Cluster size %03d", cluster.size()) , String.format("Num works %03d", result.size()))
            }
        })
    }

    void edition() {
        statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                String editionStatment = cluster.collect(whelk.&getDocument).collect {
                    getPathSafe(it.data, ['@graph', 1, 'editionStatement'])
                }.grep().join('\n')

                if (!editionStatment.isBlank()) {
                    println(editionStatment + '\n')
                }
            }
        })
    }

    void fictionNotFiction() {
        //statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                Collection<Collection<Doc>> titleClusters = titleClusters(cluster)
                        .sort { a, b -> a.first().instanceDisplayTitle() <=> b.first().instanceDisplayTitle() }

                for (titleCluster in titleClusters) {
                    if (titleCluster.size() > 1) {
                        Set<String> diffFields = getDiffFields(titleCluster, ['contribution', 'genreForm'] as Set)
                        if (!diffFields.contains('contribution') && diffFields.contains('genreForm')) {
                            String gf = titleCluster.collect { it.getDisplayText('genreForm') }.join(' ')
                            if (gf.contains('marc/FictionNotFurtherSpecified') && gf.contains('marc/NotFictionNotFurtherSpecified')) {
                                println(titleCluster.collect { it.getDoc().shortId }.join('\t'))
                            }
                        }
                    }
                }
            }
        })
    }

    void fiction() {
        //statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                if (textMonoDocs(cluster).any { it.isFiction() }) {
                    println(cluster.join('\t'))
                }
            }
        })
    }

    static def infoFields = ['instance title', 'work title', 'instance type', 'editionStatement', 'responsibilityStatement', 'encodingLevel', 'publication', 'identifiedBy']

    String clusterTable(Collection<Doc> cluster, List<List<String>> diffPaths = []) {
        String id = "${clusterId.incrementAndGet()}"
        String header = """
            <tr>
                <th><a id="${id}"><a href="#${id}">${id}</th>
                ${cluster.collect { doc -> "<th><a href=\"${doc.link()}\">${doc.instanceDisplayTitle()}</a></th>" }.join('\n')}                                                             
            </tr>
           """.stripIndent()

        def statuses = WorkComparator.compare(cluster)

        String info = infoFields.collect(fieldRows(cluster, "info")).join('\n')
        String equal = statuses.get(EQUAL, []).collect(fieldRows(cluster, cluster.size() > 1 ? EQUAL.toString() : "")).join('\n')
        String compatible = statuses.get(COMPATIBLE, []).collect(fieldRows(cluster, COMPATIBLE.toString())).join('\n')
        String diff = statuses.get(DIFF, []).collect(fieldRows(cluster, DIFF.toString())).join('\n')

        return """
            <table>
                ${header}   
                ${equal}
                ${compatible}
                ${diff}
                ${info}
            </table>
            <br/><br/>
        """
    }

    def fieldRows(Collection<Doc> cluster, String cls) {
        { field ->
            """
            <tr class="${cls}">
                <td>${field}</td>
                ${cluster.collect { "<td>${it.getDisplayText(field)}</td>" }.join('\n')}   
            </tr> """.stripIndent()
        }
    }

    void run(Function<List<String>, Runnable> f) {
        ExecutorService s = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4)

        AtomicInteger i = new AtomicInteger()
        clusters.eachLine() {
            List<String> cluster = Arrays.asList(it.split(/[\t ]+/))

            s.submit({
                try {
                    f.apply(cluster).run()
                    int n = i.incrementAndGet()
                    if (n % 100 == 0) {
                        System.err.println("$n")
                    }
                }
                catch (NoWorkException e) {
                    //println("No work:" + e.getMessage())
                }
                catch (Exception e) {
                    e.printStackTrace()
                }
            })
        }

        s.shutdown()
        s.awaitTermination(1, TimeUnit.DAYS)
    }

    List<List<Document>> mergeWorks(Collection<String> cluster) {
        def t = titleClusters(cluster)
                .grep { it.size() > 1 }

        t.collect(this.&works)

        return t
    }

    private Collection<Doc> textMonoDocs(Collection<String> cluster) {
        whelk
                .bulkLoad(cluster).values()
                .collect { new Doc(whelk, it) }
                .findAll { it.isText() && it.isMonograph() }
    }

    private Collection<Doc> loadDocs(Collection<String> cluster) {
        whelk
                .bulkLoad(cluster).values()
                .collect { new Doc(whelk, it) }
    }

    private Collection<Collection<Doc>> titleClusters(Collection<String> cluster) {
        textMonoDocs(cluster)
                .with { partitionByTitle(it) }
    }

    void tryMerge(Collection<Doc> docs) {
        def d = new DocumentComparator()
        Collection<Collection<Doc>> works = partition(docs) { Doc a, Doc b ->
            return d.isEqual(a.getWork(), b.getWork())
        }

        if (works.size() > 1) {
            works.eachWithIndex { a, i ->
                works.eachWithIndex { b, j ->
                    if (j <= i) {
                        return
                    }

                    compare(a.first(), b.first())
                    compare(b.first(), a.first())
                }
            }
        }

        statistics.increment("TITLE CLUSTERS - SIZE", docs.size().toString())
        statistics.increment('TOTAL', 'TITLE CLUSTERS')


        works.grep { it.size() > 1 }.each { work ->
            statistics.increment("WORKS - NUM INSTANCES", work.size().toString())
            statistics.increment('TOTAL', 'TOTAL WORKS')
        }
    }

    void compare(Doc a, Doc b) {
        JSONCompareResult r = JSONCompare.compareJSON(
                mapper.writeValueAsString(workNoTitle(a.getWork())),
                mapper.writeValueAsString(workNoTitle(b.getWork())),
                JSONCompareMode.NON_EXTENSIBLE)

        if (r.failed()) {
            r.getMessage().replace('\n', '--').split(';').each { s ->
                statistics.increment("diff", StringUtils.normalizeSpace(s))
            }
            /*
            r.getFieldMissing().collect{it.getExpected()}.each { field ->
                statistics.increment("exists in one but not the other", field.toString())
            }
            r.getFieldUnexpected().collect{it.getActual()}.each { field ->
                statistics.increment("exists in one but not the other", field.toString())
            }
            differentSizeFields(r).each { field ->
                statistics.increment("contents diff", field)
            }
             */
        }
    }

    Map workNoTitle(Map work) {
        Map w = new HashMap<>(work)
        w.remove('hasTitle')
        return w
    }

    Collection<Collection<Doc>> partitionByTitle(Collection<Doc> docs) {
        return partition(docs) { Doc a, Doc b ->
            !a.getTitleVariants().intersect(b.getTitleVariants()).isEmpty()
        }
    }

    static Map getWork(Whelk whelk, Document d) {
        Map work = Normalizers.getWork(whelk.jsonld, d)
        if (!work) {
            throw new NoWorkException(d.shortId)
        }
        work = new HashMap<>(work)
        Map instance = d.data['@graph'][1]

        if (!work['hasTitle']) {
            work['hasTitle'] = flatTitles(instance)
        } else if (!work['hasTitle'].first().containsKey('flatTitle')) {
            work['hasTitle'] = flatTitles(work)
        }

        //TODO works with already title
        //TODO 'marc:fieldref'

        work.remove('@id')
        return work
    }

    static def titleComponents = ['mainTitle', 'titleRemainder', 'subtitle', 'hasPart', 'partNumber', 'partName', 'marc:parallelTitle', 'marc:equalTitle']

    static private List flatTitles(thing) {
        thing['hasTitle'].collect {
            def old = new TreeMap(it)

            if (it['subtitle']) {
                DocumentUtil.traverse(it['subtitle']) { value, path ->
                    if (path && value instanceof String && nonsenseSubtitle(value)) {
                        new DocumentUtil.Remove()
                    }
                }
            }

            def title = new TreeMap<>()
            title['flatTitle'] = normalize(Doc.flatten(old, titleComponents))
            if (it['@type']) {
                title['@type'] = it['@type']
            }

            title
        }
    }

    static def noise =
            [",", '"', "'", '[', ']', ',', '.', '.', ':', ';', '-', '(', ')', ' the ', '-', '–', '+', '!', '?'].collectEntries { [it, ' '] }

    private static String normalize(String s) {
        return Unicode.asciiFold(Unicode.normalizeForSearch(StringUtils.normalizeSpace(" $s ".toLowerCase().replace(noise))))
    }

    private static Object getPathSafe(item, path, defaultTo = null) {
        for (p in path) {
            if (item[p] != null) {
                item = item[p]
            } else {
                return defaultTo
            }
        }
        return item
    }
    
    private boolean nonsenseSubtitle(String s) {
        s = normalize(s)
        if (s.startsWith("en ")) {
            s = s.substring("en ".length())
        }
        return s in IGNORED_SUBTITLES
    }
}

class NoWorkException extends RuntimeException {
    NoWorkException(String msg) {
        super(msg)
    }
}









