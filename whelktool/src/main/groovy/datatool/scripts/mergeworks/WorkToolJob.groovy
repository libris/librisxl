package datatool.scripts.mergeworks


import whelk.IdGenerator
import whelk.Whelk
import whelk.meta.WhelkConstants
import whelk.util.Statistics

import java.text.SimpleDateFormat
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

import static datatool.scripts.mergeworks.Util.getPathSafe
import static datatool.scripts.mergeworks.Util.partition

class WorkToolJob {
    Whelk whelk
    Statistics statistics
    File clusters

    String date = new SimpleDateFormat('yyyyMMdd-HHmmss').format(new Date())
    String jobId = IdGenerator.generate()
    File reportDir = new File("reports/merged-works/$date")

    boolean dryRun = true
    boolean skipIndex = false
    boolean loud = false
    boolean verbose = false
    int numThreads = -1

    WorkToolJob(File clusters) {
        this.clusters = clusters

        this.whelk = Whelk.createLoadedSearchWhelk('secret', true)
        this.statistics = new Statistics()
    }

    public static Closure qualityMonographs = { Doc doc ->
        (doc.isText()
                && doc.isMonograph()
                && !doc.isMaybeAggregate()
                && (doc.encodingLevel() != 'marc:PartialPreliminaryLevel' && doc.encodingLevel() != 'marc:PrepublicationLevel'))
                && !doc.isTactile()
                && !doc.isDrama()
                && !doc.isThesis()
                && !doc.isInSb17Bibliography()
    }

    void show() {
        println(Html.START)
        run({ cluster ->
            return {
                try {
                    if (cluster.size() > 1) {
                        Collection<Doc> docs = loadLastUnlinkedVersion(cluster).each { it.addComparisonProps() }
                                .sort { a, b -> a.workType() <=> b.workType() }
                                .sort { it.numPages() }

                        println(Html.clusterTable(docs) + Html.HORIZONTAL_RULE)
                    }
                }
                catch (NoWorkException e) {
                    System.err.println(e.getMessage())
                }
                catch (Exception e) {
                    System.err.println(e.getMessage())
                    e.printStackTrace(System.err)
                }
            }
        })
        println(Html.END)
    }

    void showWorks() {
        println(Html.START)
        run({ cluster ->
            return {
                try {
                    def merged = mergedWorks(loadLastUnlinkedVersion(cluster)).findAll { it instanceof NewWork }
                    if (merged) {
                        println(merged.collect { [it.doc] + it.derivedFrom }
                                .collect { Html.clusterTable(it) }
                                .join('') + Html.HORIZONTAL_RULE
                        )
                    }
                }
                catch (Exception e) {
                    System.err.println(e.getMessage())
                    e.printStackTrace(System.err)
                }
            }
        })
        println(Html.END)
    }

    void showHubs() {
        println(Html.START)
        run({ cluster ->
            return {
                try {
                    def hub = mergedWorks(loadLastUnlinkedVersion(cluster))
                    if (hub.size() > 1) {
                        println(Html.hubTable(hub) + Html.HORIZONTAL_RULE)
                    }
                }
                catch (Exception e) {
                    System.err.println(e.getMessage())
                    e.printStackTrace(System.err)
                }
            }
        })
        println(Html.END)
    }

    void merge() {
        def s = statistics.printOnShutdown()
        def multiWorkClusters = Collections.synchronizedList([])

        run({ cluster ->
            return {
                def docs = loadDocs(cluster)
                def works = mergedWorks(docs)

                if (works.size() > 1) {
                    multiWorkClusters.add(works)
                }

                def linkableWorks = works.findAll { it instanceof NewWork || it instanceof LinkedWork }

                if (linkableWorks && !dryRun) {
                    whelk.setSkipIndex(skipIndex)
                    works.each { work ->
                        work.addCloseMatch(linkableWorks.collect { it.document.getThingIdentifiers().first() })
                        work.setGenerationFields()
                        work.store(whelk)
                        work.linkInstances(whelk)
                    }
                }

                String report = htmlReport(docs, linkableWorks)

//                new File(reportDir, "${Html.clusterId(cluster)}.html") << report
                linkableWorks.each {
                    if (it instanceof NewWork) {
                        s.increment('num derivedFrom (new works)', "${it.derivedFrom.size()}", it.document.shortId)
                    } else if (it instanceof LinkedWork) {
                        s.increment('num derivedFrom (updated works)', "${it.derivedFrom.size()}", it.document.shortId)
                    }
                    it.reportDir.mkdirs()
                    new File(it.reportDir, "${it.document.shortId}.html") << report
                }
            }
        })

        new File(reportDir, "multi-work-clusters.html").with { f ->
            f.append(Html.START)
            multiWorkClusters.each {
                f.append(Html.hubTable(it) + Html.HORIZONTAL_RULE)
            }
            f.append(Html.END)
        }
    }

    String htmlReport(Collection<Doc> titleCluster, Collection<Work> works) {
        StringBuilder s = new StringBuilder()

        s.append(Html.START)

        s.append("<h1>Title cluster</h1>")
        titleCluster
                .each { it.addComparisonProps() }
                .sort { a, b -> a.workType() <=> b.workType() }
                .sort { it.numPages() }
        s.append(Html.clusterTable(titleCluster))
        s.append(Html.HORIZONTAL_RULE)

        s.append("<h1>Extracted works</h1>")
        works.collect { [it.doc] + it.derivedFrom }
                .each { s.append(Html.clusterTable(it)) }

        s.append(Html.END)

        return s.toString()
    }

    private Collection<Work> mergedWorks(Collection<Doc> titleCluster) {
        def works = []

        prepareForCompare(titleCluster)

        WorkComparator c = new WorkComparator(WorkComparator.allFields(titleCluster))

        def workClusters = partition(titleCluster, { Doc a, Doc b -> c.sameWork(a, b) })
                .each { work -> work.each { doc -> doc.removeComparisonProps() } }

        workClusters.each { wc ->
            def (local, linked) = wc.split { it.instanceData }
            if (!linked) {
                if (local.size() == 1) {
                    Doc doc = local.first()
                    works.add(new LocalWork(doc, reportDir))
                } else {
                    works.add(new NewWork(local, reportDir)
                            .tap { it.createDoc(whelk, c.merge(local)) })
                }
            } else if (linked.size() == 1) {
                Doc doc = linked.first()
                works.add(new LinkedWork(doc, local, reportDir)
                        .tap { it.update(c.merge(linked + local)) })
            } else {
                System.err.println("Local works in ${local.collect { it.shortId }} matches multiple linked works: ${linked.collect { it.shortId }}. Duplicate linked works?")
            }
        }

        return works
    }

    void swedishFiction() {
        def swedish = { Doc doc ->
            Util.asList(doc.workData['language']).collect { it['@id'] } == ['https://id.kb.se/language/swe']
        }

        run({ cluster ->
            return {
                def c = loadDocs(cluster).split { it.instanceData }
                        .with { local, linked ->
                            linked + local.findAll(qualityMonographs).findAll(swedish)
                        }

                if (c.any { Doc d -> d.isFiction() } && !c.any { Doc d -> d.isNotFiction() }) {
                    println(c.collect { Doc d -> d.document.shortId }.join('\t'))
                }
            }
        })
    }

    void filterDocs(Closure<Doc> predicate) {
        run({ cluster ->
            return {
                def c = loadDocs(cluster).findAll(predicate)
                if (c.size() > 0) {
                    println(c.collect { it.document.shortId }.join('\t'))
                }
            }
        })
    }

    void outputTitleClusters() {
        run({ cluster ->
            return {
                titleClusters(loadDocs(cluster)).findAll { it.size() > 1 }.each {
                    println(it.collect { it.document.shortId }.join('\t'))
                }
            }
        })
    }

    private void run(Function<List<String>, Runnable> f) {
        ExecutorService s = createExecutorService()

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

    private def createExecutorService() {
        int poolSize = numThreads > 1 ? numThreads : defaultNumThreads()
        def linkedBlockingDeque = new LinkedBlockingDeque<Runnable>((int) (poolSize * 1.5))

        def executorService = new ThreadPoolExecutor(poolSize, poolSize,
                1, TimeUnit.DAYS,
                linkedBlockingDeque, new ThreadPoolExecutor.CallerRunsPolicy())

        executorService.setThreadFactory(new ThreadFactory() {
            ThreadGroup group = new ThreadGroup(WhelkConstants.BATCH_THREAD_GROUP)

            @Override
            Thread newThread(Runnable runnable) {
                return new Thread(group, runnable)
            }
        })

        return executorService
    }

    private static int defaultNumThreads() {
        Runtime.getRuntime().availableProcessors() * 4
    }

    private Collection<Doc> loadDocs(Collection<String> cluster) {
        whelk
                .bulkLoad(cluster).values()
                .collect { new Doc(whelk, it) }
    }

    private Collection<Doc> loadLastUnlinkedVersion(Collection<String> cluster) {
        cluster.findResults {
            whelk.storage.
                    loadAllVersions(it)
                    .reverse()
                    .find { getPathSafe(it.data, it.workIdPath) == null }
                    ?.with { new Doc(whelk, it) }
        }
    }

    def loadUniqueLinkedWorks = { Collection<Doc> docs ->
        docs.findResults { it.workIri() }
                .unique()
                .collect { new Doc(whelk, whelk.storage.getDocumentByIri(it)) }
                .plus(docs.findAll { !it.workIri() })
    }

    private Collection<Collection<Doc>> titleClusters(Collection<Doc> docs) {
        partitionByTitle(docs)
                .findAll { !it.any { doc -> doc.hasGenericTitle() } }
                .collect(loadUniqueLinkedWorks)
                .findAll { it.size() > 1 }
                .sort { a, b -> a.first().view.instanceDisplayTitle() <=> b.first().view.instanceDisplayTitle() }
    }

    Collection<Collection<Doc>> partitionByTitle(Collection<Doc> docs) {
        return partition(docs) { Doc a, Doc b ->
            !a.flatInstanceTitle().intersect(b.flatInstanceTitle()).isEmpty()
        }
    }

    private Collection<Doc> prepareForCompare(Collection<Doc> docs) {
        docs.each {
            if (it.instanceData) {
                it.addComparisonProps()
            }
        }.sort { it.numPages() }
    }
}

class NoWorkException extends RuntimeException {
    NoWorkException(String msg) {
        super(msg)
    }
}
