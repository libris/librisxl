package datatool.scripts.mergeworks


import whelk.IdGenerator
import whelk.Whelk
import whelk.exception.WhelkRuntimeException
import whelk.meta.WhelkConstants
import whelk.util.LegacyIntegrationTools
import whelk.util.Statistics

import java.text.SimpleDateFormat
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

import static datatool.scripts.mergeworks.Util.buildWorkDocument
import static datatool.scripts.mergeworks.Util.getPathSafe
import static datatool.scripts.mergeworks.Util.partition

class WorkToolJob {
    Whelk whelk
    Statistics statistics
    File clusters

    String date = new SimpleDateFormat('yyyyMMdd-HHmmss').format(new Date())
    String jobId = IdGenerator.generate()
    File reportDir = new File("reports/merged-works/$date")

    String changedIn = "xl"
    String changedBy = "SEK"
    String generationProcess = 'https://libris.kb.se/sys/merge-works'

    boolean dryRun = true
    boolean skipIndex = false
    boolean loud = false
    boolean verbose = false
    int numThreads = -1

    private enum WorkStatus {
        NEW('new'),
        UPDATED('updated')

        String status

        private WorkStatus(String status) {
            this.status = status
        }
    }

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
                    def merged = uniqueWorks(loadLastUnlinkedVersion(cluster)).findAll { !it.existsInStorage }
                    if (merged) {
                        println(merged.collect { [it] + it.unlinkedInstances }
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
                    def hub = uniqueWorks(loadLastUnlinkedVersion(cluster))
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
                def works = uniqueWorks(docs)
                def createdOrUpdated = works.findAll { it.unlinkedInstances }

                WorkStatus.values().each {
                    new File(reportDir, it.status).tap { it.mkdir() }
                }
                writeSingleWorkReport(docs, createdOrUpdated, s)

                if (works.size() > 1) {
                    multiWorkClusters.add(works)
                }

                if (!dryRun) {
                    def linkableWorkIris = works.findResults { it.workIri() }
                    works.each { doc ->
                        doc.addCloseMatch(linkableWorkIris)
                        store(doc)
                        doc.unlinkedInstances?.each {
                            it.replaceWorkData(['@id': doc.thingIri()])
                            store(it)
                        }
                    }
                }
            }
        })

        writeMultiWorkReport(multiWorkClusters)
    }

    void store(Doc doc) {
        whelk.setSkipIndex(skipIndex)
        doc.document.setGenerationDate(new Date())
        doc.document.setGenerationProcess(generationProcess)

        if (!doc.existsInStorage) {
            if (!whelk.createDocument(doc.document, changedIn, changedBy,
                    LegacyIntegrationTools.determineLegacyCollection(doc.document, whelk.getJsonld()), false)) {
                throw new WhelkRuntimeException("Could not store new work: ${doc.shortId()}")
            }
        } else if (doc.modified) {
            whelk.storeAtomicUpdate(doc.document, !loud, false, changedIn, generationProcess, doc.preUpdateChecksum)
        }
    }

    void writeSingleWorkReport(Collection<Doc> titleClusters, Collection<Doc> derivedWorks, Statistics s) {
        String report = htmlReport(titleClusters, derivedWorks)
        derivedWorks.each {
            def status = it.existsInStorage ? WorkStatus.UPDATED.status : WorkStatus.NEW.status
            new File(reportDir, "$status/${it.shortId()}.html") << report
            s.increment("num derivedFrom ($status works)", "${it.unlinkedInstances.size()}", it.shortId())
        }
    }

    void writeMultiWorkReport(Collection<Collection<Doc>> workClusters) {
        new File(reportDir, "multi-work-clusters.html").with { f ->
            f.append(Html.START)
            workClusters.each {
                f.append(Html.hubTable(it) + Html.HORIZONTAL_RULE)
            }
            f.append(Html.END)
        }
    }

    String htmlReport(Collection<Doc> titleCluster, Collection<Doc> works) {
        StringBuilder s = new StringBuilder()

        s.append(Html.START)

        s.append("<h1>Title cluster</h1>")
        titleCluster
                .each { it.addComparisonProps() }
                .sort { a, b -> a.workType() <=> b.workType() }
                .sort { it.numPages() }
        s.append(Html.clusterTable(titleCluster))
        s.append(Html.HORIZONTAL_RULE)

        titleCluster.each {
            it.removeComparisonProps()
        }

        s.append("<h1>Extracted works</h1>")
        works.collect { [it] + it.unlinkedInstances }
                .each { s.append(Html.clusterTable(it)) }

        s.append(Html.END)

        return s.toString()
    }

    private Collection<Doc> uniqueWorks(Collection<Doc> titleCluster) {
        def works = []

        prepareForCompare(titleCluster)

        WorkComparator c = new WorkComparator(WorkComparator.allFields(titleCluster))

        def workClusters = partition(titleCluster, { Doc a, Doc b -> c.sameWork(a, b) })
                .each { work -> work.each { doc -> doc.removeComparisonProps() } }

        workClusters.each { Collection<Doc> wc ->
            def (local, linked) = wc.split { it.instanceData }
            if (!linked) {
                if (local.size() == 1) {
                    works.add(local.first())
                } else {
                    def newWork = new Doc(whelk, buildWorkDocument(c.merge(local), reportDir)).tap {
                        it.existsInStorage = false
                        it.unlinkedInstances = local
                    }
                    works.add(newWork)
                }
            } else if (linked.size() == 1) {
                def existingWork = linked.first().tap { Doc d ->
                    if (local) {
                        d.replaceWorkData(c.merge(linked + local))
                        d.unlinkedInstances = local
                    }
                }
                works.add(existingWork)
            } else {
                System.err.println("Local works ${local.collect { it.shortId() }} match multiple linked works: ${linked.collect { it.shortId() }}. Duplicate linked works?")
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

                if (c.size() > 1 && c.any { Doc d -> d.isFiction() } && !c.any { Doc d -> d.isNotFiction() }) {
                    println(c.collect { Doc d -> d.shortId() }.join('\t'))
                }
            }
        })
    }

    void filterDocs(Closure<Doc> predicate) {
        run({ cluster ->
            return {
                def c = loadDocs(cluster).findAll(predicate)
                if (c.size() > 0) {
                    println(c.collect { it.shortId() }.join('\t'))
                }
            }
        })
    }

    void outputTitleClusters() {
        run({ cluster ->
            return {
                titleClusters(loadDocs(cluster)).findAll { it.size() > 1 }.each {
                    println(it.collect { it.shortId() }.join('\t'))
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
