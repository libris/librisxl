package datatool.scripts.mergeworks


import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd
import whelk.Whelk
import whelk.exception.WhelkRuntimeException
import whelk.util.LegacyIntegrationTools
import whelk.util.Statistics

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

import static datatool.scripts.mergeworks.FieldStatus.COMPATIBLE
import static datatool.scripts.mergeworks.FieldStatus.DIFF
import static datatool.scripts.mergeworks.FieldStatus.EQUAL
import static datatool.scripts.mergeworks.Util.asList
import static datatool.scripts.mergeworks.Util.getPathSafe
import static datatool.scripts.mergeworks.Util.partition

class WorkToolJob {
    private String CSS = WorkToolJob.class.getClassLoader()
            .getResourceAsStream('merge-works/table.css').getText("UTF-8")

    Whelk whelk
    Statistics statistics
    File clusters

    AtomicInteger clusterId = new AtomicInteger()

    String changedIn = "xl"
    String changedBy = "SEK"
    boolean dryRun = true
    boolean skipIndex = false
    boolean loud = false

    WorkToolJob(File clusters) {
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

    void show() {
        println("""<html><head>
                    <meta charset="UTF-8">
                    <style>$CSS</style>
                    </head><body>""")
        run({ cluster ->
            return {
                try {
                    Collection<Collection<Doc>> docs = titleClusters(cluster)
                            

                    if (docs.isEmpty() || docs.size() == 1 && docs.first().size() == 1) {
                        return
                    }

                    docs.each {it.each {it.addComparisonProps() } }
                    
                    println(docs
                            .collect { it.sort { a, b -> a.getWork()['@type'] <=> b.getWork()['@type'] } }
                            .collect { it.sort { it.numPages() } }
                            .collect { clusterTable(it) }
                            .join('') + "<hr/><br/>\n")
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
        println('</body></html>')
    }

    void show2() {
        println("""<html><head>
                    <meta charset="UTF-8">
                    <style>$CSS</style>
                    </head><body>""")
        run({ cluster ->
            return {
                try {
                    println(works(cluster).collect {[new Doc2(whelk, it.work)] + it.derivedFrom }
                            .collect { clusterTable(it) }
                            .join('') + "<hr/><br/>\n")
                }
                catch (Exception e) {
                    System.err.println(e.getMessage())
                    e.printStackTrace(System.err)
                }
            }
        })
        println('</body></html>')
    }

    void merge() {
        def s = statistics.printOnShutdown()
        run({ cluster ->
            return {
                works(cluster).each {
                    s.increment('num derivedFrom', "${it.derivedFrom.size()}", it.work.shortId)
                    store(it)
                }
            }
        })
    }

    class MergedWork {
        Document work
        Collection<Doc> derivedFrom
    }

    private Document buildWorkDocument(Map workData) {
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

    private void store(MergedWork work) {
        if (!dryRun) {
            if (!whelk.createDocument(work.work, changedIn, changedBy,
                    LegacyIntegrationTools.determineLegacyCollection(work.work, whelk.getJsonld()), false, !skipIndex)) {
                throw new WhelkRuntimeException("Could not store new work: ${work.work.shortId}")
            }

            String workIri = work.work.thingIdentifiers.first()

            work.derivedFrom
                    .collect { it.ogDoc }
                    .each {
                        def sum = it.checksum
                        it.data[JsonLd.GRAPH_KEY][1]['instanceOf'] = [(JsonLd.ID_KEY): workIri]
                        whelk.storeAtomicUpdate(it, !loud, changedIn, changedBy, sum, !skipIndex)
                    }
        }
    }

    private Collection<MergedWork> works(List<String> cluster) {
        def titleClusters = loadDocs(cluster)
                .findAll(filter)
                .each {it.addComparisonProps() }
                .with { partitionByTitle(it) }
                .findAll { it.size() > 1 }
                .findAll { !it.any{ doc -> doc.hasGenericTitle() } }

        def works = []
        titleClusters.each {titleCluster ->
            titleCluster.sort {it.numPages() }
            WorkComparator c = new WorkComparator(WorkComparator.allFields(titleCluster))

            works.addAll(partition(titleCluster, { Doc a, Doc b -> c.sameWork(a, b)})
                    .findAll {it.size() > 1 }
                    .each { work -> work.each {doc -> doc.removeComparisonProps()}}
                    .collect{new MergedWork(work: buildWorkDocument(c.merge(it)), derivedFrom: it)})
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

    void printInstanceValue(String field) {
        run({ cluster ->
            return {
                String values = cluster.collect(whelk.&getDocument).collect {
                    "${it.shortId}\t${getPathSafe(it.data, ['@graph', 1, field])}"
                }.join('\n')

                println(values + '\n')
            }
        })
    }

    void fictionNotFiction() {
        run({ cluster ->
            return {
                Collection<Collection<Doc>> titleClusters = titleClusters(cluster)

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
        run({ cluster ->
            return {
                if (textMonoDocs(cluster).any { it.isFiction() }) {
                    println(cluster.join('\t'))
                }
            }
        })
    }

    void outputTitleClusters() {
        run({ cluster ->
            return {
                titleClusters(cluster).findAll{ it.size() > 1 }.each {
                    println(it.collect{it.doc.shortId }.join('\t'))
                }
            }
        })
    }

    void linkContribution() {
        //statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                // TODO: check work language?
                def docs = cluster.collect(whelk.&getDocument)
                
                List<Map> linked = []
                docs.each { Document d ->
                    def contribution = getPathSafe(d.data, ['@graph', 1, 'instanceOf', 'contribution'], [])
                    contribution.each { Map c ->
                        if (c.agent && c.agent['@id']) {
                            // TODO: fix whelk, add load by IRI method
                            def id = c.agent['@id']
                            whelk.storage.loadDocumentByMainId(id)?.with { doc ->
                                Map agent = doc.data['@graph'][1]
                                agent.roles = asList(c.role)
                                linked << agent
                            }
                        }
                    }
                }

                docs.each { Document d ->
                    def contribution = getPathSafe(d.data, ['@graph', 1, 'instanceOf', 'contribution'], [])
                    contribution.each { Map c ->
                        if (c.agent && !c.agent['@id']) {
                            def l = linked.find {
                                (it.givenName == c.agent.givenName && it.firstName == c.agent.firstName) && (!c.role || it.roles.containsAll(c.role)) 
                            }
                            if (l) {
                                println("$c --> $l")
                            }
                        }
                    }
                }
            }
        })
    }
    
    static def infoFields = ['instance title', 'work title', 'instance type', 'editionStatement', 'responsibilityStatement', 'encodingLevel', 'publication', 'identifiedBy', 'extent']

    private String clusterTable(Collection<Doc> cluster) {
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

    private def fieldRows(Collection<Doc> cluster, String cls) {
        { field ->
            """
            <tr class="${cls}">
                <td>${field}</td>
                ${cluster.collect { "<td>${it.getDisplayText(field)}</td>" }.join('\n')}   
            </tr> """.stripIndent()
        }
    }

    private void run(Function<List<String>, Runnable> f) {
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
                .findAll { !it.any{ doc -> doc.hasGenericTitle() } }
                .sort { a, b -> a.first().instanceDisplayTitle() <=> b.first().instanceDisplayTitle() }
    }

    Collection<Collection<Doc>> partitionByTitle(Collection<Doc> docs) {
        return partition(docs) { Doc a, Doc b ->
            !a.getTitleVariants().intersect(b.getTitleVariants()).isEmpty()
        }
    }
}

class NoWorkException extends RuntimeException {
    NoWorkException(String msg) {
        super(msg)
    }
}









