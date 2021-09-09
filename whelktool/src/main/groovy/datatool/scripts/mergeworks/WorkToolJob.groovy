package datatool.scripts.mergeworks


import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd
import whelk.Whelk
import whelk.exception.WhelkRuntimeException
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
    
    Closure qualityMonographs = { Doc doc ->
        (doc.isText()
                && doc.isMonograph()
                && !doc.hasPart()
                && (doc.encodingLevel() != 'marc:PartialPreliminaryLevel' && doc.encodingLevel() != 'marc:PrepublicationLevel'))
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

    private static Document buildWorkDocument(Map workData) {
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
                .findAll(qualityMonographs)
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
                        def statuses = WorkComparator.compare(cluster)
                        if (!statuses[DIFF].contains('contribution')) {
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

    void swedishFiction() {
        def swedish = { Doc doc ->
            Util.asList(doc.getWork()['language']).collect { it['@id'] } == ['https://id.kb.se/language/swe']
        }
        
        run({ cluster ->
            return {
                def c = loadDocs(cluster)
                        .findAll(qualityMonographs)
                        .findAll(swedish)        
                
                if (c.any { it.isFiction() } && !c.any{ it.isNotFiction()}) {
                    println(c.collect { it.doc.shortId }.join('\t'))
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
        statistics.printOnShutdown()
        run({ cluster ->
            return {
                statistics.increment('link contribution', 'clusters checked')
                // TODO: check work language?
                def docs = cluster
                        .collect(whelk.&getDocument)
                        .collect {[doc: it, checksum: it.getChecksum(whelk.jsonld), changed:false]}
                
                List<Map> linked = []
                docs.each { d ->
                    def contribution = getPathSafe(d.doc.data, ['@graph', 1, 'instanceOf', 'contribution'], [])
                    contribution.each { Map c ->
                        if (c.agent && c.agent['@id']) {
                            // TODO: fix whelk, add load by IRI method
                            String id = c.agent['@id']
                            whelk.storage.loadDocumentByMainId(id)?.with { agentDoc ->
                                Map agent = agentDoc.data['@graph'][1]
                                agent.roles = asList(c.role)
                                linked << agent
                            }
                        }
                    }
                    statistics.increment('link contribution', 'docs checked')
                }

                docs.each { 
                    Document d = it.doc
                    def contribution = getPathSafe(d.data, ['@graph', 1, 'instanceOf', 'contribution'], [])
                    contribution.each { Map c ->
                        if (c.agent && !c.agent['@id']) {
                            def l = linked.find {
                                agentMatches(c.agent, it) && (!c.role || it.roles.containsAll(c.role)) 
                            }
                            if (l) {
                                println("${d.shortId} ${Util.chipString(c, whelk)} --> ${Util.chipString(l, whelk)}")
                                c.agent = ['@id': l['@id']]
                                it.changed = true
                                statistics.increment('link contribution', 'agents linked')
                            }
                        }
                    }
                }

                def primaryAutIds = []
                docs.each {
                    def contribution = getPathSafe(it.doc.data, ['@graph', 1, 'instanceOf', 'contribution'], [])
                    def p = contribution.findAll()
                    contribution.each { 
                        if (it['@type'] == 'PrimaryContribution' && it['role'] == ['@id': 'https://id.kb.se/relator/author'] && it['agent'] && it['agent']['@id']) {
                            primaryAutIds << it['agent']['@id']
                        }
                    }
                }
                                
                docs.each {
                    Document d = it.doc
                    def contribution = getPathSafe(d.data, ['@graph', 1, 'instanceOf', 'contribution'], [])
                    contribution.each { Map c ->
                        if (c['@type'] == 'PrimaryContribution' && !c.role) {
                            println("PPP $c")
                            if (it.agent && it.agent['@id'] in primaryAutIds) {
                                c.role = ['@id': 'https://id.kb.se/relator/author']
                                it.changed = true
                                statistics.increment('link contribution', 'author role added to primary contribution')
                            }
                        }
                    }
                }
                
                docs.each {
                    if (!dryRun && it.changed) {
                        whelk.storeAtomicUpdate(it.doc, !loud, changedIn, changedBy, it.checksum, !skipIndex)    
                    }
                }
            }
        })
    }

    static boolean agentMatches(Map local, Map linked) {
        nameMatch(local, linked) && !yearMismatch(local, linked)
    }

    static boolean nameMatch(Map local, Map linked) {
        def variants = [linked] + asList(linked.hasVariant)
        def name = { 
            Map p -> (p.givenName && p.familyName) 
                    ? Unicode.asciiFold("${p.givenName} ${p.familyName}")
                    : p.name ? Unicode.asciiFold("${p.name}") : null
        }
        name(local) && variants.any {
            name(it) && name(local) == name(it)    
        }
    }

    static boolean yearMismatch(Map local, Map linked) {
        def birth = { Map p -> p.lifeSpan?.with { (it.split('-') as List)[0] } }
        def death = { Map p -> p.lifeSpan?.with { (it.split('-') as List)[1] } }
        def b = birth(local) && birth(linked) && birth(local) != birth(linked)
        def d = death(local) && death(linked) && death(local) != death(linked)
        b || d
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

    private Collection<Doc> loadDocs(Collection<String> cluster) {
        whelk
                .bulkLoad(cluster).values()
                .collect { new Doc(whelk, it) }
    }

    private Collection<Collection<Doc>> titleClusters(Collection<String> cluster) {
        loadDocs(cluster)
                .findAll(qualityMonographs)
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









