package datatool.scripts.mergeworks

import datatool.util.DocumentComparator
import org.apache.commons.lang3.StringUtils
import org.codehaus.jackson.map.ObjectMapper
import org.skyscreamer.jsonassert.JSONCompare
import org.skyscreamer.jsonassert.JSONCompareMode
import org.skyscreamer.jsonassert.JSONCompareResult
import se.kb.libris.Normalizers
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.util.DocumentUtil
import whelk.util.Statistics
import whelk.util.Unicode

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

class MergeWorks {
    Whelk whelk
    Statistics statistics
    File clusters

    AtomicInteger clusterId = new AtomicInteger()

    ObjectMapper mapper = new ObjectMapper()

    MergeWorks(File clusters) {
        this.clusters = clusters

        this.whelk = Whelk.createLoadedSearchWhelk('secret', true)
        this.statistics = new Statistics()
    }

    //overflow-x: auto;
    //white-space: nowrap;
    void show(List<List<String>> diffPaths = []) {
        println("""<html><head>
                    <meta charset="UTF-8">
                    <style>
                    table {
                      border-collapse: collapse;
                    }
                    
                    table, th, td {
                      border: 1px solid grey;
                    }
                    th {
                      text-align: left;
                    }
                    tr.info td{
                      background-color: lightgrey;
                    }
                    tr.diff td{
                      background-color: lightpink;
                    }
                    tr.ok td{
                      background-color: lightgreen;
                    }
                    td {
                      vertical-align: top;
                    }
                    hr {
                      border: 4px solid;
                    }
                    </style>
                    </head><body>""")
        run({ cluster ->
            return {
                try {
                    Collection<Collection<Doc>> docs = titleClusters(cluster)
                            .sort { a, b -> a.first().instanceDisplayTitle() <=> b.first().instanceDisplayTitle() }

                    println(docs
                            .collect { it.sort{ a, b -> a.getWork()['@type'] <=> b.getWork()['@type'] } }
                            .collect { clusterTable(it, diffPaths) }
                            .join('') + "<hr/><br/>\n")
                }
                catch (Exception e) {
                    System.err.println(e.getMessage())
                }
            }
        })
        println("""</body""")
        println('</html>')
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

    void edition() {
        statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                String titles = cluster.collect(whelk.&getDocument).collect {
                    getPathSafe(it.data, ['@graph', 1, 'editionStatement'])
                }.grep().join('\n')

                if (!titles.isBlank()) {
                    println(titles + '\n')
                }
            }
        })
    }

    void fiction() {
        //statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                Collection<Collection<Doc>> titleClusters = titleClusters(cluster)
                        .sort { a, b -> a.first().instanceDisplayTitle() <=> b.first().instanceDisplayTitle() }

                for (titleCluster in titleClusters) {
                    if (titleCluster.size() > 1) {
                        Set<String> diffFields = getDiffFields(titleCluster, ['contribution', 'genreForm'] as Set)
                        if (!diffFields.contains('contribution') && diffFields.contains('genreForm')) {
                            String gf = titleCluster.collect{ it.get('genreForm') }.join(' ')
                            if (gf.contains('marc/FictionNotFurtherSpecified') && gf.contains('marc/NotFictionNotFurtherSpecified')) {
                                println(titleCluster.collect{ it.getDoc().shortId }.join('\t'))
                            }
                        }
                    }
                }
            }
        })
    }

    void fiction2() {
        //statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                if (docs(cluster).any{ it.isFiction() }) {
                    println(cluster.join('\t'))
                }
            }
        })
    }

    static def infoFields = ['instance title', 'work title', 'instance type', 'editionStatement', 'responsibilityStatement', 'encodingLevel', 'publication']
    String clusterTable(Collection<Doc> cluster, List<List<String>> diffPaths = []) {
        Set<String> fields = new HashSet<>()
        cluster.each { fields.addAll(it.getWork().keySet()) }
        Set<String> diffFields = getDiffFields(cluster, fields)

        String id = "${clusterId.incrementAndGet()}"
        String header = """
            <tr>
                <th><a id="${id}"><a href="#${id}">${id}</th>
                ${cluster.collect { doc -> "<th><a href=\"${doc.link()}\">${doc.instanceDisplayTitle()}</a></th>" }.join('\n')}                                                             
            </tr>
           """.stripIndent()

        String info = infoFields.collect(fieldRows(cluster, "info")).join('\n')
        String diff = diffFields.collect(fieldRows(cluster, "diff")).join('\n')
        String same = (fields - diffFields).collect(fieldRows(cluster, cluster.size() > 1 ? "ok" : "")).join('\n')

        return """
            <table>
                ${header}   
                ${same}
                ${diff}
                ${info}
            </table>
            <br/><br/>
        """
    }

    Set<String> getDiffFields(Collection<Doc> cluster, Set<String> fields) {
        Set<String> diffFields = new TreeSet<>()

        [cluster, cluster].combinations().each { List combination ->
            Doc a = combination.first()
            Doc b = combination.last()
            fields.each { p ->
                if (!equal(a, b, p)) {
                    diffFields.add(p)
                }
            }
        }

        return diffFields
    }

    def fieldRows(Collection<Doc> cluster, String cls) {
        { field ->
            """
            <tr class="${cls}">
                <td>${field}</td>
                ${cluster.collect { "<td>${it.get(field)}</td>" }.join('\n')}   
            </tr> """.stripIndent()
        }
    }

    void merge() {
        statistics.printOnShutdown(10)
        run({ cluster ->
            return {
                statistics.increment('TOTAL', 'TOTAL CLUSTERS')
                List<List<Document>> result = mergeWorks(cluster)
                //statistics.increment(String.format("Cluster size %03d", cluster.size()) , String.format("Num works %03d", result.size()))
            }
        })
    }

    void run(Function<List<String>, Runnable> f) {
        ExecutorService s = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4)

        clusters.eachLine() {
            List<String> cluster = Arrays.asList(it.split(/[\t ]+/))

            s.submit({
                try {
                    f.apply(cluster).run()
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

        t.each(this.&tryMerge)

        return t
    }

    private Collection<Doc> docs(Collection<String> cluster) {
        whelk
                .bulkLoad(cluster).values()
                .collect { new Doc(whelk, it) }
                .findAll { it.isText() && it.isMonograph() }
    }

    private Collection<Collection<Doc>> titleClusters(Collection<String> cluster) {
        docs(cluster)
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

    boolean equal(Doc a, Doc b, String field) {
        JSONCompareResult r = JSONCompare.compareJSON(
                mapper.writeValueAsString(a.getWork().getOrDefault(field, "")),
                mapper.writeValueAsString(b.getWork().getOrDefault(field, "")),
                JSONCompareMode.NON_EXTENSIBLE)

        return !r.failed()
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
        return Unicode.normalizeForSearch(StringUtils.normalizeSpace(" $s ".toLowerCase().replace(noise)))
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

    /**
     * Partition a collection based on equality condition
     *
     * NOTE: O(n^2)...
     */
    static <T> Collection<Collection<T>> partition(Collection<T> collection, Closure matcher) {
        List<List<T>> result = []

        for (T t : collection) {
            boolean match = false
            for (List<T> group : result) {
                if (groupMatches(t, group, matcher)) {
                    group.add(t)
                    match = true
                    break
                }
            }

            if (!match) {
                result.add([t])
            }
        }
        return result
    }

    static <T> boolean groupMatches(T t, List<T> group, Closure matcher) {
        for (T other : group) {
            if (matcher(other, t)) {
                return true
            }
        }
        return false
    }

    class NoWorkException extends RuntimeException {
        NoWorkException(String msg) {
            super(msg)
        }
    }

    private boolean nonsenseSubtitle(String s) {
        s = normalize(s)
        if (s.startsWith("en ")) {
            s = s.substring("en ".length())
        }
        return s in SUBTITLES
    }

    private static Set<String> SUBTITLES = """
a comedy
a history
a novel
a play
a romance
a tale
aforismer
berättelse
berättelse för barn
berättelse för flickor
berättelse för pojkar
berättelse för unga flickor
berättelser
bilderbok
comédie
contos
deckare
detektivroman
dikt
dikter
drama
ein coq-rouge-thriller
ein roman
eine erzählung
erzählung
erzählungen
essays
essäer
ett fall för kay scarpetta
fortælling
historisk roman
homandeckare
jack reacher-thriller
komedi
komedi i fyra akter
krimi
kriminalroman
kärlekshistoria
kärleksroman
kåserier
lustspel i en akt
nouvelles
novela
novell
novelle
noveller
pjäs
polisroman
povesti
powieść
poėma
reseguide
resehandbok
rikosromaani
romaani
romaani rikoksesta
roman
roman om skivvärlden
romance
romanzo
runoja
saga
sagor
sann historia
skildringar
skáldsaga
spänningsroman
stories
thriller
ungdomsroman
""".readLines().grep() as Set
}

class Doc {
    public static final String SAOGF_SKÖN = 'https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur'
    public static final String MARC_FICTION = 'https://id.kb.se/marc/FictionNotFurtherSpecified'
    Whelk whelk
    Document doc
    Map work
    Map framed
    List<String> titles

    Doc(Whelk whelk, Document doc) {
        this.whelk = whelk
        this.doc = doc
    }

    Map getWork() {
        if (!work) {
            work = MergeWorks.getWork(whelk, doc)
        }

        return work
    }

    Map getInstance() {
        return doc.data['@graph'][1]
    }

    def titleVariant = ['Title', 'ParallelTitle', 'VariantTitle', 'CoverTitle']

    List<String> getTitleVariants() {
        if (!titles) {
            titles = getWork()['hasTitle'].grep { it['@type'] in titleVariant }.collect { it['flatTitle'] }
        }

        return titles
    }

    private String displayTitle(Map thing) {
        thing['hasTitle'].collect { it['@type'] + ": " + it['flatTitle'] }.join(', ')
    }

    String instanceDisplayTitle() {
        displayTitle(['hasTitle': MergeWorks.flatTitles(getInstance())])
    }

    String link() {
        String base = Document.getBASE_URI().toString()
        String kat = "katalogisering/"
        String id = doc.shortId
        return base + kat + id
    }

    boolean isMonograph() {
        getInstance()['issuanceType'] == 'Monograph'
    }

    String get(String field) {
        if (field == 'contribution') {
            return contributorStrings().join("<br>")
        }
        else if (field == 'classification') {
            return classificationStrings().join("<br>")
        }
        else if (field == 'instance title') {
            return getInstance()['hasTitle'] ?: ''
        }
        else if (field == 'work title') {
            return getFramed()['instanceOf']['hasTitle'] ?: ''
        }
        else if (field == 'instance type') {
            return getInstance()['@type']
        }
        else if (field == 'editionStatement') {
            return getInstance()['editionStatement'] ?: ''
        }
        else if (field == 'responsibilityStatement') {
            return getInstance()['responsibilityStatement'] ?: ''
        }
        else if (field == 'encodingLevel') {
            return doc.data['@graph'][0]['encodingLevel'] ?: ''
        }
        else if (field == 'publication') {
            return chipString(getInstance()['publication'] ?: [])
        }
        else {
            return chipString(getWork().getOrDefault(field, []))
        }
    }

    private String chipString (def thing) {
        def chips = whelk.jsonld.toChip(thing)
        if (chips.size() < 2) {
            chips = thing
        }
        if (chips instanceof List) {
            return chips.collect{ valuesString(it) }.sort().join('<br>')
        }
        return valuesString(chips)
    }

    private String valuesString (def thing) {
        if (thing instanceof List) {
           return thing.collect{ valuesString(it) }.join(' • ')
        }
        if (thing instanceof Map) {
            return thing.findAll { k, v -> k != '@type'}.values().collect{ valuesString(it) }.join(' • ')
        }
        return thing.toString()
    }

    String tooltip(String string, String tooltip) {
        """<abbr title="${tooltip}">${string}</abbr>"""
    }

    private List classificationStrings() {
        List<Map> classification =  MergeWorks.getPathSafe(getFramed(), ['instanceOf', 'classification'], [])
        classification.collect() { c ->
            StringBuilder s = new StringBuilder()
            s.append(flatMaybeLinked(c['inScheme'], ['code', 'version']).with { it.isEmpty() ? it : it + ': ' })
            s.append(flatMaybeLinked(c, ['code']))
            return s.toString()
        }
    }

    private List contributorStrings() {
        List contribution = MergeWorks.getPathSafe(getFramed(), ['instanceOf', 'contribution'], [])

        return contribution.collect { Map c ->
            contributionStr(c)
        }
    }

    private Map getFramed() {
        if(!framed) {
            framed = JsonLd.frame(doc.getThingIdentifiers().first(), whelk.loadEmbellished(doc.shortId).data)
        }

        return framed
    }

    private String contributionStr(Map contribution) {
        StringBuilder s = new StringBuilder()

        if (contribution['@type'] == 'PrimaryContribution') {
            s.append('<b>')
        }

        s.append(flatMaybeLinked(contribution['role'], ['code', 'label']).with { it.isEmpty() ? it : it + ': ' })
        s.append(flatMaybeLinked(contribution['agent'], ['givenName', 'familyName', 'lifeSpan', 'name']))

        if (contribution['@type'] == 'PrimaryContribution') {
            s.append('</b>')
        }

        return s.toString()
    }

    private static String flatten(Object o, List order, String mapSeparator = ': ') {
        if (o instanceof String) {
            return o
        }
        if (o instanceof List) {
            return o
                    .collect { flatten(it, order) }
                    .join(' || ')
        }
        if (o instanceof Map) {
            return order
                    .collect { o.get(it, null) }
                    .grep { it != null }
                    .collect { flatten(it, order) }
                    .join(mapSeparator)
        }

        throw new RuntimeException(String.format("unexpected type: %s for %s", o.class.getName(), o))
    }

    private String flatMaybeLinked(Object thing, List order) {
        if (!thing)
            return ''

        if (thing instanceof List) {
            return thing.collect { flatMaybeLinked(it, order) }.join(' | ')
        }
        String s = flatten(thing, order, ', ')

        thing['@id']
                ? """<a href="${thing['@id']}">$s</a>"""
                : s
    }

    boolean isFiction() {
        isMarcFiction() || isSaogfFiction() || isSabFiction()
    }

    boolean isMarcFiction() {
        (getWork()['genreForm'] ?: []).any{ it['@id'] == MARC_FICTION }
    }

    boolean isSaogfFiction() {
        (getWork()['genreForm'] ?: []).any{ whelk.relations.isImpliedBy(SAOGF_SKÖN, it['@id'] ?: '') }
    }

    boolean isSabFiction() {
        classificationStrings().any{ it.contains('kssb') && it.contains(': H') }
    }

    boolean isText() {
        getWork()['@type'] == 'Text'
    }
}
