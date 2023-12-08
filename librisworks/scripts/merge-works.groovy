import se.kb.libris.mergeworks.Html
import se.kb.libris.mergeworks.WorkComparator
import se.kb.libris.mergeworks.Doc

import static se.kb.libris.mergeworks.Util.workClusters

maybeDuplicates = getReportWriter("maybe-duplicate-linked-works.tsv")
multiWorkReport = getReportWriter("multi-work-clusters.html")

enum WorkStatus {
    NEW('new'),
    UPDATED('updated')

    String status

    private WorkStatus(String status) {
        this.status = status
    }
}

WorkStatus.values().each {
    new File(reportsDir, it.status).with { it.mkdirs() }
}

def changedBy = 'SEK'
def generationProcess = 'https://libris.kb.se/sys/merge-works'

def newWorks = []
def idToUpdatedWorkData = [:]
def instanceIdToWorkId = [:]
def dataForReports = []
def dataForMultiWorkReport = []

def clusters = new File(System.getProperty('clusters')).collect {it.split(/[\t ]+/) as List }
loadDocs(clusters).each { docs ->
    WorkComparator c = new WorkComparator(WorkComparator.allFields(docs))

    List<Tuple2<Doc, Collection<Doc>>> uniqueWorksAndTheirInstances = []

    workClusters(docs, c).each { wc ->
        def (localWorks, linkedWorks) = wc.split { it.instanceData }
        if (linkedWorks.isEmpty()) {
            if (localWorks.size() == 1) {
                uniqueWorksAndTheirInstances.add(new Tuple2(localWorks.find(), localWorks))
            } else {
                Doc newWork = createNewWork(c.merge(localWorks))
                uniqueWorksAndTheirInstances.add(new Tuple2(newWork, localWorks))
            }
        } else if (linkedWorks.size() == 1) {
            uniqueWorksAndTheirInstances.add(new Tuple2(linkedWorks.find(), localWorks))
        } else {
            maybeDuplicates.println(linkedWorks.collect { it.shortId() }.join('\t'))
            System.err.println("Local works ${localWorks.collect { it.shortId() }} match multiple linked works: ${linkedWorks.collect { it.shortId() }}. Duplicated linked works?")
        }
    }

    List<Doc> linkableWorks = uniqueWorksAndTheirInstances.findResults { workDoc, _ -> workDoc.workIri() ? workDoc : null }

    uniqueWorksAndTheirInstances.each { Doc workDoc, List<Doc> instanceDocs ->
        // Link more instances to existing linked work
        if (workDoc.existsInStorage && !workDoc.instanceData && instanceDocs) {
            replaceWorkData(workDoc, c.merge([workDoc] + instanceDocs))
            // TODO: Update adminMetadata? To say that additional instances may have contributed to the linked work.
            addCloseMatch(workDoc, linkableWorks)
            idToUpdatedWorkData[workDoc.shortId()] = workDoc.document.data
            instanceDocs.each {instanceIdToWorkId[it.shortId()] = workDoc.thingIri() }
            dataForReports.add([docs, workDoc, instanceDocs, WorkStatus.UPDATED])
            return
        }
        // New merged work
        if (!workDoc.existsInStorage && !workDoc.instanceData) {
            addAdminMetadata(workDoc, instanceDocs.collect { ['@id': it.recordIri()] })
            addCloseMatch(workDoc, linkableWorks)
            newWorks.add(workDoc.docItem)
            instanceDocs.each {instanceIdToWorkId[it.shortId()] = workDoc.thingIri() }
            dataForReports.add([docs, workDoc, instanceDocs, WorkStatus.NEW])
            return
        }
        // Local work, save if new closeMatch links created
        if (workDoc.instanceData && addCloseMatch(workDoc, linkableWorks)) {
            idToUpdatedWorkData[workDoc.shortId()] = workDoc.document.data
        }
    }

    if (uniqueWorksAndTheirInstances.size() > 1) {
        dataForMultiWorkReport.add(uniqueWorksAndTheirInstances.transpose())
    }
}

selectFromIterable(newWorks) {
    it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
}

selectByIds(idToUpdatedWorkData.keySet()) {
    if (idToUpdatedWorkData[it.doc.shortId]) {
        it.doc.data = idToUpdatedWorkData[it.doc.shortId]
        it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
    }
}

selectByIds(instanceIdToWorkId.keySet()) {
    if (instanceIdToWorkId[it.doc.shortId]) {
        it.graph[1]['instanceOf'] = ['@id': instanceIdToWorkId[it.doc.shortId]]
        it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
    }
}

writeMultiWorkReport(dataForMultiWorkReport)
writeIndividualWorkReports(dataForReports)

Doc createNewWork(Map workData) {
    workData['@id'] = "TEMPID#it"
    Map data = [
            "@graph": [
                    [
                            "@id"       : "TEMPID",
                            "@type"     : "Record",
                            "mainEntity": ["@id": "TEMPID#it"],

                    ],
                    workData
            ]
    ]

    return new Doc(create(data))
}

void addAdminMetadata(Doc doc, List<Map> derivedFrom) {
    doc.record()['hasChangeNote'] = [
            [
                    '@type': 'CreateNote',
                    'tool' : ['@id': 'https://id.kb.se/generator/mergeworks']
            ]
    ]
    doc.record()['derivedFrom'] = derivedFrom
    doc.record()['descriptionLanguage'] = ['@id': 'https://id.kb.se/language/swe']
}

void writeIndividualWorkReports(List data) {
    data.each { cluster, work, derivedFrom, workStatus ->
        String report = htmlReport(cluster, work, derivedFrom)
        getReportWriter("${workStatus.status}/${work.shortId()}.html").print(report)
        incrementStats("num derivedFrom (${workStatus.status} works)", "${derivedFrom.size()}", work.shortId())
    }
}

void writeMultiWorkReport(List data) {
    multiWorkReport.print(Html.START)
    data.each { workDocs, instanceDocs ->
        multiWorkReport.print(Html.hubTable(workDocs, instanceDocs) + Html.HORIZONTAL_RULE)
    }
    multiWorkReport.print(Html.END)
}

static String htmlReport(Collection<Doc> titleCluster, Doc work, Collection<Doc> instances) {
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

    s.append("<h1>Extracted work</h1>")
    s.append(Html.clusterTable([work] + instances))

    s.append(Html.END)

    return s.toString()
}

static void replaceWorkData(Doc workDoc, Map replacement) {
    workDoc.workData.clear()
    workDoc.workData.putAll(replacement)
}

boolean addCloseMatch(Doc workDoc, List<Doc> linkableWorks) {
    def linkTo = linkableWorks.findAll { d ->
        d.workIri() != workDoc.thingIri()
                && d.primaryContributor() == workDoc.primaryContributor()
    }.collect { ['@id': it.workIri()] }

    def closeMatch = asList(workDoc.workData['closeMatch'])

    if (linkTo && !closeMatch.containsAll(linkTo)) {
        workDoc.workData['closeMatch'] = (closeMatch + linkTo).unique()
        return true
    }

    return false
}

Collection<Collection<Doc>> loadDocs(Collection<Collection<String>> clusters) {
    def idToDoc = Collections.synchronizedMap([:])
    selectByIds(clusters.flatten()) {
        idToDoc[it.doc.shortId] = new Doc(it)
    }
    return clusters.collect { c -> c.collect { id -> idToDoc[id] }.findAll() }.grep()
}