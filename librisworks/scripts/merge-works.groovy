import se.kb.libris.mergeworks.Html
import se.kb.libris.mergeworks.WorkComparator
import se.kb.libris.mergeworks.Doc

import static se.kb.libris.mergeworks.Util.partition
import static se.kb.libris.mergeworks.Util.sortByIntendedAudience

maybeDuplicates = getReportWriter("maybe-duplicate-linked-works.tsv")
multiWorkReport = getReportWriter("multi-work-clusters.html")
multiWorkReport.print(Html.START)

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

new File(System.getProperty('clusters')).splitEachLine(~/[\t ]+/) { cluster ->
    def docs = Collections.synchronizedList([])
    selectByIds(cluster) {
        docs.add(new Doc(it))
    }

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
            System.err.println("Local works ${localWorks.collect { it.shortId() }} match multiple linked works: ${linkedWorks.collect { it.shortId() }}. Duplicate linked works?")
        }
    }

    List<String> linkableWorkIris = uniqueWorksAndTheirInstances.findResults { it.getV1().workIri() }

    uniqueWorksAndTheirInstances.each { Doc workDoc, List<Doc> instanceDocs ->
        if (!workDoc.instanceData) {
            if (workDoc.existsInStorage) {
                if (instanceDocs) {
                    replaceWorkData(workDoc, c.merge([workDoc] + instanceDocs))
                    // TODO: Add adminmetadata
                    writeWorkReport(docs, workDoc, instanceDocs, WorkStatus.UPDATED)
                }
            } else {
                addTechnicalNote(workDoc, WorkStatus.NEW) //TODO: Add more/better adminmetadata
                writeWorkReport(docs, workDoc, instanceDocs, WorkStatus.NEW)
            }
            addCloseMatch(workDoc, linkableWorkIris)
            saveAndLink(workDoc, instanceDocs, workDoc.existsInStorage)
        } else {
            if (addCloseMatch(workDoc, linkableWorkIris)) {
                saveAndLink(workDoc)
            }
        }
    }

    if (uniqueWorksAndTheirInstances.size() > 1) {
        def (workDocs, instanceDocs) = uniqueWorksAndTheirInstances.transpose()
        multiWorkReport.print(Html.hubTable(workDocs, instanceDocs) + Html.HORIZONTAL_RULE)
    }
}

multiWorkReport.print(Html.END)

void saveAndLink(Doc workDoc, Collection<Doc> instanceDocs = [], boolean existsInStorage = true) {
    def changedBy = 'SEK'
    def generationProcess = 'https://libris.kb.se/sys/merge-works'

    if (existsInStorage) {
        selectByIds([workDoc.shortId()]) {
            it.doc.data = workDoc.document.data
            it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
        }
    } else {
        selectFromIterable([workDoc.docItem]) {
            it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
        }
    }

    selectByIds(instanceDocs.collect { it.shortId() }) {
        it.graph[1]['instanceOf'] = ['@id': workDoc.thingIri()]
        it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
    }
}

Collection<Collection<Doc>> workClusters(Collection<Doc> docs, WorkComparator c) {
    docs.each {
        if (it.instanceData) {
            it.addComparisonProps()
        }
    }.with { sortByIntendedAudience(it) }

    def workClusters = partition(docs, { Doc a, Doc b -> c.sameWork(a, b) })
            .each { work -> work.each { doc -> doc.removeComparisonProps() } }

    return workClusters
}

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

void addTechnicalNote(Doc doc, WorkStatus workStatus) {
    def reportUri = "http://xlbuild.libris.kb.se/works/${reportsDir.getPath()}/${workStatus.status}/${doc.shortId()}.html"

    doc.record()['technicalNote'] = [[
                                             "@type"  : "TechnicalNote",
                                             "hasNote": [[
                                                                 "@type": "Note",
                                                                 "label": ["Maskinellt utbrutet verk... TODO"]
                                                         ]],
                                             "uri"    : [reportUri]
                                     ]]
}

void writeWorkReport(Collection<Doc> titleCluster, Doc derivedWork, Collection<Doc> derivedFrom, WorkStatus workStatus) {
    String report = htmlReport(titleCluster, derivedWork, derivedFrom)
    getReportWriter("${workStatus.status}/${derivedWork.shortId()}.html").print(report)
    incrementStats("num derivedFrom (${workStatus.status} works)", "${derivedFrom.size()}", derivedWork.shortId())
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

boolean addCloseMatch(Doc workDoc, List<String> workIris) {
    def linkable = (workIris - workDoc.thingIri()).collect { ['@id': it] }
    def closeMatch = asList(workDoc.workData['closeMatch'])

    if (linkable && !closeMatch.containsAll(linkable)) {
        workDoc.workData['closeMatch'] = (closeMatch + linkable).unique()
        return true
    }

    return false
}