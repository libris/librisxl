package mergeworks.scripts

import mergeworks.Html
import mergeworks.WorkComparator
import mergeworks.Doc
import whelk.datatool.DocumentItem

import static mergeworks.Util.partition

multiWorkReport = getReportWriter("multi-work-clusters.html")
multiWorkReport.print(Html.START)

String changedBy = 'SEK'
String generationProcess = 'https://libris.kb.se/sys/merge-works'

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
        if (!linkedWorks) {
            if (localWorks.size() == 1) {
                uniqueWorksAndTheirInstances.add(new Tuple2(localWorks.find(), localWorks))
            } else {
                DocumentItem newWork = createNewWork(c.merge(localWorks))
                addTechnicalNote(newWork, WorkStatus.NEW) //TODO: Add more/better adminmetadata
                selectFromIterable([newWork]) {
                    it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
                }
                Doc workDoc = new Doc(newWork)
                uniqueWorksAndTheirInstances.add(new Tuple2(workDoc, localWorks))
                writeWorkReport(docs, workDoc, localWorks, WorkStatus.NEW)
                selectByIds(localWorks.collect { it.shortId() }) {
                    it.graph[1]['instanceOf'] = ['@id': workDoc.thingIri()]
                    it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
                }
            }
        } else if (linkedWorks.size() == 1) {
            Doc workDoc = linkedWorks.find()
            workDoc.replaceWorkData(c.merge(linkedWorks + localWorks))
            // TODO: Add adminmetadata
            uniqueWorksAndTheirInstances.add(new Tuple2(workDoc, localWorks))
            writeWorkReport(docs, workDoc, localWorks, WorkStatus.UPDATED)
            selectByIds(localWorks.collect { it.shortId() }) {
                it.graph[1]['instanceOf'] = ['@id': workDoc.thingIri()]
                it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
            }
        } else {
            System.err.println("Local works ${localWorks.collect { it.shortId() }} match multiple linked works: ${linkedWorks.collect { it.shortId() }}. Duplicate linked works?")
        }
    }

    if (uniqueWorksAndTheirInstances.size() > 1) {
        def (workDocs, instanceDocs) = uniqueWorksAndTheirInstances.transpose()
        multiWorkReport.print(Html.hubTable(workDocs, instanceDocs) + Html.HORIZONTAL_RULE)
    }

    List<Doc> uniqueWorks = uniqueWorksAndTheirInstances.collect { it.getV1() }
    List<String> linkableWorkIris = uniqueWorks.findResults { it.workIri() }

    uniqueWorks.each { Doc workDoc ->
        workDoc.addCloseMatch(linkableWorkIris)
        selectByIds([workDoc.shortId()]) {
            it.doc.data = workDoc.document.data
            it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
        }
    }
}

multiWorkReport.print(Html.END)

Collection<Collection<Doc>> workClusters(Collection<Doc> docs, WorkComparator c) {
    docs.each {
        if (it.instanceData) {
            it.addComparisonProps()
        }
    }

    def workClusters = partition(docs, { Doc a, Doc b -> c.sameWork(a, b) })
            .each { work -> work.each { doc -> doc.removeComparisonProps() } }

    return workClusters
}

DocumentItem createNewWork(Map workData) {
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

    return create(data)
}

void addTechnicalNote(DocumentItem docItem, WorkStatus workStatus) {
    def reportUri = "http://xlbuild.libris.kb.se/works/${reportsDir.getPath()}/${workStatus.status}/${docItem.doc.shortId}.html"

    docItem.graph[0]['technicalNote'] = [[
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