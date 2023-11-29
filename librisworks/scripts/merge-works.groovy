/**
 * Match and merge works.
 *
 * First create clusters of works that are considered equal according to given criteria.
 * If a work cluster contains only local works (two or more), merge those and create a new linkable work.
 * If a work cluster contains exactly one linked work and at least one local work, merge the local work(s) into the linked one.
 * If a work cluster contains two or more linked works, report. There should be no duplicate linked works.
 *
 * If multiple work clusters are found, add closeMatch links from each unique work to each resulting linked work.
 *
 * See script for details.
 */

import se.kb.libris.mergeworks.Html
import se.kb.libris.mergeworks.WorkComparator
import se.kb.libris.mergeworks.Doc

import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY

import static se.kb.libris.mergeworks.Util.workClusters
import static whelk.JsonLd.THING_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.WORK_KEY

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
        // Only local works have instance data in the same record
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
            saveAndLink(workDoc, instanceDocs, workDoc.existsInStorage)
//            writeWorkReport(docs, workDoc, instanceDocs, WorkStatus.UPDATED)
            return
        }
        // New merged work
        if (!workDoc.existsInStorage && !workDoc.instanceData) {
            addAdminMetadata(workDoc, instanceDocs.collect { [(ID_KEY): it.recordIri()] })
            addCloseMatch(workDoc, linkableWorks)
            saveAndLink(workDoc, instanceDocs, workDoc.existsInStorage)
//            writeWorkReport(docs, workDoc, instanceDocs, WorkStatus.NEW)
            return
        }
        // Local work, save if new closeMatch links created
        if (workDoc.instanceData && addCloseMatch(workDoc, linkableWorks)) {
            saveAndLink(workDoc)
        }
    }

    // Multiple unique works in same title cluster, save report showing how they differ.
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

    if (!instanceDocs.isEmpty()) {
        selectByIds(instanceDocs.collect { it.shortId() }) {
            it.graph[1][WORK_KEY] = [(ID_KEY): workDoc.thingIri()]
            it.scheduleSave(changedBy: changedBy, generationProcess: generationProcess)
        }
    }
}

Doc createNewWork(Map workData) {
    workData[ID_KEY] = "TEMPID#it"
    Map data = [
            (GRAPH_KEY): [
                    [
                            (ID_KEY)   : "TEMPID",
                            (TYPE_KEY) : "Record",
                            (THING_KEY): [(ID_KEY): "TEMPID#it"],

                    ],
                    workData
            ]
    ]

    return new Doc(create(data))
}

void addAdminMetadata(Doc doc, List<Map> derivedFrom) {
    doc.record()['hasChangeNote'] = [
            [
                    (ID_KEY): 'CreateNote',
                    'tool' : ['@id': 'https://id.kb.se/generator/mergeworks']
            ]
    ]
    doc.record()['derivedFrom'] = derivedFrom
    doc.record()['descriptionLanguage'] = [(ID_KEY): 'https://id.kb.se/language/swe']
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

boolean addCloseMatch(Doc workDoc, List<Doc> linkableWorks) {
    def linkTo = linkableWorks.findAll { d ->
        d.workIri() != workDoc.thingIri()
                && d.primaryContributor() == workDoc.primaryContributor()
    }.collect { [(ID_KEY): it.workIri()] }

    def closeMatch = asList(workDoc.workData['closeMatch'])

    if (linkTo && !closeMatch.containsAll(linkTo)) {
        workDoc.workData['closeMatch'] = (closeMatch + linkTo).unique()
        return true
    }

    return false
}