import se.kb.libris.mergeworks.Doc
import se.kb.libris.mergeworks.Html
import se.kb.libris.mergeworks.WorkComparator

import static se.kb.libris.mergeworks.Util.workClusters

htmlReport = getReportWriter('works.html')

htmlReport.println(Html.START)

new File(System.getProperty('clusters')).splitEachLine(~/[\t ]+/) { cluster ->
    List<Doc> docs = Collections.synchronizedList([])

    selectByIds(cluster) {
        it.getVersions()
                .reverse()
                .find { getAtPath(it.data, it.workIdPath) == null }
                ?.with { docs.add(new Doc(getWhelk(), it)) }
    }

    WorkComparator c = new WorkComparator(WorkComparator.allFields(docs))

    def workClusters = workClusters(docs, c).findAll { it.size() > 1 }

    workClusters.collect { [createNewWork(c.merge(it))] + it }
            .each { htmlReport.println(Html.clusterTable(it) + Html.HORIZONTAL_RULE) }
}

htmlReport.println(Html.END)

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