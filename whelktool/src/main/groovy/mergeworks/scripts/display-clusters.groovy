package mergeworks.scripts

import mergeworks.Doc
import mergeworks.Html

htmlReport = getReportWriter('clusters.html')

htmlReport.println(Html.START)

new File(System.getProperty('clusters')).splitEachLine(~/[\t ]+/) { cluster ->
    List<Doc> docs = Collections.synchronizedList([])

    selectByIds(cluster) {
        it.getVersions()
                .reverse()
                .find { getAtPath(it.data, it.workIdPath) == null }
                ?.with { docs.add(new Doc(getWhelk, it)) }
    }

    docs.each { it.addComparisonProps() }

    htmlReport.println(Html.clusterTable(docs) + Html.HORIZONTAL_RULE)
}

htmlReport.println(Html.END)