package se.kb.libris.mergeworks


import org.apache.commons.codec.digest.DigestUtils

import static FieldStatus.COMPATIBLE
import static FieldStatus.DIFF
import static FieldStatus.EQUAL

class Html {
    private static String CSS = Html.class.getClassLoader()
            .getResourceAsStream('merge-works/table.css').getText("UTF-8")

    static final String START = """<html><head>
                    <meta charset="UTF-8">
                    <style>$CSS</style>
                    </head><body>"""
    static final String END = '</body></html>'
    static final String HORIZONTAL_RULE = "<hr/><br/>\n"

    static def infoFields = ['reproductionOf', 'instance title', 'instance type', 'editionStatement', 'responsibilityStatement', 'encodingLevel', 'publication', 'identifiedBy', 'extent', 'physicalDetailsNote']

    static String clusterTable(Collection<Doc> cluster) {
        String id = clusterId(cluster.collect { it.shortId() })
        String header = """
            <tr>
                <th><a id="${id}"><a href="#${id}">${id}</th>
                ${cluster.collect { doc -> "<th><a id=\"${doc.shortId()}\" href=\"${doc.view.link()}\">${doc.shortId()}</a></th>" }.join('\n')}
            </tr>
            <tr>
                <td></td>
                ${cluster.collect { doc -> "<td>${doc.view.instanceDisplayTitle()}</td>" }.join('\n')}                                     
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

    static String hubTable(Collection<Doc> workDocs, Collection<Collection<Doc>> instanceDocs) {
        def clusterId = clusterId(instanceDocs.flatten().collect { Doc d -> d.shortId() })

        String header = """
            <tr>
                <th><a id="${clusterId}"><a href="#${clusterId}">${clusterId}</th>
                ${workDocs.collect { it.workIri()
                ? "<th><a id=\"${it.shortId()}\" href=\"${it.view.link()}\">${it.shortId()}</a></th>"
                : "<th></th>" }
                .join('\n')}
            </tr>
           """.stripIndent()

        def link = { Doc d -> "<a id=\"${d.shortId()}\" href=\"${d.view.link()}\">${d.shortId()}</a>" }

        String instances =
                """
                    <tr class="info">
                        <td>_instances</td>
                        ${instanceDocs.collect { "<td>${it.collect(link).join('<br>')}</td>" }.join('\n')}
                        </tr>
                """.stripIndent()

        def statuses = WorkComparator.compare(workDocs)

        String equal = statuses.get(EQUAL, []).collect(fieldRows(workDocs, workDocs.size() > 1 ? EQUAL.toString() : "")).join('\n')
        String compatible = statuses.get(COMPATIBLE, []).collect(fieldRows(workDocs, COMPATIBLE.toString())).join('\n')
        String diff = statuses.get(DIFF, []).collect(fieldRows(workDocs, DIFF.toString())).join('\n')

        return """
            <table>
                ${header}
                ${equal}
                ${compatible}
                ${diff}
                ${instances}
            </table>
            <br/><br/>
        """
    }

    static String clusterId(Collection<String> cluster) {
        cluster
                ? DigestUtils.md5Hex(cluster.sort().first()).toUpperCase().substring(0, 12)
                : ""
    }

    private static def fieldRows(Collection<Doc> cluster, String cls) {
        { field ->
            """
            <tr class="${cls}">
                <td>${field}</td>
                ${cluster.collect { "<td>${it.view.getDisplayText(field)}</td>" }.join('\n')}   
            </tr> """.stripIndent()
        }
    }
}
