package datatool.scripts.mergeworks

import org.apache.commons.codec.digest.DigestUtils

import static datatool.scripts.mergeworks.FieldStatus.COMPATIBLE
import static datatool.scripts.mergeworks.FieldStatus.DIFF
import static datatool.scripts.mergeworks.FieldStatus.EQUAL

class Html {
    private static String CSS = Html.class.getClassLoader()
            .getResourceAsStream('merge-works/table.css').getText("UTF-8")

    static final String START = """<html><head>
                    <meta charset="UTF-8">
                    <style>$CSS</style>
                    </head><body>"""
    static final String END = '</body></html>'
    static final String HORIZONTAL_RULE = "<hr/><br/>\n"

    static def infoFields = ['reproductionOf', 'instance title', 'work title', 'instance type', 'editionStatement', 'responsibilityStatement', 'encodingLevel', 'publication', 'identifiedBy', 'extent']

    static String clusterTable(Collection<Doc> cluster) {
        String id = clusterId(cluster.collect { it.doc.shortId })
        String header = """
            <tr>
                <th><a id="${id}"><a href="#${id}">${id}</th>
                ${cluster.collect { doc -> "<th><a id=\"${doc.doc.shortId}\" href=\"${doc.link()}\">${doc.doc.shortId}</a></th>" }.join('\n')}
            </tr>
            <tr>
                <td></td>
                ${cluster.collect { doc -> "<td>${doc.instanceDisplayTitle()}</td>" }.join('\n')}                                                             
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

    static String hubTable(List<Collection<Doc>> docs) {
        def mergedWorks = docs*.first()
        def ids = docs.collect { group ->
            group.drop(1).collectEntries { doc ->
                [doc.doc.shortId, doc.link()]
            }
        }
        def clusterId = clusterId(ids*.keySet().flatten())

        String header = """
            <tr>
                <th><a id="${clusterId}"><a href="#${clusterId}">${clusterId}</th>
                ${mergedWorks.collect { "<th></th>" }.join('\n')}
            </tr>
           """.stripIndent()

        String derivedFrom =
                """
                    <tr class="info">
                        <td>_derivedFrom</td>
                        ${ids.collect { "<td>${it.collect { id, link -> "<a id=\"$id\" href=\"$link\">$id</a>" }.join('\n')}</td>" }.join('\n')}
                        </tr> 
                """.stripIndent()

        def statuses = WorkComparator.compare(mergedWorks)

        String equal = statuses.get(EQUAL, []).collect(fieldRows(mergedWorks, mergedWorks.size() > 1 ? EQUAL.toString() : "")).join('\n')
        String compatible = statuses.get(COMPATIBLE, []).collect(fieldRows(mergedWorks, COMPATIBLE.toString())).join('\n')
        String diff = statuses.get(DIFF, []).collect(fieldRows(mergedWorks, DIFF.toString())).join('\n')

        return """
            <table>
                ${header}
                ${equal}
                ${compatible}
                ${diff}
                ${derivedFrom}
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
                ${cluster.collect { "<td>${it.getDisplayText(field)}</td>" }.join('\n')}   
            </tr> """.stripIndent()
        }
    }
}
