package datatool.scripts.mergeworks.normalize

import whelk.Document

import static datatool.scripts.mergeworks.Util.asList
import static datatool.scripts.mergeworks.Util.getPathSafe
import static datatool.scripts.mergeworks.Util.contributionPath
import static datatool.scripts.mergeworks.Util.clusters
import static datatool.scripts.mergeworks.Util.Relator

/**
 Example:
 $ ENV=qa && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters="reports/clusters.tsv" -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run src/main/groovy/datatool/scripts/mergeworks/normalize/add-9pu-to-illustrators.groovy
 */

PrintWriter report = getReportWriter("report.txt")

def ill = ['@id': Relator.ILLUSTRATOR.iri]
def pu = ['@id': Relator.PRIMARY_RIGHTS_HOLDER.iri]

new File(System.getProperty(clusters)).splitEachLine('\t') { cluster ->
    incrementStats('add 9pu', 'clusters checked')

    def docs = Collections.synchronizedList([])
    selectByIds(cluster.collect { it.trim() }) {
        docs << it.doc
    }

    docs.each { Document d ->
        incrementStats('add 9pu', 'docs checked')

        getPathSafe(d.data, contributionPath, []).each { Map c ->
            def r = asList(c.role)

            if (pu in r || !(ill in r) || c.'@type' == 'PrimaryContribution')
                return

            for (Document other : docs) {

                def found9pu = getPathSafe(other.data, contributionPath, []).any { Map oc ->
                    asList(c.agent) == asList(oc.agent) && asList(oc.role).containsAll([ill, pu])
                }

                if (found9pu) {
                    c.role = asList(c.role) + pu
                    selectByIds([d.shortId]) {
                        it.doc.data = d.data
                        it.scheduleSave()
                    }
                    incrementStats('add 9pu', "9pu added")
                    report.println("${d.shortId} <- ${other.shortId}")
                    break
                }
            }
        }
    }
}