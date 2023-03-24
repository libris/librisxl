package datatool.scripts.mergeworks.normalize

import whelk.Document

import static datatool.scripts.mergeworks.Util.asList
import static datatool.scripts.mergeworks.Util.getPathSafe
import static datatool.scripts.mergeworks.Util.Relator

/**
 Example:
 $ ENV=qa && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters="reports/clusters.tsv" -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run src/main/groovy/datatool/scripts/mergeworks/normalize/add-missing-translationOf.groovy
 */

PrintWriter report = getReportWriter("report.tsv")

def workPath = ['@graph', 1, 'instanceOf']
def contributionPath = workPath + ['contribution']
def translationOfPath = workPath + ['translationOf']
def trl = ['@id': Relator.TRANSLATOR.iri]

new File(System.getProperty('clusters')).splitEachLine('\t') { cluster ->
    incrementStats('add translationOf', 'clusters checked')

    def docs = Collections.synchronizedList([])
    selectByIds(cluster.collect { it.trim() }) {
        docs << it.doc
    }

    docs.each { Document d ->
        incrementStats('add translationOf', 'docs checked')

        if (getPathSafe(d.data, translationOfPath)) {
            return
        }

        for (Map c : getPathSafe(d.data, contributionPath, [])) {
            def r = asList(c.role)

            if (!r.contains(trl)) {
                continue
            }

            def title = getPathSafe(d.data, workPath).hasTitle

            if (title) {
                selectByIds([d.shortId]) {
                    def work = getPathSafe(it.doc.data, workPath)
                    work['translationOf'] = ['@type': 'Work', 'hasTitle': title]
                    work.remove('hasTitle')
                    incrementStats('add translationOf', "title moved to new translationOf")
                    report.println("${it.doc.shortId}\t${work['translationOf']}")
                    it.scheduleSave()
                }
                break
            }

            def found = false

            for (Document other : docs) {
                def hasSameTranslator = getPathSafe(other.data, contributionPath, []).any { Map oc ->
                    asList(c.agent) == asList(oc.agent) && asList(oc.role).contains(trl)
                }

                if (hasSameTranslator) {
                    def translationOf = getPathSafe(other.data, translationOfPath)
                    if (translationOf) {
                        selectByIds([d.shortId]) {
                            getPathSafe(it.doc.data, workPath)['translationOf'] = translationOf
                            it.scheduleSave()
                        }
                        incrementStats('add translationOf', "translationOf added from other")
                        report.println("${d.shortId} <- ${other.shortId}\t${translationOf}")
                        found = true
                        break
                    }
                }
            }

            if (found) {
                break
            }
        }
    }
}

