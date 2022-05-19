import groovy.transform.Memoized
import whelk.util.Statistics

import static datatool.scripts.mergeworks.Util.asList
import static datatool.scripts.mergeworks.Util.parseRespStatement
import static datatool.scripts.mergeworks.Util.getPathSafe
import static datatool.scripts.mergeworks.Util.Relator
import static datatool.scripts.mergeworks.Util.bestEncodingLevel
import static datatool.scripts.mergeworks.WorkToolJob.nameMatch

PrintWriter allStatements = getReportWriter("all-statements.csv")
PrintWriter notParsed = getReportWriter("not-parsed.txt")
PrintWriter roleSpecified = getReportWriter("role-specified.tsv")
PrintWriter agentFoundInCluster = getReportWriter("agent-found-in-cluster.tsv")
PrintWriter parsedButUnmatched = getReportWriter("parsed-but-unmatched.tsv")
PrintWriter pseudonyms = getReportWriter("pseudonyms")

Statistics s = new Statistics().printOnShutdown()

def clusters = System.getProperty('clustersDir')
        .with {new File(it, 'clusters.tsv') }
        .collect { it.split() as List }

clusters.each { cluster ->
    s.increment('fetch contribution from respStatement', 'clusters checked')

    selectByIds(cluster) { bib ->
        def data = bib.doc.data
        def id = bib.doc.shortId
        def respStatement = getPathSafe(data, ['@graph', 1, 'responsibilityStatement'])
        def encodingLevel = getPathSafe(data, ['@graph', 0, 'encodingLevel'])

        if (!respStatement)
            return

        s.increment('fetch contribution from respStatement', 'docs checked')
        allStatements.println(respStatement)

        def contributionsInRespStmt = parseRespStatement(respStatement)
        def contribution = getPathSafe(data, ['@graph', 1, 'instanceOf', 'contribution'], [])

        if (contributionsInRespStmt.isEmpty()) {
            notParsed.println([respStatement, id].join('\t'))
            return
        }

        contribution.each { Map c ->
            asList(c.agent).each { a ->
                def matchedOnName = contributionsInRespStmt.find { n, r ->
                    nameMatch(n, loadIfLink(a))
                }

                if (!matchedOnName)
                    return

                // Contributor found locally, omit from further search
                contributionsInRespStmt.remove(matchedOnName.key)


                def dontAdd = { Relator relator, boolean isFirstStmtPart ->
                    relator == Relator.UNSPECIFIED_CONTRIBUTOR
                            || isFirstStmtPart && relator == Relator.AUTHOR
                                && c.'@type' != 'PrimaryContribution'
                }

                def rolesInRespStatement = matchedOnName.value
                        .findResults { dontAdd(it) ? null : it.getV1() }

                if (rolesInRespStatement.isEmpty())
                    return

                def rolesInContribution = asList(c.role).findAll { it.'@id' != Relator.UNSPECIFIED_CONTRIBUTOR.iri }
                def roleShort = { it.split('/').last() }
                def joinRoles = { roles -> roles.collect { r -> r.'@id' ? roleShort(r.'@id') : 'BLANK' }.join('|') }

                rolesInRespStatement.removeAll { r ->
                    r == Relator.EDITOR && rolesInContribution.findIndexOf {
                        it.'@id' == Relator.ADAPTER.iri
                    }.with {
                        if (it == -1) {
                            return false
                        } else {
                            rolesInContribution[it]['@id'] = Relator.EDITOR.iri
                            return true
                        }
                    }
                }

                if (rolesInRespStatement.size() <= rolesInContribution.size())
                    return

                rolesInRespStatement.each { r ->
                    def idLink = ['@id': r.iri]
                    if (!(idLink in rolesInContribution)) {
                        rolesInContribution << idLink
                        s.increment('fetch contribution from respStatement', "${roleShort(r.iri)} roles specified")
                        roleSpecified.println([id, joinRoles(asList(c.role)), joinRoles(rolesInContribution), matchedOnName.key, respStatement].join('\t'))
                    }
                }
            }
        }

        def comparable = {
            it*.getV1().findResults { Relator r ->
                r != Relator.UNSPECIFIED_CONTRIBUTOR
                        ? ['@id': r.iri]
                        : null
            }
        }

        contributionsInRespStmt.each { name, roles ->
            def roleShort = { it.getV1().iri.split('/').last() }
            def concat = { it.collect { r -> roleShort(r) }.join('|') }

            def found = false

            for (String otherId : cluster) {
                def doc = loadDoc(otherId)
                if (!doc)
                    continue
                def otherEncodingLevel = getPathSafe(doc.data, ['@graph', 0, 'encodingLevel'])

                def matched = getPathSafe(doc.data, ['@graph', 1, 'instanceOf', 'contribution'], [])
                        .find { Map c ->
                            asList(c.agent).any { a ->
                                nameMatch(name, loadIfLink(a))
                                        && comparable(roles).with { r -> !r.isEmpty() && asList(c.role).containsAll(r) }
                                        && bestEncodingLevel.indexOf(encodingLevel) <= bestEncodingLevel.indexOf(otherEncodingLevel)
                            }
                        }

                if (matched) {
                    def isPseudonym = {
                        asList(it.agent).any { a ->
                            loadIfLink(a).description =~ /(?i)pseud/
                        }
                    }

                    if (isPseudonym(matched)) {
                        pseudonyms.println([id, concat(roles), name, otherId].join('\t'))
                        continue
                    }

                    roles.each { s.increment('fetch contribution from respStatement', "${roleShort(it)} found in cluster") }
                    agentFoundInCluster.println([id, concat(roles), name, otherId, respStatement].join('\t'))

                    found = true
                    break
                }
            }

            if (!found)
                parsedButUnmatched.println([id, concat(roles), name, respStatement].join('\t'))
        }
    }
}

def loadIfLink(Map agent) {
    agent['@id'] ? loadThing(agent['@id']) : agent
}

@Memoized
def loadThing(String id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

@Memoized
def loadDoc(String id) {
    def doc
    selectByIds([id]) { d ->
        doc = d.doc
    }
    return doc
}
