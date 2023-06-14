package datatool.scripts.mergeworks.normalize

import groovy.transform.Memoized
import org.apache.commons.lang3.StringUtils
import whelk.JsonLd

import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

import static datatool.scripts.mergeworks.Util.asList
import static datatool.scripts.mergeworks.Util.name
import static datatool.scripts.mergeworks.Util.normalize
import static datatool.scripts.mergeworks.Util.Relator
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.looksLikeIri

/**
 Example:
 $ ENV=qa && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters="reports/clusters.tsv" -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run src/main/groovy/datatool/scripts/mergeworks/normalize/fetch-contribution-from-respStatement.groovy
 */

linkedFoundInCluster = getReportWriter("linked-agent-found-in-cluster.tsv")
linkedFoundInCluster.println(['id', 'agent name', 'roles', 'matched agent', 'agent occurrences in cluster', 'occurrences by role', 'examples'].join('\t'))

linkedFoundGlobally = getReportWriter("linked-agent-found-globally.tsv")
linkedFoundGlobally.println(['id', 'agent name', 'roles', 'matched agent', 'occurrences by role', 'examples'].join('\t'))

ambiguous = getReportWriter("ambiguous.tsv")
ambiguous.println(['id', 'agent name', 'roles', 'matched agent', 'occurrences by role', 'examples'].join('\t'))

roleFoundInCluster = getReportWriter("role-found-in-cluster.tsv")
roleFoundInCluster.println(['id', 'agent', 'roles', 'added role', 'occurrences in cluster', 'examples'].join('\t'))

roleAddedFromRespStatement = getReportWriter("role-added-from-respStatement.tsv")
roleAddedFromRespStatement.println(['id', 'agent name', 'roles', 'added role', 'occurrences', 'examples', 'resp statement'].join('\t'))

respStatementLinkedAgentFoundInCluster = getReportWriter("respStatement-linked-agent-found-in-cluster.tsv")
respStatementLinkedAgentFoundInCluster.println(['id', 'agent name', 'matched agent', 'resp statement roles', 'roles in cluster', 'new contribution roles', 'agent occurrences in cluster', 'occurrences by role', 'examples', 'resp statement'].join('\t'))

respStatementLinkedAgentFoundGlobally = getReportWriter("respStatement-linked-agent-found-globally.tsv")
respStatementLinkedAgentFoundGlobally.println(['id', 'agent name', 'matched agent', 'resp statement roles', 'occurrences by role', 'examples'].join('\t'))

respStatementLocalAgentFoundInCluster = getReportWriter("respStatement-local-agent-found-in-cluster.tsv")
respStatementLocalAgentFoundInCluster.println(['id', 'agent name', 'existing names', 'resp statement roles', 'roles in cluster', 'new contribution roles', 'agent occurrences in cluster', 'occurrences by role', 'examples', 'resp statement'].join('\t'))

unmatchedContributionsInRespStatement = getReportWriter("unmatched-contributions-in-resp-statement.tsv")
unmatchedContributionsInRespStatement.println(['id', 'agent name', 'existing names', 'roles', 'resp statement'].join('\t'))

vagueNames = getReportWriter("vague-names.tsv")

titleMovedToTranslationOf = getReportWriter("title-moved-to-translationOf.tsv")

originalWorkFoundInCluster = getReportWriter("original-work-found-in-cluster.tsv")
originalWorkFoundInCluster.println(['id', 'added translationOf', 'occurrences in cluster', 'examples'].join('\t'))

def clusters = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }

idToCluster = initIdToCluster(clusters)
nameToAgents = new ConcurrentHashMap<String, ConcurrentHashMap>()
agentToRolesToIds = new ConcurrentHashMap<String, ConcurrentHashMap<Map, ConcurrentHashMap>>()
agentToLifeSpan = new ConcurrentHashMap<String, String>()
linkedAgentToPrefName = new ConcurrentHashMap<String, String>()
idToTranslationOf = new ConcurrentHashMap<String, Object>()

// Populate maps
selectByIds(clusters.flatten()) { bib ->
    def id = bib.doc.shortId
    def work = bib.graph[1].instanceOf

    if (!work || work[ID_KEY]) return

    work.contribution?.each { Map c ->
        asList(c.agent).each { Map agent ->
            def agentStr = toString(agent)
            def loadedAgent = loadIfLink(agent)
            if (agent.containsKey('@id')) {
                agentToLifeSpan.computeIfAbsent(agentStr, f -> lifeSpan(loadedAgent))
                String agentName = name(loadedAgent)
                if (agentName) {
                    linkedAgentToPrefName[agentStr] = agentName
                }
            }
            ([loadedAgent] + asList(loadedAgent.hasVariant)).each { a ->
                String agentName = name(a)
                if (agentName) {
                    nameToAgents.computeIfAbsent(agentName, f -> new ConcurrentHashMap().newKeySet()).add(agentStr)
                }
            }
            def roleToIds = agentToRolesToIds.computeIfAbsent(agentStr, f -> new ConcurrentHashMap())
            asList(c.role).with {
                if (it.isEmpty()) {
                    roleToIds.computeIfAbsent(([:]), f -> new ConcurrentHashMap().newKeySet()).add(id)
                } else {
                    it.each { r ->
                        roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
                    }
                }
            }
        }
    }

    if (work['translationOf']) {
        idToTranslationOf[id] = work['translationOf']
    }
}

agentToNames = initAgentToNames(nameToAgents)

selectByIds(clusters.flatten()) { bib ->
    Map thing = bib.graph[1]
    def id = bib.doc.shortId

    def respStatement = thing.responsibilityStatement
    def work = thing.instanceOf
    def contribution = work?.contribution

    if (!contribution) return

    def normalizedNameToName = [:]
    def contributionsInRespStatement = parseRespStatement(respStatement).collectEntries { name, roles ->
        def normalizedName = normalize(name)
        normalizedNameToName[normalizedName] = name
        [normalizedName, roles]
    }

    def modified = contribution.removeAll { !it.agent }

    contribution.each { Map c ->
        modified |= tryLinkAgent(c, id)
        modified |= tryAddRole(c, id)
        modified |= tryAddRolesFromRespStatement(c, contributionsInRespStatement, respStatement, id)
    }

    modified |= tryAddLinkedAgentContributionsFromRespStatement(contribution, contributionsInRespStatement, respStatement, id)
    modified |= tryAddLocalAgentContributionsFromRespStatement(contribution, contributionsInRespStatement, respStatement, id)
    modified |= addRemainingContributionsFromRespStatement(contribution, contributionsInRespStatement, normalizedNameToName, respStatement, id)

    modified |= tryAddMissingTranslationOf(work, contribution, id)

    if (modified) {
        bib.scheduleSave()
    }
}

def initIdToCluster(List<List<String>> clusters) {
    def idToCluster = [:]
    clusters.each { cluster ->
        cluster.each { id ->
            idToCluster[id] = cluster as Set - id
        }
    }
    return idToCluster
}

static Map<Object, String> initAgentToNames(Map<String, List<Object>> nameToAgents) {
    def agentToNames = [:]
    nameToAgents.each { name, agents ->
        agents.each {
            agentToNames.computeIfAbsent(it, f -> [] as Set).add(name)
        }
    }
    return agentToNames
}

boolean tryLinkAgent(Map contribution, String id) {
    def modified = false

    asList(contribution.agent).each { Map agent ->
        if (!agent.containsKey(ID_KEY)) {
            def names = agentToNames[toString(agent)]
            if (!names) return
            def matchingLinkedAgents = nameToAgents.subMap(names).values().flatten().toSet().findAll { a ->
                JsonLd.looksLikeIri(a) && !yearMismatch(lifeSpan(agent), agentToLifeSpan[a])
            }
            def matchedGlobally = []
            for (agentIri in matchingLinkedAgents) {
                Map roleToIds = agentToRolesToIds[agentIri]
                def inCluster = roleToIds.findAll { _, ids -> idToCluster[id].intersect(ids) }
                if (inCluster) {
                    agent.clear()
                    agent[ID_KEY] = agentIri
                    def occursInIds = inCluster.collect { _, ids -> idToCluster[id].intersect(ids) }.flatten() as Set
                    def ratio = "${occursInIds.size()}/${idToCluster[id].size()}"
                    def examples = occursInIds.take(3)
                    def numContributionsByRole = inCluster.collectEntries { r, ids -> [roleShort(r[ID_KEY]), idToCluster[id].intersect(ids).size()] }
                            .sort { -it.value }
                    def currentRoles = asList(contribution.role).findResults { roleShort(it[ID_KEY]) }.sort()
                    linkedFoundInCluster.println([id, linkedAgentToPrefName[agentIri], currentRoles, idShort(agentIri), ratio, numContributionsByRole, examples].join('\t'))
                    incrementStats('linked agent found in cluster', currentRoles)
                    asList(contribution.role).each { r ->
                        roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
                    }
                    return modified = true
                }

                def matchingRoles = roleToIds.findAll { role, _ -> role in asList(contribution.role) }
                if (matchingRoles) {
                    def numContributionsByRole = roleToIds.collectEntries { r, ids -> [roleShort(r[ID_KEY]), ids.size()] }.sort { -it.value }
                    matchedGlobally.add([agentIri, numContributionsByRole])
                }
            }
            matchedGlobally.each { agentIri, numContributionsByRole ->
                def name = linkedAgentToPrefName[agentIri]
                def roleToIds = agentToRolesToIds[agentIri]
                def examples = roleToIds.values().flatten().toSet().take(3)
                def currentRolesShort = asList(contribution.role).findResults { roleShort(it[ID_KEY]) }.sort()
                if (matchedGlobally.size() == 1) {
                    agent.clear()
                    agent[ID_KEY] = agentIri
                    linkedFoundGlobally.println([id, name, currentRolesShort, idShort(agentIri), numContributionsByRole, examples].join('\t'))
                    incrementStats('linked agent found globally', currentRolesShort, id)
                    asList(contribution.role).each { r ->
                        roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
                    }
                    modified = true
                } else {
                    ambiguous.println([id, name, currentRolesShort, idShort(agentIri), numContributionsByRole, examples].join('\t'))
                }
            }
        }
    }

    return modified
}

boolean tryAddRole(Map contribution, String id) {
    def linkedAgent = asList(contribution.agent).findResult { it[ID_KEY] }
    if (!linkedAgent) return false

    Map roleToIds = agentToRolesToIds[linkedAgent]
    if (!roleToIds) return false

    def currentRoles = asList(contribution.role)
    def foundRolesInCluster = roleToIds.findAll { r, ids ->
        r.containsKey(ID_KEY) && !noRole([r]) && ids.intersect(idToCluster[id])
    }.collect { it.key }
    def newRoles = foundRolesInCluster - currentRoles

    if (newRoles) {
        contribution['role'] = noRole(currentRoles) ? newRoles : currentRoles + newRoles
        def currentRolesShort = currentRoles.findResults { roleShort(it[ID_KEY]) }
        newRoles.each { r ->
            def inClusterWithSameRole = roleToIds[r].intersect(idToCluster[id])
            def shortRole = roleShort(r[ID_KEY])
            def ratio = "${inClusterWithSameRole.size()}/${idToCluster[id].size()}"
            def examples = inClusterWithSameRole.take(3)
            roleFoundInCluster.println([id, idShort(linkedAgent), currentRolesShort, shortRole, ratio, examples].join('\t'))
            incrementStats('role found in cluster', shortRole, id)
            roleToIds[r].add(id)
        }
        return true
    }

    return false
}

boolean tryAddRolesFromRespStatement(Map contribution, Map contributionsInRespStatement, String respStatement, String id) {
    if (contributionsInRespStatement.isEmpty()) return false

    String agent = toString(asList(contribution.agent).find())

    def match = contributionsInRespStatement.subMap(agentToNames[agent] ?: []).find()
    if (!match) return false

    String name = match.key
    List<Relator> rolesInRespStatement = match.value
    contributionsInRespStatement.remove(name)

    Map roleToIds = agentToRolesToIds[agent]
    if (!roleToIds) return false

    def currentRoles = asList(contribution.role)
    def isPrimaryContribution = contribution[ID_KEY] == 'PrimaryContribution'
    def rolesOfInterest = rolesInRespStatement.findResults { Relator relator ->
        relator != Relator.UNSPECIFIED_CONTRIBUTOR
                && !(relator == Relator.IMPLICIT_AUTHOR && !isPrimaryContribution)
                ? [(ID_KEY): relator.iri]
                : null
    }
    def newRoles = rolesOfInterest - currentRoles
    if (newRoles) {
        contribution['role'] = noRole(currentRoles) ? newRoles : currentRoles + newRoles
        def currentRolesShort = currentRoles.findResults { roleShort(it[ID_KEY]) }
        newRoles.each { r ->
            def globalOccurences = roleToIds[r]?.size() ?: 0
            def shortRole = roleShort(r[ID_KEY])
            def examples = roleToIds[r]?.take(3) ?: []
            roleAddedFromRespStatement.println([id, name, currentRolesShort, shortRole, globalOccurences, examples, respStatement].join('\t'))
            incrementStats("role added from responsibilityStatement", shortRole, id)
            roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
        }
        return true
    }

    return false
}

boolean tryAddLinkedAgentContributionsFromRespStatement(List<Map> contribution, Map contributionsInRespStatement, String respStatement, String id) {
    if (contributionsInRespStatement.isEmpty()) return false

    return contributionsInRespStatement.removeAll { String name, List<Relator> roles ->
        def agents = nameToAgents[name]
        if (!agents) return false

        def linkedAgents = agents.findAll { looksLikeIri(it) }
        def respStatementRoles = roles.findResults { r -> r == Relator.UNSPECIFIED_CONTRIBUTOR ? null : [(ID_KEY): r.iri] }

        def matchedGlobally = []

        for (agentIri in linkedAgents) {
            Map roleToIds = agentToRolesToIds[agentIri]
            def inCluster = roleToIds.findAll { _, ids ->
                idToCluster[id].intersect(ids)
            }
            if (inCluster) {
                def currentRoles = inCluster.findResults { r, _ -> noRole([r]) ? null : r }
                def newContribution =
                        [
                                '@type': 'Contribution',
                                'agent': [(ID_KEY): agentIri]
                        ]
                (currentRoles + respStatementRoles).unique().with {
                    if (!it.isEmpty()) {
                        newContribution['role'] = it
                    }
                }
                contribution.add(newContribution)

                def respStatementRolesShort = roles.collect { r -> roleShort(r.iri) }
                def currentRolesShort = inCluster.collect { r, _ -> roleShort(r[ID_KEY]) }
                def newContributionRolesShort = newContribution['role'].collect { roleShort(it[ID_KEY]) }.sort()
                def occursInIds = inCluster.collect { _, ids -> idToCluster[id].intersect(ids) }.flatten() as Set
                def ratio = "${occursInIds.size()}/${idToCluster[id].size()}"
                def examples = occursInIds.take(3)
                def numContributionsByRole = inCluster.collectEntries { r, ids -> [roleShort(r[ID_KEY]), idToCluster[id].intersect(ids).size()] }
                        .sort { -it.value }
                respStatementLinkedAgentFoundInCluster.println([id, name, idShort(agentIri), respStatementRolesShort, currentRolesShort, newContributionRolesShort, ratio, numContributionsByRole, examples, respStatement].join('\t'))
                incrementStats('linked agents from respStatement (found in cluster)', newContributionRolesShort, id)
                newContribution['role'].each { r ->
                    roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
                }
                return true
            }
            def matchingRoles = respStatementRoles.intersect(roleToIds.keySet())
            if (matchingRoles) {
                def numContributionsByRole = roleToIds.collectEntries { r, ids -> [roleShort(r[ID_KEY]), ids.size()] }.sort { -it.value }
                matchedGlobally.add([agentIri, numContributionsByRole])
            }
        }

        def unambiguousMatch = false

        matchedGlobally.each { agentIri, numContributionsByRole ->
            def roleToIds = agentToRolesToIds[agentIri]
            def examples = roleToIds.findAll { r, _ -> r in respStatementRoles }.values().flatten().take(3)
            def respStatementRolesShort = respStatementRoles.collect { roleShort(it[ID_KEY]) }
            if (matchedGlobally.size() == 1) {
                def newContribution =
                        [
                                '@type': 'Contribution',
                                'agent': [(ID_KEY): agentIri],
                                'role' : respStatementRoles
                        ]
                contribution.add(newContribution)
                respStatementLinkedAgentFoundGlobally.println([id, name, idShort(agentIri), respStatementRolesShort, numContributionsByRole, examples].join('\t'))
                incrementStats('linked agents from respStatement (found globally)', respStatementRolesShort, id)
                newContribution['role'].each { r ->
                    roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
                }
                unambiguousMatch = true
            } else {
                ambiguous.println([id, name, respStatementRolesShort, idShort(agentIri), numContributionsByRole, examples].join('\t'))
            }
        }

        return unambiguousMatch
    }
}

boolean tryAddLocalAgentContributionsFromRespStatement(List<Map> contribution, Map contributionsInRespStatement, String respStatement, String id) {
    if (contributionsInRespStatement.isEmpty()) return false

    def existingNames = contribution.findResults {
        def agent = asList(it.agent).find()
        return agent[ID_KEY] ? linkedAgentToPrefName[toString(agent)] : agentToNames[toString(agent)]?.find()
    }

    return contributionsInRespStatement.removeAll { String name, List<Relator> roles ->
        def agents = nameToAgents[name]
        if (!agents) return false

        def localAgents = agents.findAll { !looksLikeIri(it) }
        def respStatementRoles = roles.findResults { r -> r == Relator.UNSPECIFIED_CONTRIBUTOR ? null : [(ID_KEY): r.iri] }

        for (localAgent in localAgents) {
            Map roleToIds = agentToRolesToIds[localAgent]
            def inCluster = roleToIds.findAll { _, ids ->
                idToCluster[id].intersect(ids)
            }
            if (inCluster) {
                def currentRoles = inCluster.findResults { r, _ -> noRole([r]) ? null : r }
                def newContribution =
                        [
                                '@type': 'Contribution',
                                'agent': toMap(localAgent)
                        ]
                (currentRoles + respStatementRoles).unique().with {
                    if (!it.isEmpty()) {
                        newContribution['role'] = it
                    }
                }
                contribution.add(newContribution)

                def respStatementRolesShort = roles.collect { r -> roleShort(r.iri) }
                def currentRolesShort = inCluster.collect { r, _ -> roleShort(r[ID_KEY]) }
                def newContributionRolesShort = newContribution['role'].collect { roleShort(it[ID_KEY]) }.sort()
                def occursInIds = inCluster.collect { _, ids -> idToCluster[id].intersect(ids) }.flatten() as Set
                def ratio = "${occursInIds.size()}/${idToCluster[id].size()}"
                def examples = occursInIds.take(3)
                def numContributionsByRole = inCluster.collectEntries { r, ids -> [roleShort(r[ID_KEY]), idToCluster[id].intersect(ids).size()] }
                        .sort { -it.value }
                respStatementLocalAgentFoundInCluster.println([id, name, existingNames, respStatementRolesShort, currentRolesShort, newContributionRolesShort, ratio, numContributionsByRole, examples, respStatement].join('\t'))
                incrementStats('local agents from respStatement (found in cluster)', newContributionRolesShort, id)
                newContribution['role'].each { r ->
                    roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
                }
                return true
            }
        }

        return false
    }
}

boolean addRemainingContributionsFromRespStatement(List<Map> contribution, Map contributionsInRespStatement, Map normalizedNames, String respStatement, String id) {
    if (contributionsInRespStatement.isEmpty()) return false

    def existingNames = contribution.findResults {
        def agent = asList(it.agent).find()
        return agent[ID_KEY] ? linkedAgentToPrefName[toString(agent)] : agentToNames[toString(agent)]?.find()
    }

    contributionsInRespStatement.each { name, roles ->
        def newContribution =
                [
                        '@type': 'Contribution',
                        'agent': ['@type': 'Person', 'name': normalizedNames[name]]
                ]
        roles = roles.findResults { r -> r == Relator.UNSPECIFIED_CONTRIBUTOR ? null : [(ID_KEY): r.iri] }
        if (roles) {
            newContribution['role'] = roles
        }
        contribution.add(newContribution)
        unmatchedContributionsInRespStatement.println([id, normalizedNames[name], existingNames, roles.collect { roleShort(it[ID_KEY]) }, respStatement].join('\t'))
        def roleToIds = agentToRolesToIds.computeIfAbsent(toString(newContribution.agent), f -> new ConcurrentHashMap())
        asList(newContribution.role).each { r ->
            roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
        }
    }

    return true
}

boolean tryAddMissingTranslationOf(Map work, List<Map> contribution, String id) {
    def trl = [(ID_KEY): Relator.TRANSLATOR.iri]
    def translators = contribution.findResults { asList(it.role).contains(trl) ? toString(asList(it.agent).find()) : null }

    if (!translators || work['translationOf']) return false

    def title = work.remove('hasTitle')
    if (title) {
        work['translationOf'] = ['@type': 'Work', 'hasTitle': title]
        incrementStats('add missing translationOf', "title moved to new translationOf", id)
        titleMovedToTranslationOf.println([id, work['translationOf']].join('\t'))
        return true
    }

    for (String translator : translators) {
        def roleToIds = agentToRolesToIds[translator]
        def inClusterSameTranslator = roleToIds[trl].intersect(idToCluster[id])
        def origWorks = inClusterSameTranslator.findResults { idToTranslationOf[it] }

        if (origWorks) {
            work['translationOf'] = origWorks.countBy { it }.max { it.value }?.key
            def ratio = "${origWorks.size()}/${idToCluster[id].size()}"
            def examples = inClusterSameTranslator.findAll { idToTranslationOf.containsKey(it) }.take(3)
            incrementStats('add missing translationOf', 'original work found in cluster (same translator)', id)
            originalWorkFoundInCluster.println([id, work['translationOf'], ratio, examples].join('\t'))
            return true
        }
    }

    return false
}

boolean noRole(List<Map> roles) {
    roles == [[:]] || roles == [[(ID_KEY): Relator.UNSPECIFIED_CONTRIBUTOR.iri]]
}

private Map loadIfLink(Map m) {
    m[ID_KEY] ? loadThing(m[ID_KEY]) : m
}

@Memoized
private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

Map<String, List<Relator>> parseRespStatement(String respStatement) {
    def parsedContributions = [:]

    if (respStatement) {
        respStatement.split(';').eachWithIndex { part, i ->
            parseSwedishFictionContribution(StringUtils.normalizeSpace(part), i == 0).each { name, roles ->
                parsedContributions
                        .computeIfAbsent(name, r -> [])
                        .addAll(roles)
            }
        }
    }

    return parsedContributions.findAll { name, roles ->
        if (name =~ /\s/) {
            return true
        }
        vagueNames.println([name, respStatement].join('\t'))
        return false
    }
}

static Map<String, List<Relator>> parseSwedishFictionContribution(String contribution, boolean isFirstStmtPart) {
    def roleToPattern =
            [
                    (Relator.TRANSLATOR)         : ~/(bemynd(\w+|\.)? )?öf?v(\.|ers(\.|\p{L}+)?)( (till|från) \p{L}+)?|(till svenskan?|från \p{L}+)|svensk text/,
                    (Relator.AUTHOR)             : ~/^(text(e[nr])?|skriven|written)/,
                    (Relator.ILLUSTRATOR)        : ~/\bbild(er)?|ill(\.|ustr(\.|\w+)?)|\bvi(gn|nj)ett(er|ill)?|ritad/,
                    (Relator.AUTHOR_OF_INTRO)    : ~/förord|inl(edn(\.|ing)|edd)/,
                    (Relator.COVER_DESIGNER)     : ~/omslag/,
                    (Relator.AUTHOR_OF_AFTERWORD): ~/efter(ord|skrift)/,
                    (Relator.PHOTOGRAPHER)       : ~/\bfoto\w*\.?/,
                    (Relator.EDITOR)             : ~/red(\.(?! av)|aktör(er)?)|\bbearb(\.|\w+)?|återberättad|sammanställ\w*/,
            ]

    def rolePattern = ~/((?iu)${roleToPattern.values().join('|')})/
    def followsRolePattern = ~/(:| a[fv]| by) /
    def initialPattern = ~/\p{Lu}/
    def namePattern = ~/\p{Lu}:?\p{Ll}+('\p{Ll})?(,? [Jj](r|unior))?/
    def betweenNamesPattern = ~/-| |\. ?| (de(l| la)?|von|van( de[nr])?|v\.|le|af|du|dos) | [ODdLl]'/
    def fullNamePattern = ~/(($initialPattern|$namePattern)($betweenNamesPattern)?)*$namePattern/
    def conjPattern = ~/ (och|&|and) /
    def roleAfterNamePattern = ~/( ?\(($rolePattern$conjPattern)?$rolePattern\))/
    def fullContributionPattern = ~/(($rolePattern($conjPattern|\/))*$rolePattern$followsRolePattern)?$fullNamePattern($conjPattern$fullNamePattern)*$roleAfterNamePattern?/

    // Make roles lower case so that they can't be mistaken for names
    contribution = (contribution =~ rolePattern)*.first()
            .collectEntries { [it, it.toLowerCase()] }
            .with { contribution.replace(it) }

    def nameToRoles = [:]

    def matched = (contribution =~ fullContributionPattern)*.first()

    matched.each { m ->
        // Extract roles from the contribution
        def roles = roleToPattern
                .findAll { _, pattern -> m =~ /(?iu)$pattern/ }
                .with {
                    it.isEmpty() && contribution =~ /.+$followsRolePattern/
                            ? [Relator.UNSPECIFIED_CONTRIBUTOR]
                            : it.collect { role, _ -> role }
                }

        // Author should be the role if first part of respStatement (before ';') and no role seems to be stated
        if (roles.isEmpty() && isFirstStmtPart) {
            roles << Relator.IMPLICIT_AUTHOR
        }

        // Extract names from the contribution
        def names = parseNames(fullNamePattern, conjPattern, m)

        // Assign the roles to each name
        nameToRoles.putAll(names.collectEntries { [it, roles] })
    }

    return nameToRoles
}

static List<String> parseNames(Pattern namePattern, Pattern conjPattern, String s) {
    def names = []

    (s =~ namePattern).each {
        def name = it.first()
        // Handle the case of "Jan och Maria Larsson"
        def previousName = names.isEmpty() ? null : names.last()
        if (previousName?.split()?.size() == 1 && s =~ /$previousName$conjPattern$name/) {
            def nameParts = name.split()
            if (nameParts.size() > 1) {
                names[-1] += " ${nameParts.last()}"
            }
        }
        names << name
    }

    return names
}

@Memoized
def getWhelk() {
    // A little hack to get a handle to whelk...
    def whelk = null
    selectByIds(['https://id.kb.se/marc']) { docItem ->
        whelk = docItem.whelk
    }
    if (!whelk) {
        throw new RuntimeException("Could not get Whelk")
    }
    return whelk
}

static boolean yearMismatch(String a, String b) {
    a && b && a != b
}

static String lifeSpan(Map agent) {
    agent.lifeSpan?.replaceAll(~/[^\-0-9]/, '')?.replaceAll(~/-+/, '-')
}

static String toString(Map agent) {
    agent[ID_KEY] ?: new JsonBuilder(agent).toString()
}

static toMap(String agent) {
    new JsonSlurper().parseText(agent)
}

static String idShort(String iri) {
    iri.split("[#/]").dropRight(1).last()
}

static String roleShort(String iri) {
    iri?.split("/")?.last() ?: 'NO ROLE'
}

