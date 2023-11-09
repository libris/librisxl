import groovy.transform.Memoized
import org.apache.commons.lang3.StringUtils

import whelk.Document

import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

import static se.kb.libris.mergeworks.Util.name
import static se.kb.libris.mergeworks.Util.normalize
import static se.kb.libris.mergeworks.Util.Relator
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.looksLikeIri

linkedFoundInCluster = getReportWriter("linked-agent-found-in-cluster.tsv")
linkedFoundInCluster.println(['id', 'matched agent', 'agent occurs in (examples)'].join('\t'))

roleAddedFromRespStatement = getReportWriter("role-added-from-respStatement.tsv")
roleAddedFromRespStatement.println(['id', 'agent name', 'added roles', 'resp statement'].join('\t'))

lifeSpanFoundInCluster = getReportWriter("life-span-found-in-cluster.tsv")
lifeSpanFoundInCluster.println(['id', 'agent name', 'lifeSpan', 'agent occurs with lifeSpan in (examples)'].join('\t'))

respStatementLinkedAgentFoundInCluster = getReportWriter("respStatement-linked-agent-found-in-cluster.tsv")
respStatementLinkedAgentFoundInCluster.println(['id', 'agent name', 'matched agent', 'resp statement roles', 'agent occurs in (examples)', 'resp statement'].join('\t'))

respStatementLocalAgentFoundInCluster = getReportWriter("respStatement-local-agent-found-in-cluster.tsv")
respStatementLocalAgentFoundInCluster.println(['id', 'agent name', 'resp statement roles', 'agent occurs in (examples)', 'resp statement'].join('\t'))

unmatchedContributionsInRespStatement = getReportWriter("unmatched-contributions-in-resp-statement.tsv")
unmatchedContributionsInRespStatement.println(['id', 'agent name', 'roles', 'resp statement'].join('\t'))

roleFoundInCluster = getReportWriter("role-found-in-cluster.tsv")
roleFoundInCluster.println(['id', 'agent', 'added role', 'agent occurs with role in (examples)'].join('\t'))

titleMovedToTranslationOf = getReportWriter("title-moved-to-translationOf.tsv")

originalWorkFoundInCluster = getReportWriter("original-work-found-in-cluster.tsv")
originalWorkFoundInCluster.println(['id', 'added translationOf', 'translationOf occurs in (examples)'].join('\t'))

def clusters = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }

idToCluster = initIdToCluster(clusters)
nameToAgents = new ConcurrentHashMap<String, ConcurrentHashMap>()
agentToRolesToIds = new ConcurrentHashMap<String, ConcurrentHashMap<Map, ConcurrentHashMap>>()
linkedAgentToLifeSpan = new ConcurrentHashMap<String, String>()
localAgentToLifeSpansToIds = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap>>()
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
            if (agent.lifeSpan) {
                if (agent.containsKey(ID_KEY)) {
                    linkedAgentToLifeSpan.computeIfAbsent(agentStr, f -> lifeSpan(loadedAgent))
                } else {
                    def lifeSpansToIds = localAgentToLifeSpansToIds.computeIfAbsent(agentStr, f -> new ConcurrentHashMap())
                    lifeSpansToIds.computeIfAbsent(agent.lifeSpan, f -> new ConcurrentHashMap().newKeySet()).add(id)
                }
            }
            ([loadedAgent] + asList(loadedAgent.hasVariant)).each { a ->
                String agentName = name(a)
                if (agentName) {
                    nameToAgents.computeIfAbsent(agentName, f -> new ConcurrentHashMap().newKeySet()).add(agentStr)
                    def acronym = agentName.split(' ').findAll().with { parts ->
                        (0..<parts.size() - 1).each {
                            parts[it] = parts[it][0]
                        }
                        parts.join(' ')
                    }
                    nameToAgents.computeIfAbsent(acronym, f -> new ConcurrentHashMap().newKeySet()).add(agentStr)
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

    if (!work || work[ID_KEY]) return

    def contribution = work.contribution

    if (!contribution) return

    // extract names + roles from responsibilityStatement
    // normalize the names for comparison but also save the original strings for later use
    def normalizedNameToName = [:]
    def contributionsInRespStatement = parseRespStatement(respStatement).collectEntries { name, roles ->
        def normalizedName = normalize(name)
        normalizedNameToName[normalizedName] = name
        [normalizedName, roles]
    }

    // remove useless contributions
    def modified = contribution.removeAll { !it.agent }

    contribution.each { Map c ->
        // match local agent against linked ones in same cluster
        modified |= tryLinkAgent(c, id)
        // if there are more roles stated in responsibilityStatement other than the existing ones in this contribution, add those
        modified |= tryAddRolesFromRespStatement(c, contributionsInRespStatement, respStatement, id)
        modified |= tryAddLifeSpanToLocalAgent(c, id)
    }

    // drop "implicit authors", e.g. Astrid Lindgren in "Astrid Lindgren ; illustrerad av Ilon Wikland" (likely to already exist)
    contributionsInRespStatement.removeAll { _, roles -> roles == [Relator.IMPLICIT_AUTHOR] }

    // agents in responsibilityStatement that are not in contribution? match against linked agents in same cluster
    modified |= tryAddLinkedAgentContributionsFromRespStatement(contribution, contributionsInRespStatement, respStatement, id)

    // drop unmatched agents that are likely to already exist (agent with same initials exists or contribution with same role exists)
    def existingNames = contribution.findResults { agentToNames[toString(asList(it.agent).find())] }.flatten()
    contributionsInRespStatement.removeAll { String name, List<Relator> roles ->
        existingNames.any { similarName(it, name) }
                || roles.collect { toIdMap(it.iri) }.intersect(contribution.collect { it.role }.flatten())
    }

    // match remaining against local agents in same cluster
    modified |= tryAddLocalAgentContributionsFromRespStatement(contribution, contributionsInRespStatement, respStatement, id)
    // if still no match, add constructed local Contribution with agent + roles extracted from responsibilityStatement
    modified |= addRemainingContributionsFromRespStatement(contribution, contributionsInRespStatement, normalizedNameToName, respStatement, id)

    if (modified) {
        bib.scheduleSave()
    }
}

selectByIds(clusters.flatten()) { bib ->
    def id = bib.doc.shortId
    def work = bib.graph[1].instanceOf
    def contribution = work?.contribution

    if (!contribution) return

    def modified = false

    contribution.each { Map c ->
        // add roles from contributions in same cluster with matching agent
        modified |= tryAddRole(c, id)
    }

    // works with translators should have translationOf, add if missing
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
            // get agent name variants
            def names = agentToNames[toString(agent)]
            if (!names) return
            // get linked agents with matching name
            def matchingLinkedAgents = nameToAgents.subMap(names).values().flatten().toSet().findAll { a ->
                looksLikeIri(a) && !yearMismatch(lifeSpan(agent), linkedAgentToLifeSpan[a])
            }
            for (agentIri in matchingLinkedAgents) {
                // roles that the linked agent appears as and in which records respectively
                Map roleToIds = agentToRolesToIds[agentIri]
                // records in same cluster where the linked agent appears
                def inClusterWithAgent = roleToIds.findResults { _, ids -> idToCluster[id].intersect(ids) }.flatten() as Set
                if (inClusterWithAgent) {
                    // matching linked agent appears in same cluster -> add link
                    agent.clear()
                    agent[ID_KEY] = agentIri
                    // report
                    def examples = inClusterWithAgent.take(3)
                    def currentRoles = asList(contribution.role).findResults { roleShort(it[ID_KEY]) }.sort()
                    linkedFoundInCluster.println([id, idShort(agentIri), examples].join('\t'))
                    incrementStats('linked agent found in cluster', currentRoles)
                    // add this id to "records that the agent appears in" for each role
                    asList(contribution.role).each { r ->
                        roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
                    }
                    return modified = true
                }
            }
        }
    }

    return modified
}

boolean tryAddRolesFromRespStatement(Map contribution, Map contributionsInRespStatement, String respStatement, String id) {
    if (contributionsInRespStatement.isEmpty()) return false

    String agent = toString(asList(contribution.agent).find())

    // any matching agent (name) in responsibilityStatement?
    def matching = contributionsInRespStatement.subMap(agentToNames[agent] ?: [])
    if (!matching) return false

    // matched and will be handled, remove
    matching.each { name, _ -> contributionsInRespStatement.remove(name) }

    def firstMatch = matching.find()
    String name = firstMatch.key
    List<Relator> rolesInRespStatement = firstMatch.value

    Map roleToIds = agentToRolesToIds[agent]
    if (!roleToIds) return false

    def currentRoles = asList(contribution.role)
    def isPrimaryContribution = contribution[ID_KEY] == 'PrimaryContribution'
    // author role needs to be explicitly stated in responsibilityStatement to be added to "regular" Contribution
    def rolesOfInterest = rolesInRespStatement.findResults { Relator relator ->
        relator == Relator.IMPLICIT_AUTHOR && !isPrimaryContribution
                ? null
                : toIdMap(relator.iri)
    }

    def modified = false
    def newRoles = rolesOfInterest - currentRoles
    if (newRoles) {
        // add new roles (replace existing unspecifiedContributor)
        contribution['role'] = noRole(currentRoles) ? newRoles : currentRoles + newRoles
        // report
        def newRolesShort = newRoles.findResults { roleShort(it[ID_KEY]) }
        roleAddedFromRespStatement.println([id, name, newRolesShort, respStatement].join('\t'))
        incrementStats("roles added from responsibilityStatement", newRolesShort.sort(), id)
        // add this id to "records that the agent appears in" for each added role
        newRoles.each { r ->
            roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
        }
        modified = true
    }

    return modified
}

boolean tryAddLifeSpanToLocalAgent(Map contribution, String id) {
    def agent = asList(contribution.agent).find()
    if (agent instanceof Map && !agent[ID_KEY] && !agent.lifeSpan) {
        def names = agentToNames[toString(agent)]
        if (!names) return
        def matchingLocalAgentsWithLifeSpan = nameToAgents.subMap(names).values().flatten().toSet().findAll { a ->
            !looksLikeIri(a) && localAgentToLifeSpansToIds[a]
        }
        for (localAgent in matchingLocalAgentsWithLifeSpan) {
            def lifeSpanToIds = localAgentToLifeSpansToIds[localAgent]
            def lifeSpanInCluster = lifeSpanToIds.find { _, ids -> idToCluster[id].intersect(ids) }?.key
            if (lifeSpanInCluster) {
                agent['lifeSpan'] = lifeSpanInCluster
                def examples = idToCluster[id].intersect(lifeSpanToIds[lifeSpanInCluster]).take(3)
                lifeSpanFoundInCluster.println([id, name(agent), lifeSpanInCluster, examples].join('\t'))
                return true
            }
        }
    }
    return false
}

boolean tryAddLinkedAgentContributionsFromRespStatement(List<Map> contribution, Map contributionsInRespStatement, String respStatement, String id) {
    if (contributionsInRespStatement.isEmpty()) return false

    return contributionsInRespStatement.removeAll { String name, List<Relator> roles ->
        // get agents with matching name
        def agents = nameToAgents[name]
        if (!agents) return false

        // get only linked agents
        def linkedAgents = agents.findAll { looksLikeIri(it) }

        for (agentIri in linkedAgents) {
            Map roleToIds = agentToRolesToIds[agentIri]
            def inClusterWithAgent = roleToIds.findResults { _, ids -> idToCluster[id].intersect(ids) }.flatten() as Set
            if (inClusterWithAgent) {
                def newContribution =
                        [
                                '@type': 'Contribution',
                                'agent': toIdMap(agentIri)
                        ]

                roles = roles.collect { r -> toIdMap(r.iri) }

                if (roles) {
                    newContribution['role'] = roles
                }

                if (!contribution.contains(newContribution)) {
                    contribution.add(newContribution)
                }

                def rolesShort = roles.collect { r -> roleShort(r[ID_KEY]) }.sort()
                def examples = inClusterWithAgent.take(3)
                respStatementLinkedAgentFoundInCluster.println([id, name, idShort(agentIri), rolesShort, examples, respStatement].join('\t'))
                incrementStats('linked agents from respStatement (found in cluster)', rolesShort, id)

                roles.each { r ->
                    roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
                }

                return true
            }
        }

        return false
    }
}

boolean tryAddLocalAgentContributionsFromRespStatement(List<Map> contribution, Map contributionsInRespStatement, String respStatement, String id) {
    if (contributionsInRespStatement.isEmpty()) return false

    return contributionsInRespStatement.removeAll { String name, List<Relator> roles ->
        def agents = nameToAgents[name]
        if (!agents) return false

        def localAgents = agents.findAll { !looksLikeIri(it) }

        for (localAgent in localAgents) {
            Map roleToIds = agentToRolesToIds[localAgent]
            def inClusterWithAgent = roleToIds.findResults { _, ids -> idToCluster[id].intersect(ids) }.flatten() as Set
            if (inClusterWithAgent) {
                def newContribution =
                        [
                                '@type': 'Contribution',
                                'agent': toMap(localAgent)
                        ]

                roles = roles.collect { r -> toIdMap(r.iri) }

                if (roles) {
                    newContribution['role'] = roles
                }

                contribution.add(newContribution)

                def rolesShort = roles.collect { r -> roleShort(r[ID_KEY]) }
                def examples = inClusterWithAgent.take(3)
                respStatementLocalAgentFoundInCluster.println([id, name, rolesShort, examples, respStatement].join('\t'))
                incrementStats('local agents from respStatement (found in cluster)', rolesShort, id)

                roles.each { r ->
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

    return contributionsInRespStatement.removeAll { name, roles ->
        def translatorEditor = roles.findResults { r -> r == Relator.TRANSLATOR || r == Relator.EDITOR ? toIdMap(r.iri) : null }

        if (translatorEditor) {
            def newContribution =
                    [
                            '@type': 'Contribution',
                            'agent': ['name': normalizedNames[name], '@type': 'Person'],
                            'role' : translatorEditor
                    ]

            contribution.add(newContribution)

            def rolesShort = translatorEditor.collect { roleShort(it[ID_KEY]) }.sort()
            unmatchedContributionsInRespStatement.println([id, normalizedNames[name], rolesShort, respStatement].join('\t'))
            incrementStats('unmatched agents in respStatement', rolesShort, id)

            def roleToIds = agentToRolesToIds.computeIfAbsent(toString(newContribution.agent), f -> new ConcurrentHashMap())
            translatorEditor.each { r ->
                roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
            }

            return true
        }
    }
}


boolean tryAddRole(Map contribution, String id) {
    def agent = asList(contribution.agent).find()
    def agentStr = toString(agent)

    Map roleToIds = agentToRolesToIds[agentStr]
    if (!roleToIds) return false

    def adapterEditor = [Relator.EDITOR, Relator.ADAPTER].collect { toIdMap(it.iri) }

    def currentRoles = asList(contribution.role)
    // find roles in cluster that can be added (certain conditions need to be met)
    def rolesInCluster = roleToIds.findAll { r, ids ->
        def inCluster = idToCluster[id]
        def inClusterWithRole = ids.intersect(idToCluster[id])
        return inClusterWithRole
                && !noRole([r])
                && (inClusterWithRole.size() >= inCluster.size() / 2
                || noRole(currentRoles)
                || r == toIdMap(Relator.PRIMARY_RIGHTS_HOLDER.iri)
                || (r in adapterEditor && currentRoles.intersect(adapterEditor)))
    }.collect { it.key }
    def newRoles = rolesInCluster - currentRoles
    if (newRoles) {
        contribution['role'] = noRole(currentRoles) ? newRoles : currentRoles + newRoles
        newRoles.each { r ->
            def shortRole = roleShort(r[ID_KEY])
            def examples = roleToIds[r].intersect(idToCluster[id]).take(3)
            def agentShort = agent[ID_KEY] ? idShort(agentStr) : agentToNames[agentStr]?.getAt(0)
            roleFoundInCluster.println([id, agentShort, shortRole, examples].join('\t'))
            incrementStats('role found in cluster', shortRole, id)
            roleToIds[r].add(id)
        }
        return true
    }

    return false
}

boolean tryAddMissingTranslationOf(Map work, List<Map> contribution, String id) {
    def trl = toIdMap(Relator.TRANSLATOR.iri)
    def translators = contribution.findResults { asList(it.role).contains(trl) ? toString(asList(it.agent).find()) : null }

    if (!translators || work['translationOf']) return false

    def title = work.remove('hasTitle')
    if (title) {
        // the title should be in translationOf, construct a new local work and put the title there
        work['translationOf'] = ['@type': 'Work', 'hasTitle': title]
        incrementStats('add missing translationOf', "title moved to new translationOf", id)
        titleMovedToTranslationOf.println([id, work['translationOf']].join('\t'))
        return true
    }

    for (String translator : translators) {
        def roleToIds = agentToRolesToIds[translator]

        if (!roleToIds) continue

        def inClusterSameTranslator = roleToIds[trl].intersect(idToCluster[id])
        def origWorks = inClusterSameTranslator.findResults { idToTranslationOf[it] }

        if (origWorks) {
            // translationOf found on other work in cluster with matching translator, add to this work (pick the most common if several)
            work['translationOf'] = origWorks.countBy { it }.max { it.value }?.key
            def examples = inClusterSameTranslator.findAll { idToTranslationOf.containsKey(it) }.take(3)
            incrementStats('add missing translationOf', 'original work found in cluster (same translator)', id)
            originalWorkFoundInCluster.println([id, work['translationOf'], examples].join('\t'))
            return true
        }
    }

    return false
}

boolean noRole(List<Map> roles) {
    roles.isEmpty() || roles == [[:]] || roles == [toIdMap(Relator.UNSPECIFIED_CONTRIBUTOR.iri)]
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

    return parsedContributions.findAll { name, _ -> name =~ /\s/ }
}

static Map<String, List<Relator>> parseSwedishFictionContribution(String contribution, boolean isFirstStmtPart) {
    def roleToPattern =
            [
                    (Relator.TRANSLATOR)         : ~/(bemynd(\w+|\.)? )?öf?v(\.|ers(\.|\p{L}+)?)( (till|från) \p{L}+)?|(till svenskan?|från \p{L}+)|svensk text/,
                    (Relator.AUTHOR)             : ~/^(text(e[nr])?|skriven|written)/,
                    (Relator.ILLUSTRATOR)        : ~/\bbild(er)?|ill(\.|ustr(\.|\w+)?)|\bvi(gn|nj)ett(er|ill)?|ritad|\bteckn(ad|ingar)/,
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
    def betweenNamesPattern = ~/-| |\. ?| ([Dd]e(l| la)?|von|van( de[nr])?|v\.|le|af|du|dos) | [ODdLl]'/
    def fullNamePattern = ~/(($initialPattern|$namePattern)($betweenNamesPattern)?)*$namePattern/
    def conjPattern = ~/(,| och| &| and) /
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
        def roles = roleToPattern.findResults { role, pattern -> m =~ /(?iu)$pattern/ ? role : null }

        // Author should be the role if first part of respStatement (before ';') and no role seems to be stated
        if (roles.isEmpty() && isFirstStmtPart && !(contribution =~ /.+$followsRolePattern/)) {
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
    agent[ID_KEY]?.replaceFirst(".+/", Document.BASE_URI.toString()) ?: new JsonBuilder(agent).toString()
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

static boolean similarName(String a, String b) {
    [nameParts(a), nameParts(b)].with { n1, n2 ->
        n1.size() == 1 || n2.size() == 1
                ? n1.intersect(n2)
                : [initials(n1), initials(n2)].with { i1, i2 -> i1.containsAll(i2) || i2.containsAll(i1) }
    }
}

static List<Character> initials(List nameParts) {
    nameParts.collect { it[0] }
}

static List<String> nameParts(String s) {
    s.split(' ').findAll()
}

def toIdMap(String iri) {
    [(ID_KEY): iri]
}