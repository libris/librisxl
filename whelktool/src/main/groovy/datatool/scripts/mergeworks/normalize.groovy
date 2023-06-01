package datatool.scripts.mergeworks

import groovy.transform.Memoized
import org.apache.commons.lang3.StringUtils
import whelk.JsonLd

import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

import static datatool.scripts.mergeworks.Util.asList
import static datatool.scripts.mergeworks.Util.chipString
import static datatool.scripts.mergeworks.Util.name
import static datatool.scripts.mergeworks.Util.normalize
import static datatool.scripts.mergeworks.Util.Relator
import static whelk.JsonLd.ID_KEY

/**
 Example:
 $ ENV=qa && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters="reports/clusters.tsv" -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run src/main/groovy/datatool/scripts/mergeworks/normalize/fetch-contribution-from-respStatement.groovy
 */

linkedFoundInCluster = getReportWriter("linked-agent-found-in-cluster.tsv")
linkedFoundGlobally = getReportWriter("linked-agent-found-globally.tsv")
roleFoundInCluster = getReportWriter("role-found-in-cluster.tsv")
roleFoundGlobally = getReportWriter("role-found-globally.tsv")
roleAddedFromRespStatement = getReportWriter("role-added-from-respStatement.tsv")

def clusters = new File(System.getProperty('clusters')).collect { it.split('\t').collect { it.trim() } }

idToCluster = initIdToCluster(clusters)
nameToAgents = new ConcurrentHashMap<String, ConcurrentHashMap>()
agentToRolesToIds = new ConcurrentHashMap<String, ConcurrentHashMap<Map, ConcurrentHashMap>>()

// Populate maps
selectByIds(clusters.flatten()) { bib ->
    def id = bib.doc.shortId

    bib.graph[1].instanceOf?.contribution?.each { Map c ->
        asList(c.agent).each { Map agent ->
            ([agent] + asList(agent.hasVariant)).each {
                String agentName = name(loadIfLink(it))
                if (agentName) {
                    def a = toString(agent)
                    nameToAgents.computeIfAbsent(agentName, f -> new ConcurrentHashMap().newKeySet()).add(a)
                    def roleToIds = agentToRolesToIds.computeIfAbsent(a, f -> new ConcurrentHashMap())
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
        }
    }
}

agentToName = initAgentToName(nameToAgents)

//selectByIds(['zfwsfq7dw4tqw22r']) { bib ->
selectByIds(clusters.flatten()) { bib ->
    Map thing = bib.graph[1]
    def id = bib.doc.shortId

    def respStatement = thing.responsibilityStatement
    def contribution = thing.instanceOf?.contribution

    if (!contribution) {
        println("No contribution: $id")
        return
    }

    def contributionsInRespStatement = parseRespStatement(respStatement)

    def modified = contribution.removeAll { !it.agent }

    contribution.each { Map c ->
        modified |= tryLinkAgent(c, id)
        modified |= tryAddRole(c, id)
        if (contributionsInRespStatement) {
            modified |= tryAddRolesFromRespStatement(c, contributionsInRespStatement, id)
        }
    }

    if (!contributionsInRespStatement.isEmpty()) {
//        modified |= tryAddAgentsFromRespStatement(contribution, contributionsInRespStatement, id)
//        if (localAgentsFromRespStatement()) {
//            contribution += localAgentsFromRespStatement()
//            modified = true
//        }
    }
//
//    contribution.each {
//        tryAdd9pu()
//        tryAddTranslationOf()
//    }

    if (modified) {
        bib.scheduleSave()
    }
}

String toString(Map agent) {
    agent[ID_KEY] ?: agent.toString()
}

def roleShort(String iri) {
    iri?.split("/")?.last()
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

static Map<Object, String> initAgentToName(Map<String, List<Object>> nameToAgents) {
    def agentToName = [:]
    nameToAgents.each { name, agents ->
        agents.each {
            agentToName[(it)] = name
        }
    }
    return agentToName
}

boolean tryLinkAgent(Map contribution, String id) {
    def modified = false

    asList(contribution.agent).each { Map agent ->
        if (!agent.containsKey(ID_KEY)) {
            def name = agentToName[toString(agent)]
            if (!name) return
            def matchingLinkedAgents = nameToAgents[name].findAll {
                JsonLd.looksLikeIri(it) && (asList(contribution.role).isEmpty() || agentToRolesToIds[it].keySet().intersect(asList(contribution.role)))
            }
            def matchedGlobally = []
            for (agentIri in matchingLinkedAgents) {
                def matchingAgent = loadThing(agentIri)
                if (!yearMismatch(agent, matchingAgent)) {
                    Map roleToIds = agentToRolesToIds[agentIri]
                    def agentIsInCluster = roleToIds.findAll { idToCluster[id].intersect(it.value) }.with {
                        !it.isEmpty() && it.keySet().containsAll(asList(contribution.role))
                    }
                    if (agentIsInCluster) {
                        agent.clear()
                        agent[ID_KEY] = agentIri
                        asList(contribution.role).each { r ->
                            roleToIds[r].add(id)
                        }
                        linkedFoundInCluster.println([id, name, chipString(contribution, getWhelk())].join('\t'))
                        incrementStats('linked agent found in cluster', asList(contribution.role).collect { roleShort(it[ID_KEY]) }.sort())
                        modified = true
                        return
                    }

                    roleToIds = roleToIds.findAll { it.key != [:] }
                    def numDifferentRoles = roleToIds.size()
                    if (numDifferentRoles == 1) {
                        def (usualRole, numContributions) = roleToIds.collect { role, ids -> [role, ids.size()] }.first()
                        if (numContributions > 5) {
                            matchedGlobally.add([agentIri, usualRole, numContributions])
                        }
                    }
                }
            }
            if (matchedGlobally.size() == 1) {
                def (agentIri, usualRole, numContributions) = matchedGlobally[0]
                agent.clear()
                agent[ID_KEY] = agentIri
                agentToRolesToIds[agentIri][usualRole].add(id)
                linkedFoundGlobally.println([id, name, chipString(contribution, getWhelk()), numContributions].join('\t'))
                incrementStats('linked agent found globally', shortRole, id)
                modified = true
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

    def currentRoles = roleToIds.findAll { role, ids -> id in ids }.keySet()
    if (!noRole(currentRoles)) return false

    def foundRolesInCluster = roleToIds.findAll { role, ids ->
        role.containsKey(ID_KEY) && !noRole([role] as Set) && role[ID_KEY] != Relator.PRIMARY_RIGHTS_HOLDER.iri && ids.intersect(idToCluster[id])
    }.collect { it.key }

    if (foundRolesInCluster) {
        contribution['role'] = foundRolesInCluster
        foundRolesInCluster.each { r ->
            roleToIds[r].add(id)
            def inClusterWithSameRole = roleToIds[r].intersect(idToCluster[id])
            def shortRole = roleShort(r[ID_KEY])
            def ratio = "${inClusterWithSameRole.size()}/${idToCluster[id].size()}"
            roleFoundInCluster.println([id, linkedAgent, shortRole, ratio, inClusterWithSameRole.take(3)].join('\t'))
            incrementStats('role found in cluster', shortRole, id)
        }
        return true
    }

    roleToIds = roleToIds.findAll { it.key != [:] && it.key != [(ID_KEY): Relator.PRIMARY_RIGHTS_HOLDER.iri] }
    def numDifferentRoles = roleToIds.size()
    if (numDifferentRoles == 1) {
        def (usualRole, numContributions) = roleToIds.collect { role, ids -> [role, ids.size()] }.first()
        if (numContributions > 5 && usualRole[ID_KEY] != Relator.UNSPECIFIED_CONTRIBUTOR.iri) {
            roleToIds[usualRole].add(id)
            contribution['role'] = [usualRole]
            def shortRole = roleShort(usualRole[ID_KEY])
            roleFoundGlobally.println([id, linkedAgent, shortRole, numContributions].join('\t'))
            incrementStats('role found globally', shortRole, id)
            return true
        }
    }

    return false
}

boolean tryAddRolesFromRespStatement(Map contribution, Map contributionsInRespStatement, String id) {
    String agent = toString(asList(contribution.agent).find())
    String agentName = agentToName[agent]
    List<Tuple2> rolesInRespStatement = contributionsInRespStatement.remove(agentName)
    Map roleToIds = agentToRolesToIds[agent]

    if (!rolesInRespStatement || !roleToIds) return false

    def currentRoles = roleToIds.findAll { role, ids -> id in ids }.keySet()
    def isPrimaryContribution = contribution[ID_KEY] == 'PrimaryContribution'
    def rolesOfInterest = rolesInRespStatement.findResults { Relator relator, boolean isImplicitRole ->
        relator != Relator.UNSPECIFIED_CONTRIBUTOR
                && !(relator == Relator.AUTHOR && isImplicitRole && !isPrimaryContribution)
                ? [(ID_KEY): relator.iri]
                : null
    }

    if (rolesOfInterest) {
        def replace = noRole(currentRoles)
        def add = !replace && !currentRoles.containsAll(rolesOfInterest)
        if (replace || add) {
            def preUpdate = currentRoles.findResults { roleShort(it[ID_KEY]) }
            rolesOfInterest.each { r ->
                roleToIds.computeIfAbsent(r, f -> new ConcurrentHashMap().newKeySet()).add(id)
                def inClusterWithSameRole = roleToIds[r].intersect(idToCluster[id])
                def ratio = "${inClusterWithSameRole.size()}/${idToCluster[id].size()}"
                def globalOccurences = roleToIds[r].size()
                def shortRole = roleShort(r[ID_KEY])
                roleAddedFromRespStatement.println([id, preUpdate, shortRole, ratio, globalOccurences, inClusterWithSameRole.take(3)].join('\t'))
                incrementStats("role added from responsibilityStatement", shortRole, id)
            }
            contribution['role'] = replace ? rolesOfInterest : (asList(contribution['role']) + rolesOfInterest).unique()
            return true
        }
    }

    return false
}

boolean tryAddAgentsFromRespStatement(Map contribution, Map contributionsInRespStatement, String id) {
    contributionsInRespStatement.removeAll { String name, List roles ->
        roles = roles.collect { Relator r, _ -> [(ID_KEY): r.iri] }
        def (linkedAgents, localAgents) = asList(nameToAgents[name]).split()
        def linkedAgentsMatchingAnyRole = linkedAgents.findAll {
            agentToRolesToIds[it]
                    .keySet()
                    .intersect
        }
    }
}

boolean noRole(Set<Map> roles) {
    roles == ([[:]] as Set) || roles == ([[(ID_KEY): Relator.UNSPECIFIED_CONTRIBUTOR.iri]] as Set)
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

private static Map<String, List<Tuple2<Relator, Boolean>>> parseRespStatement(String respStatement) {
    def parsedContributions = [:]

    if (respStatement) {
        respStatement.split(';').eachWithIndex { part, i ->
            parseSwedishFictionContribution(StringUtils.normalizeSpace(part), i == 0).each { name, roles ->
                parsedContributions
                        .computeIfAbsent(normalize(name), r -> [])
                        .addAll(roles)
            }
        }
    }

    return parsedContributions
}

private static Map<String, List<Tuple2<Relator, Boolean>>> parseSwedishFictionContribution(String contribution, boolean isFirstPart) {
    def roleToPattern =
            [
                    (Relator.TRANSLATOR)         : ~/(bemynd(\w+|\.)? )?öf?v(\.|ers(\.|\p{L}+)?)( (till|från) \p{L}+)?|(till svenskan?|från \p{L}+)|svensk text/,
                    (Relator.AUTHOR)             : ~/^(text(e[nr])?|skriven|written)/,
                    (Relator.ILLUSTRATOR)        : ~/\bbild(er)?|ill(\.|ustr(\.|\w+)?)|\bvi(gn|nj)ett(er|ill)?|ritad/,
                    (Relator.AUTHOR_OF_INTRO)    : ~/förord|inl(edn(\.|ing)|edd)/,
                    (Relator.COVER_DESIGNER)     : ~/omslag/,
                    (Relator.AUTHOR_OF_AFTERWORD): ~/efter(ord|skrift)/,
                    (Relator.PHOTOGRAPHER)       : ~/\bfoto\w*\.?/,
//                        (Relator.EDITOR)             : ~/red(\.(?! av)|aktör(er)?)|\bbearb(\.|\w+)?|återberättad|sammanställ\w*/,
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
                .findAll { k, v -> m =~ /(?iu)$v/ }
                .with {
                    it.isEmpty() && contribution =~ /.+$followsRolePattern/
                            ? [new Tuple2(Relator.UNSPECIFIED_CONTRIBUTOR, isFirstPart)]
                            : it.collect { role, pattern -> new Tuple2(role, isFirstPart) }
                }

        // Author should be the role if first part of respStatement (before ';') and no role seems to be stated
        if (roles.isEmpty() && isFirstPart) {
            roles << new Tuple2(Relator.AUTHOR, isFirstPart)
        }

        // Extract names from the contribution
        def names = parseNames(fullNamePattern, conjPattern, m)

        // Assign the roles to each name
        nameToRoles.putAll(names.collectEntries { [it, roles] })
    }

    return nameToRoles
}

private static List<String> parseNames(Pattern namePattern, Pattern conjPattern, String s) {
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

private static boolean yearMismatch(Map a, Map b) {
    def lifeSpan = { Map agent ->
        agent.lifeSpan?.replaceAll(~/[^\-0-9]/, '')?.replaceAll(~/-+/, ' ')
    }
    def aLifeSpan = lifeSpan(a)
    def bLifeSpan = lifeSpan(b)
    return aLifeSpan && bLifeSpan && aLifeSpan != bLifeSpan
}

