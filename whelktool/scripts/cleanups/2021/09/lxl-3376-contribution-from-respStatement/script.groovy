import groovy.transform.Memoized
import java.util.concurrent.atomic.AtomicInteger
import java.text.Normalizer
import java.util.regex.Pattern

PrintWriter roleSpecified = getReportWriter("role-specified.txt")
PrintWriter parsing = getReportWriter("parsing.txt")
PrintWriter addedFromOtherInTitleCluster = getReportWriter("matched-in-title-cluster.txt")
PrintWriter numbers = getReportWriter("numbers.txt")

List titlesClusters = new File(scriptDir, 'title-clusters.tsv').readLines().collect { it.split() }
List fiction = new File(scriptDir, 'fiction.tsv').collect { it.split() }.flatten()

AtomicInteger numHaveRespStatementAndContribution = new AtomicInteger()
AtomicInteger sameNumContributors = new AtomicInteger()
AtomicInteger respStmtHasMore = new AtomicInteger()
AtomicInteger respStmtHasLess = new AtomicInteger()
AtomicInteger numExtraContributionsInRespStatement = new AtomicInteger()
AtomicInteger numFoundInSameTitleCluster = new AtomicInteger()
AtomicInteger numRolesSpecified = new AtomicInteger()

selectByIds(fiction) { data ->
    String recId = data.doc.shortId
    Map instance = data.graph[1]
    Map work = instance.instanceOf
    List contribution = work.contribution
    String respStmt = instance.responsibilityStatement

    if (!respStmt || !contribution)
        return

    numHaveRespStatementAndContribution.incrementAndGet()

    boolean modified

    Map contributorsInRespStmt = extractNameAndRolesFromRespStmt(respStmt)
    int numContributorsInRespStmt = contributorsInRespStmt.size()

    // Just material for verifying that the parsing works as expected
    parsing.println(data.doc.shortId)
    parsing.println(respStmt)
    parsing.println(contributorsInRespStmt)
    parsing.println()

    if (numContributorsInRespStmt == 0)
        return

    // Try adding roles from respStatement that are not specified for agent in contribution
    contribution.each { c ->
        if (!c.agent)
            return

        List cNames = getNames(c.agent)

        Map.Entry matchedOnName = contributorsInRespStmt.find { n, r ->
            matchAnyNameVariant(n, cNames)
        }

        if (matchedOnName) {
            // Contributor found locally, no need to search further later
            contributorsInRespStmt.remove(matchedOnName)

            List cRoles = asList(c.role)
            List rRoles = matchedOnName.value

            // Add missing role(s) for agent in contribution
            if (cRoles.isEmpty() || cRoles == [['@id':'https://id.kb.se/relator/unspecifiedContributor']]) {
                c['role'] = rRoles
                modified = true
                rRoles.each {
                    incrementStats("Roles specified", it.'@id')
                }
            } else {
                rRoles.each {
                    if (!(it in cRoles)) {
                        c['role'] = asList(c.role) + it
                        modified = true
                        incrementStats("Roles specified", it.'@id')
                    }
                }
            }

            if (modified) {
                roleSpecified.println(recId)
                roleSpecified.println(respStmt)
                roleSpecified.println(cNames)
                roleSpecified.println(c.role)
                roleSpecified.println()
                numRolesSpecified.incrementAndGet()
            }
        }
    }

    int numContributorsInContribution = contribution.size()

    // Compare number of contributors in respStatement vs contribution
    if (numContributorsInRespStmt == numContributorsInContribution)
        sameNumContributors.incrementAndGet()
    else if (numContributorsInRespStmt < numContributorsInContribution) {
        respStmtHasLess.incrementAndGet()
    } else {
        // Proceed if there seem to be more contributors in respStatement than in contribution
        respStmtHasMore.incrementAndGet()

        numExtraContributionsInRespStatement.addAndGet(contributorsInRespStmt.size())

        List otherInTitleCluster = titlesClusters.find { recId in it } - recId
        Map contributorsInSameTitleCluster = [:]

        selectByIds(otherInTitleCluster) {
            String id = it.doc.shortId
            List contr = it.graph[1].instanceOf.contribution
            contr?.each { c ->
                contributorsInSameTitleCluster[(c)] = id
            }
        }

        Map matchedInCluster = [:]

        // Filter out contributions in respStatement that are found somewhere else in the title cluster
        contributorsInRespStmt.removeAll { rName, rRoles ->
            Map.Entry foundContributor = contributorsInSameTitleCluster.find { c, id ->
                if (!c.agent)
                    return false
                List cRoles = asList(c.role)
                List cNames = getNames(c.agent)

                return matchAnyNameVariant(rName, cNames) && cRoles && cRoles.containsAll(rRoles)
            }
            if (foundContributor) {
                matchedInCluster << foundContributor
                return true
            }
        }

        numFoundInSameTitleCluster.addAndGet(matchedInCluster.size())

        // Now add the found contributors to local contribution
        if (matchedInCluster) {
            addedFromOtherInTitleCluster.println(data.doc.shortId)
            addedFromOtherInTitleCluster.println(respStmt)

            matchedInCluster.each { c, id ->
                contribution << c
                modified = true
                asList(c.role).each {
                    incrementStats("Roles retrieved from other in title cluster", it.'@id')
                }
                Map readable = ['names': getNames(c.agent), 'roles': asList(c.role).'@id']
                addedFromOtherInTitleCluster.println("${readable} • $id")
            }

            addedFromOtherInTitleCluster.println()
        }

        // Do something more with unmatched contributors in respStatement?
    }
    if (modified)
        data.scheduleSave()
}

numbers.println("Total: ${fiction.size()}")
numbers.println("Have respStatement and contribution: ${numHaveRespStatementAndContribution}")
numbers.println("Have same number of contributions in respStatement and contribution: ${sameNumContributors}")
numbers.println("Have less contributions in respStatement: ${respStmtHasLess}")
numbers.println("Have more contributions in respStatement: ${respStmtHasMore}")
numbers.println("Roles added from respStatement to agent in contribution locally: ${numRolesSpecified}")
numbers.println("Contributors in respStatement that could not be found locally in contribution: ${numExtraContributionsInRespStatement}")
numbers.println("Contributors in respStatement that were found in same title cluster (and added locally): ${numFoundInSameTitleCluster}")


Map extractNameAndRolesFromRespStmt(String respStmt) {
    Map roleMappings = roleMappings()
    Pattern namePattern = namePattern()
    Pattern roleAndNamePattern = roleAndNamePattern()
    Pattern likelyRolePattern = likelyRolePattern()

    Map contributorsInRespStmt = [:]

    respStmt.split(';').eachWithIndex { part, i ->
        part = part.trim().replaceAll(/\s+/, ' ')

        List extractedFromPart = (part =~ roleAndNamePattern)*.first()

        extractedFromPart.each { c ->
            List names = []
            List roles = []

            // Does the contribution seem to have a role stated?
            boolean seemsToIncludeRole = c =~ likelyRolePattern

            // Extract known roles from the contribution
            roleMappings.each { roleName, pattern ->
                if (c =~ pattern) {
                    roles << ['@id': 'https://id.kb.se/relator/' + roleName]
                    // Remove role from string, we don't want e.g. "Översättning" to be mistaken for a name later
                    c = c.replaceAll(pattern, '')
                }
            }

            // No known roles found?
            if (roles.isEmpty()) {
                // Author should be the role if first part of respStatement (before ';') and no role seems to be stated
                if (i == 0 && !seemsToIncludeRole)
                    roles << ['@id': 'https://id.kb.se/relator/author']
                // No point in continuing if we can't catch any roles
                else
                    return
            }

            // Now catch the names in the contribution
            (c =~ namePattern).each {
                String name = it[0]
                // Handle the case of "Jan och Maria Larsson"
                String previousName = names.isEmpty() ? null : names.last()
                if (previousName && previousName.split().size() == 1 && (c =~ /$previousName (&|och) $name/)) {
                    List nameParts = name.split()
                    if (nameParts.size() > 1) {
                        names[-1] += " ${nameParts.last()}"
                    }
                }
                names << name
            }

            // Assign the roles to each name
            names.each { n ->
                if (contributorsInRespStmt[n]) {
                    contributorsInRespStmt[n] += roles
                } else {
                    contributorsInRespStmt[n] = roles
                }
            }
        }
    }

    return contributorsInRespStmt
}

Map roleMappings() {
    // Add more roles?
    [
            'translator'                  : ~/(bemynd(\w+|\.)? )?öf?v(\.|ers(\.|\p{L}+)?)( (till|från) \p{L}+)?|(till|från) \p{L}+|svensk text/,
            'author'                      : ~/\btext(e[nr])?|^skriven/,
            'illustrator'                 : ~/\bbild(er)?|ill(\.|ustr(\.|\p{L}+)?)|(?<!ne[dr]|för|an)teckn\w*|vi(gn|nj)ett(er|ill)?|ritad/,
            'authorOfIntroduction'        : ~/förord|inl(edn(\.|ing)|edd)/,
            'adapter'                     : ~/\bbearb(\.|\w+)?/,
            'coverDesigner'               : ~/omslag/,
            'compiler'                    : ~/sammanställ\w*/,
            'authorOfAfterwordColophonEtc': ~/efter(ord|skrift)/,
            'photographer'                : ~/\bfoto\w*\.?/,
            'editor'                      : ~/red(\.|(iger|aktör)\w*)/
//                '?': /(åter)?berätta/,
//                '?': /komment/
    ]
}

Pattern roleAndNamePattern() {
    Pattern rolePattern = rolePattern()
    Pattern namePattern = namePattern()
    Pattern afterRole = afterRolePattern()
    Pattern conj = ~/( (och|&) |\/)/
    Pattern likelyRolePattern = likelyRolePattern()
    Pattern roleAfterName = ~/( ?\(($rolePattern$conj)?$rolePattern\))/

    return ~/(($rolePattern$conj)?(($rolePattern$afterRole)|$likelyRolePattern))?$namePattern($conj$namePattern)?$roleAfterName?/
}

Pattern rolePattern() {
    ~/((?iu)${roleMappings().values().join('|')})/
}

Pattern likelyRolePattern() {
    ~/([\p{L}|\d]\.?(:| a[fv]) )/
}

Pattern afterRolePattern() {
    ~/((:| a[fv])? )/
}

Pattern namePattern() {
    Pattern nameOrInitial = ~/\p{Lu}:?\p{Ll}*/
    Pattern midParts = ~/-| |\. ?| (de(l| la)?|von|van( de[nr])?|v\.|le|af|du|dos) | [ODdLl]'/
    Pattern name = ~/\p{Lu}:?\p{Ll}+/

    return ~/(($nameOrInitial($midParts)?)*$name)/
}

List<String> getNames(Map person) {
    return person.'@id' ? loadNames(person.'@id') : getNamesLocal(person)
}

List<String> getNamesLocal(Map person) {
    ([person] + (person.hasVariant ?: []))
            .collect { getName(it) }
            .grep()
            .collectMany { [it, it.replaceAll('-', ' ')] }
            .unique()
}

String getName(Map person) {
    if (person['@type'] == 'Person' && person.givenName && person.familyName) {
        "${person.givenName} ${person.familyName}"
    }
}

Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

@Memoized
List loadNames(def id) {
    getNamesLocal(loadThing(id))
}

boolean matchAnyNameVariant(String nameInRespStmt, List agentNames) {
    return agentNames.any { sameName(it, nameInRespStmt) }
}

boolean sameName(String a, String b) {

    List aParts = normalizeName(a).split()
    List bParts = normalizeName(b).split()

    if (aParts.size() == 1 || bParts.size() == 1) {
        return aParts == bParts
    }

    return aParts.containsAll(bParts) || bParts.containsAll(aParts)
}

String normalizeName(String name) {
    return Normalizer.normalize(name, Normalizer.Form.NFD)
            .replaceAll(/\p{M}/, '')
            .replaceAll(/\p{Lm}|\P{L}/, ' ')
            .replaceAll(/\s+/, ' ')
            .toLowerCase()
}

List asList(o) {
    return o in List ? o : o != null ? [o] : []
}
