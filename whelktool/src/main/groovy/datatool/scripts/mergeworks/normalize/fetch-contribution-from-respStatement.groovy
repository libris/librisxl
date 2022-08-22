package datatool.scripts.mergeworks.normalize

import groovy.transform.Memoized
import org.apache.commons.lang3.StringUtils
import whelk.Document

import java.util.regex.Pattern

import static datatool.scripts.mergeworks.Util.contributionPath
import static datatool.scripts.mergeworks.Util.clusters
import static datatool.scripts.mergeworks.Util.asList
import static datatool.scripts.mergeworks.Util.chipString
import static datatool.scripts.mergeworks.Util.getPathSafe
import static datatool.scripts.mergeworks.Util.bestEncodingLevel
import static datatool.scripts.mergeworks.Util.nameMatch
import static datatool.scripts.mergeworks.Util.Relator

/**
 Example:
 $ ENV=qa && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters="reports/clusters.tsv" -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run src/main/groovy/datatool/scripts/mergeworks/normalize/fetch-contribution-from-respStatement.groovy
 */

PrintWriter report = getReportWriter("report.txt")

new File(System.getProperty(clusters)).splitEachLine('\t') { cluster ->
    incrementStats('fetch contribution from respStatement', 'clusters checked')

    def docs = Collections.synchronizedList([])
    selectByIds(cluster.collect { it.trim() }) {
        docs << it.doc
    }

    docs.each { Document d ->
        def respStatement = getPathSafe(d.data, ['@graph', 1, 'responsibilityStatement'])
        if (!respStatement)
            return

        incrementStats('fetch contribution from respStatement', 'docs checked')

        def changed = false

        def contributionsInRespStmt = parseRespStatement(respStatement)
        def contribution = getPathSafe(d.data, contributionPath, [])

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
                            || isFirstStmtPart && relator == Relator.AUTHOR && c.'@type' != 'PrimaryContribution'
                }

                def rolesInRespStatement = matchedOnName.value
                        .findResults { dontAdd(it) ? null : it.getV1() }

                if (rolesInRespStatement.isEmpty())
                    return

                def rolesInContribution = asList(c.role).findAll { it.'@id' != Relator.UNSPECIFIED_CONTRIBUTOR.iri }

                // Replace Adapter with Editor
                changed |= rolesInRespStatement.removeAll { r ->
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
                        changed = true
                        def roleShort = r.iri.split('/').last()
                        incrementStats('fetch contribution from respStatement', "$roleShort roles specified")

                        report.println("${chipString(c)} (${d.shortId}) <- $roleShort")
                    }
                }

                c.role = rolesInContribution
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
            for (Document other : docs) {
                def matched = getPathSafe(other.data, contributionPath, [])
                        .find { Map c ->
                            asList(c.agent).any { a ->
                                loadIfLink(a).with { nameMatch(name, it) && !(it.description =~ /(?i)pseud/) }
                                        && comparable(roles).with { r -> !r.isEmpty() && asList(c.role).containsAll(r) }
                                        && bestEncodingLevel.indexOf(d.getEncodingLevel()) <= bestEncodingLevel.indexOf(other.getEncodingLevel())
                            }
                        }
                if (matched) {
                    contribution << matched
                    roles.each {
                        def roleShort = it.getV1().iri.split('/').last()
                        incrementStats('fetch contribution from respStatement', "$roleShort found in cluster")
                    }
                    report.println("${d.shortId} <- ${chipString(matched)} (${other.shortId})")
                    changed = true
                    break
                }
            }
        }

        if (changed) {
            selectByIds([d.shortId]) {
                it.doc.data = d.data
                it.scheduleSave()
            }
        }
    }
}

private Map loadIfLink(Map m) {
    m['@id'] ? loadThing(m['@id']) : m
}

@Memoized
private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

private Map<String, List<Tuple2<Relator, Boolean>>> parseRespStatement(String respStatement) {
    def parsedContributions = [:]

    respStatement.split(';').eachWithIndex { part, i ->
        // TODO: generalize for other material types
        parseSwedishFictionContribution(StringUtils.normalizeSpace(part), i == 0).each { name, roles ->
            parsedContributions
                    .computeIfAbsent(name, r -> [])
                    .addAll(roles)
        }
    }

    return parsedContributions
}

private Map<String, List<Tuple2<Relator, Boolean>>> parseSwedishFictionContribution(String contribution, boolean isFirstPart) {
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

private List<String> parseNames(Pattern namePattern, Pattern conjPattern, String s) {
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
