package datatool.scripts.mergeworks

import org.apache.commons.lang3.StringUtils
import whelk.Whelk
import whelk.util.DocumentUtil
import whelk.util.Unicode

import java.util.regex.Pattern

class Util {
    static def titleComponents = ['mainTitle', 'titleRemainder', 'subtitle', 'hasPart', 'partNumber', 'partName', 'marc:parallelTitle', 'marc:equalTitle']

    static def titleVariant = ['Title', 'ParallelTitle']
    // removed 'VariantTitle', 'CoverTitle' since they sometimes contain random generic stuff like "Alibis filmroman", "Kompisböcker för de yngsta"

    static enum Relator {
        TRANSLATOR('https://id.kb.se/relator/translator'),
        AUTHOR('https://id.kb.se/relator/author'),
        ILLUSTRATOR('https://id.kb.se/relator/illustrator'),
        AUTHOR_OF_INTRO('https://id.kb.se/relator/authorOfIntroduction'),
        ADAPTER('https://id.kb.se/relator/adapter'),
        COVER_DESIGNER('https://id.kb.se/relator/coverDesigner'),
        COMPILER('https://id.kb.se/relator/compiler'),
        AUTHOR_OF_AFTERWORD('https://id.kb.se/relator/authorOfAfterwordColophonEtc'),
        PHOTOGRAPHER('https://id.kb.se/relator/photographer'),
        EDITOR('https://id.kb.se/relator/editor'),
        UNSPECIFIED_CONTRIBUTOR('https://id.kb.se/relator/unspecifiedContributor')


        String iri

        private Relator(String iri) {
            this.iri = iri
        }
    }

//    private static Set<String> IGNORED_SUBTITLES = WorkToolJob.class.getClassLoader()
//            .getResourceAsStream('merge-works/ignored-subtitles.txt')
//            .readLines().grep().collect(Util.&normalize) as Set

    private static Set<String> GENERIC_TITLES = WorkToolJob.class.getClassLoader()
            .getResourceAsStream('merge-works/generic-titles.txt')
            .readLines().grep().collect(Util.&normalize) as Set

    static def noise =
            [",", '"', "'", '[', ']', ',', '.', '.', ':', ';', '-', '(', ')', ' the ', '-', '–', '+', '!', '?'].collectEntries { [it, ' '] }


    static List asList(Object o) {
        (o ?: []).with { it instanceof List ? it : [it] }
    }

    /**
     * Partition a collection based on equality condition
     *
     * NOTE: O(n^2)...
     */
    static <T> Collection<Collection<T>> partition(Collection<T> collection, Closure matcher) {
        List<List<T>> result = []

        for (T t : collection) {
            boolean match = false
            for (List<T> group : result) {
                if (groupMatches(t, group, matcher)) {
                    group.add(t)
                    match = true
                    break
                }
            }

            if (!match) {
                result.add([t])
            }
        }
        return result
    }

    static <T> boolean groupMatches(T t, List<T> group, Closure matcher) {
        for (T other : group) {
            if (matcher(other, t)) {
                return true
            }
        }
        return false
    }

    static boolean hasGenericTitle(List hasTitle) {
        hasTitle.any { it['mainTitle'] && normalize((String) it['mainTitle']) in GENERIC_TITLES }
    }

    static List dropSubTitles(List hasTitle) {
        hasTitle.collect { t ->
            def copy = new TreeMap(t)
            copy.subMap(copy.keySet() - ['subtitle', 'titleRemainder'])
        }
    }

//    static List dropGenericSubTitles(List hasTitle) {
//        hasTitle.collect {
//            def copy = new TreeMap(it)
//            if (copy['subtitle'] || copy['titleRemainder']) {
//                DocumentUtil.traverse(copy) { value, path ->
//                    if (('subtitle' in path || 'titleRemainder' in path) && value instanceof String && genericSubtitle(value)) {
//                        new DocumentUtil.Remove()
//                    }
//                }
//            }
//            copy
//        }
//    }

    static List flatTitles(List hasTitle) {
        dropSubTitles(hasTitle).collect {
            def title = new TreeMap<>()
            title['flatTitle'] = normalize(Doc.flatten(it, titleComponents))
            if (it['@type']) {
                title['@type'] = it['@type']
            }

            title
        }
    }

//    private static boolean genericSubtitle(String s) {
//        s = Util.normalize(s)
//        if (s.startsWith("en ")) {
//            s = s.substring("en ".length())
//        }
//        return s in IGNORED_SUBTITLES
//    }

    static String normalize(String s) {
        return Unicode.asciiFold(Unicode.normalizeForSearch(StringUtils.normalizeSpace(" $s ".toLowerCase().replace(noise))))
    }

    static Object getPathSafe(item, path, defaultTo = null) {
        for (p in path) {
            if ((item instanceof Collection || item instanceof Map) && item[p] != null) {
                item = item[p]
            } else {
                return defaultTo
            }
        }
        return item
    }


    static List<String> getTitleVariants(List hasTitle) {
        flatTitles(hasTitle)
                .grep { it['@type'] in titleVariant }
                .collect { it['flatTitle'] }
    }

    static String chipString(def thing, Whelk whelk) {
        if (thing instanceof Integer) {
            return thing
        }

        def chips = whelk.jsonld.toChip(thing)
        if (chips.size() < 2) {
            chips = thing
        }
        if (chips instanceof List) {
            return chips.collect { valuesString(it) }.sort().join('<br>')
        }
        return valuesString(chips)
    }

    private static String valuesString(def thing) {
        if (thing instanceof List) {
            return thing.collect { valuesString(it) }.join(' • ')
        }
        if (thing instanceof Map) {
            return thing.findAll { k, v -> k != '@type' }.values().collect { valuesString(it) }.join(' • ')
        }
        return thing.toString()
    }

    // (docs on some of these levels are normally filtered out before we reach here)
    static List bestEncodingLevel = [
            'marc:FullLevel',
            'marc:FullLevelMaterialNotExamined',
            'marc:MinimalLevel',
            'marc:LessThanFullLevelMaterialNotExamined',
            'marc:CoreLevel',
            'marc:AbbreviatedLevel',
            'marc:PartialPreliminaryLevel',
            'marc:PrepublicationLevel',
            null
    ]

    // Return the most common title for the best encodingLevel
    static Object bestTitle(Collection<Doc> docs) {
        def isTitle = { it.'@type' == 'Title' }
        def addSource = { t, d -> t.plus(['source': [d.getInstance().subMap('@id')]]) }

        for (def level : bestEncodingLevel) {
            def titles = docs
                    .findAll { it.encodingLevel() == level }
                    .collect { d ->
                        d.getWork().get('hasTitle')?.findAll(isTitle)
                                ?: d.getInstance().get('hasTitle')?.findResults { isTitle(it) ? addSource(it, d) : null }
                    }
                    .grep()

            if (!titles) {
                continue
            }

            titles = titles.collect(Util.&dropSubTitles)
            return partition(titles, { a, b -> a == b }).sort { it.size() }.reverse().first().first()
        }

        return null
    }

    static Map<String, List<Tuple2<Relator, Boolean>>> parseRespStatement(String respStatement) {
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
}
