package datatool.scripts.mergeworks

import org.apache.commons.lang3.StringUtils
import whelk.Whelk
import whelk.util.Unicode

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
        UNSPECIFIED_CONTRIBUTOR('https://id.kb.se/relator/unspecifiedContributor'),
        PRIMARY_RIGHTS_HOLDER('https://id.kb.se/relator/primaryRightsHolder')

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
            title['flatTitle'] = normalize(DisplayDoc.flatten(it, titleComponents))
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
        def addSource = { t, d -> t.plus(['source': [d.getMainEntity().subMap('@id')]]) }

        for (def level : bestEncodingLevel) {
            def onLevel = docs.findAll { it.encodingLevel() == level }

            def workTitles = onLevel.collect { it.getWork().get('hasTitle')?.findAll(isTitle) }.collect(Util.&dropSubTitles)
            def instanceTitles = onLevel.collect { it.getMainEntity().get('hasTitle')?.findAll(isTitle) }.collect(Util.&dropSubTitles)
            def instanceTitleToDoc = [instanceTitles, onLevel].transpose().collectEntries()

            def titles = [workTitles, instanceTitles].transpose().collect { wt, itt ->
                wt ?: itt
            }.grep()

            if (!titles) {
                continue
            }

            def bestTitles = partition(titles, { a, b -> a == b })
                    .sort { it.size() }
                    .reverse()
                    .with { p -> p.takeWhile { it.size() == p.first().size() } }
                    .collect { it.first() }

            return bestTitles.find { it in workTitles }
                    ?: addSource(bestTitles.first(), instanceTitleToDoc[bestTitles.first()])
        }

        return null
    }

    static boolean nameMatch(Object local, Map agent) {
        def variants = [agent] + asList(agent.hasVariant)
        def name = {
            Map p ->
                (p.givenName && p.familyName)
                        ? normalize("${p.givenName} ${p.familyName}")
                        : p.name ? normalize("${p.name}") : null
        }

        def localName = local instanceof Map ? name(local) : normalize(local)

        localName && variants.any {
            name(it) && localName == name(it)
        }
    }
}
