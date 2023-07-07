package mergeworks

import org.apache.commons.lang3.StringUtils
import whelk.Whelk
import whelk.util.DocumentUtil
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
        PRIMARY_RIGHTS_HOLDER('https://id.kb.se/relator/primaryRightsHolder'),
        ABRIDGER('https://id.kb.se/relator/abridger'),
        IMPLICIT_AUTHOR('https://id.kb.se/relator/author')

        String iri

        private Relator(String iri) {
            this.iri = iri
        }
    }

    private static Set<String> IGNORED_SUBTITLES = Util.class.getClassLoader()
            .getResourceAsStream('merge-works/ignored-subtitles.txt')
            .readLines().grep().collect(Util.&normalize) as Set

    private static Set<String> GENERIC_TITLES = Util.class.getClassLoader()
            .getResourceAsStream('merge-works/generic-titles.txt')
            .readLines().grep().collect(Util.&normalize) as Set

    static def noise =
            [",", '"', "'", "ʹ", "ʼ", '[', ']', ',', '.', '.', ':', ';', '-', '(', ')', ' the ', '-', '–', '+', '!', '?',].collectEntries { [it, ' '] }


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
        group.every { other -> matcher(other, t) }
    }

    static boolean hasGenericTitle(List hasTitle) {
        hasTitle.any { it['mainTitle'] && normalize((String) it['mainTitle']) in GENERIC_TITLES }
    }

    static List dropGenericSubTitles(List hasTitle) {
        hasTitle.collect {
            def copy = new TreeMap(it)
            if (copy['subtitle'] || copy['titleRemainder']) {
                DocumentUtil.traverse(copy) { value, path ->
                    if (('subtitle' in path || 'titleRemainder' in path) && value instanceof String) {
                        if (genericSubtitle(value)) {
                            new DocumentUtil.Remove()
                        } else {
                            ((List) value.split(':')).with {
                                if (it.size() > 1 && genericSubtitle(it.last().trim())) {
                                    new DocumentUtil.Replace(value.replaceFirst(~/\s*:.+$/, ''))
                                }
                            }
                        }
                    }
                }
            }
            copy
        }
    }

    static List flatTitles(List hasTitle) {
        dropGenericSubTitles(hasTitle).collect {
            def title = new TreeMap<>()
            title['flatTitle'] = normalize(DisplayDoc.flatten(it, titleComponents))
            if (it['@type']) {
                title['@type'] = it['@type']
            }

            title
        }
    }

    private static boolean genericSubtitle(String s) {
        s = Util.normalize(s)
        if (s.startsWith("en ")) {
            s = s.substring("en ".length())
        }
        return s in IGNORED_SUBTITLES
    }

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

    static List<String> getFlatTitle(List hasTitle) {
        flatTitles(hasTitle)
                .grep(isTitle)
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

    static def toWorkTitleForm = { Map title ->
        // partName/partNumber is usually in hasPart but not always
        def partName = title['partName']
        def partNumber = title['partNumber']

        def hasPart = title['hasPart']
        if (hasPart) {
            partName = hasPart[0]['partName']
            partNumber = hasPart[0]['partNumber']
        }

        partName = asList(partName)[0]
        partNumber = asList(partNumber)[0]

        if (partNumber && partName) {
            title['mainTitle'] += ". $partNumber, $partName"
        } else if (partNumber) {
            title['mainTitle'] += ". $partNumber"
        } else if (partName) {
            title['mainTitle'] += ". $partName"
        }

        return title.subMap(['@type', 'mainTitle', 'source'])
    }

    // Return the most common title for the best encodingLevel
    static def bestTitle(Collection<Doc> docs) {
        // TODO: which title to pick when matched with already existing linked work?
        def linkedWorkTitle = docs.findResult { it.workIri() ? it.workData['hasTitle'] : null }
        if (linkedWorkTitle) {
            return linkedWorkTitle
        }

        for (def level : bestEncodingLevel) {
            def onLevel = docs.findAll { it.encodingLevel() == level }
            def bestWorkTitle = mostCommonWorkTitle(onLevel)
            if (bestWorkTitle) {
                return bestWorkTitle
            }
        }

        for (def level : bestEncodingLevel) {
            def onLevel = docs.findAll { it.encodingLevel() == level }
            def bestInstanceTitle = mostCommonInstanceTitle(onLevel)
            if (bestInstanceTitle) {
                return bestInstanceTitle.collect(toWorkTitleForm)
            }
        }

        return null
    }

    static def bestOriginalTitle(Collection<Doc> docs) {
        for (def level : bestEncodingLevel) {
            def onLevel = docs.findAll { it.encodingLevel() == level }
            def bestOrigTitle = mostCommonOriginalTitle(onLevel)
            if (bestOrigTitle) {
                return bestOrigTitle
            }
        }

        return null
    }

    static def mostCommonOriginalTitle(Collection<Doc> docs) {
        return mostCommonWorkTitle(docs) { Doc d ->
            d.translationOf().findResult { it['hasTitle'] }?.findAll(isTitle)
        }
    }

    static def mostCommonWorkTitle(Collection<Doc> docs, Closure getTitle = { it.workTitle().findAll(isTitle) }) {
        def workTitles = docs.collect(getTitle)
                .grep()
                .collect { dropGenericSubTitles(it) }

        if (workTitles) {
            return mostCommon(workTitles)
        }

        return null
    }

    static def mostCommonInstanceTitle(Collection<Doc> docs) {
        def addSource = { t, d ->
            return t.collect { it.plus(['source': [d.instanceData.subMap('@id')]]) }
        }

        def instanceTitles = docs.collect { it.instanceTitle().findAll(isTitle) }
                .collect { dropGenericSubTitles(it) }

        if (instanceTitles.grep()) {
            def instanceTitleToDoc = [instanceTitles, docs].transpose().collectEntries()
            def best = mostCommon(instanceTitles.grep())
            return addSource(best, instanceTitleToDoc[best])
        }

        return null
    }

    static def mostCommon(titles) {
        return partition(titles, { a, b -> a == b })
                .sort { it.size() }
                .reverse()
                .first()
                .first()
    }

    static def isTitle = { it.'@type' == 'Title' }

    static boolean nameMatch(Object local, Map agent) {
        def variants = [agent] + asList(agent.hasVariant)

        def localName = local instanceof Map ? name(local) : normalize(local)

        localName && variants.any {
            name(it) && localName == name(it)
        }
    }

    static String name(Map agent) {
        (agent.givenName && agent.familyName)
                ? normalize("${agent.givenName} ${agent.familyName}")
                : agent.name ? normalize("${agent.name}") : null
    }
}