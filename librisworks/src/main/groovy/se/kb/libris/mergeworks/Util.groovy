package se.kb.libris.mergeworks

import org.apache.commons.lang3.StringUtils
import whelk.Whelk
import whelk.util.DocumentUtil
import whelk.util.Unicode

import static se.kb.libris.mergeworks.compare.IntendedAudience.preferredComparisonOrder

class Util {
    static def titleComponents = ['mainTitle', 'titleRemainder', 'subtitle', 'hasPart', 'partNumber', 'partName']

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

    static def noise =
            [",", '"', "'", "ʹ", "ʼ", '[', ']', ',', '.', '.', ':', ';', '-', '(', ')', ' the ', '-', '–', '+', '!', '?',].collectEntries { [it, ' '] }

    private static Set<String> IGNORED_SUBTITLES = Util.class.getClassLoader()
            .getResourceAsStream('merge-works/ignored-subtitles.txt')
            .readLines().grep().collect(Util.&normalize) as Set

    private static Set<String> GENERIC_TITLES = Util.class.getClassLoader()
            .getResourceAsStream('merge-works/generic-titles.txt')
            .readLines().grep().collect(Util.&normalize) as Set


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
                            // Remove substring after colon if identified as generic
                            // Example: "klanen Kennedy : roman" -> "klanen Kennedy"
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
        return Unicode.removeDiacritics(Unicode.normalizeForSearch(StringUtils.normalizeSpace(" $s ".toLowerCase().replace(noise))))
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

    static void appendTitlePartsToMainTitle(Map title, String partNumber, String partName = null) {
        if (title['mainTitle'][-1] != '.') {
            title['mainTitle'] += '.'
        }
        if (partNumber && partName) {
            title['mainTitle'] += " $partNumber, $partName"
        } else if (partNumber) {
            title['mainTitle'] += " $partNumber"
        } else if (partName) {
            title['mainTitle'] += " $partName"
        }
    }

    static String findTitlePart(List<Map> title, String prop) {
        // partName/partNumber is usually found in hasPart but not always
        def partNumber = title.findResult { Map t -> t[prop] ?: t['hasPart'].findResult { it[prop] } }
        return asList(partNumber).find()
    }

    // Return the most common title for the best encodingLevel
    static def bestTitle(Collection<Doc> docs) {
        def linkedWorkTitle = docs.findResult { it.workIri() ? it.workData['hasTitle'] : null }
        if (linkedWorkTitle) {
            return linkedWorkTitle
        }

        def bestInstanceTitle = mostCommonHighestEncodingLevel(docs, this.&mostCommonInstanceTitle)
        def bestWorkTitle = mostCommonHighestEncodingLevel(docs, this.&mostCommonWorkTitle)

        def partNumber = findTitlePart(bestInstanceTitle, 'partNumber')
        def partName = findTitlePart(bestInstanceTitle, 'partName')

        def workTitleShape = { it.subMap(['@type', 'mainTitle', 'subtitle', 'titleRemainder', 'source']) }

        if (bestWorkTitle) {
            return bestWorkTitle.each { appendTitlePartsToMainTitle(it, partNumber) }
                    .collect(workTitleShape)
        }

        return bestInstanceTitle.each { appendTitlePartsToMainTitle(it, partNumber, partName) }
                .collect(workTitleShape)
    }

    static def mostCommonHighestEncodingLevel(Collection<Doc> docs, Closure<Collection<Doc>> findMostCommon) {
        for (def level : bestEncodingLevel) {
            def onLevel = docs.findAll { it.encodingLevel() == level }
            def mostCommonTitle = findMostCommon(onLevel)
            if (mostCommonTitle) {
                return mostCommonTitle
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

    static String name(Map agent) {
        (agent.givenName && agent.familyName)
                ? normalize("${agent.givenName} ${agent.familyName}")
                : agent.name ? normalize("${agent.name}") : null
    }

    static Collection<Collection<Doc>> workClusters(Collection<Doc> docs, WorkComparator c) {
        docs.each {
            if (it.instanceData) {
                it.addComparisonProps()
            }
        }.with { preferredComparisonOrder(it) }

        def workClusters = partition(docs, { Doc a, Doc b -> c.sameWork(a, b) })
                .each { work ->
                    work.each { doc ->
                        doc.removeComparisonProps()
                        // List order may be shuffled when comparing works.
                        // Make sure PrimaryContribution always comes first in contribution.
                        doc.sortContribution()
                    }
                }

        return workClusters
    }
}