package se.kb.libris.mergeworks

import org.apache.commons.lang3.StringUtils
import whelk.Whelk
import whelk.util.DocumentUtil
import whelk.util.Unicode

import static se.kb.libris.mergeworks.compare.IntendedAudience.preferredComparisonOrder
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY

class Util {
    static final String CLASSIFICATION = 'classification'
    static final String IN_SCHEME = 'inScheme'
    static final String VERSION = 'version'

    static final String PRIMARY = 'PrimaryContribution'
    static final String CONTRIBUTION = 'contribution'
    static final String AGENT = 'agent'
    static final String GIVEN_NAME = 'givenName'
    static final String FAMILY_NAME = 'familyName'
    static final String NAME = 'name'
    static final String ROLE = 'role'
    static final String LIFE_SPAN = 'lifeSpan'

    static final String TITLE = 'Title'
    static final String HAS_TITLE = 'hasTitle'
    static final String MAIN_TITLE = 'mainTitle'
    static final String SUBTITLE = 'subtitle'
    static final String TITLE_REMAINDER = 'titleRemainder'
    static final String PART_NUMBER = 'partNumber'
    static final String PART_NAME = 'partName'
    static final String FLAT_TITLE = 'flatTitle'
    static final String SOURCE = 'source'

    static final String CODE = 'code'
    static final String LABEL = 'label'
    static final String HAS_PART = 'hasPart'

    static final String TRANSLATION_OF = 'translationOf'
    static final String GENRE_FORM = 'genreForm'
    static final String INTENDED_AUDIENCE = 'intendedAudience'
    static final String CONTENT_TYPE = 'contentType'

    static final String PUBLICATION = 'publication'
    static final String EXTENT = 'extent'
    static final String REPRODUCTION_OF = 'reproductionOf'
    static final String IDENTIFIED_BY = 'identifiedBy'
    static final String EDITION_STATEMENT = 'editionStatement'
    static final String RESP_STATEMENT = 'responsibilityStatement'
    static final String PHYS_NOTE = 'physicalDetailsNote'

    static final String ENCODING_LEVEL = 'encodingLevel'

    static def titleComponents = [MAIN_TITLE, TITLE_REMAINDER, SUBTITLE, HAS_PART, PART_NUMBER, PART_NAME]

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
        hasTitle.any { it[MAIN_TITLE] && normalize((String) it[MAIN_TITLE]) in GENERIC_TITLES }
    }

    static List dropGenericSubTitles(List hasTitle) {
        hasTitle.collect {
            def copy = new TreeMap(it)
            if (copy[SUBTITLE] || copy[TITLE_REMAINDER]) {
                DocumentUtil.traverse(copy) { value, path ->
                    if ((SUBTITLE in path || TITLE_REMAINDER in path) && value instanceof String) {
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
            title[FLAT_TITLE] = normalize(DisplayDoc.flatten(it, titleComponents))
            if (it[TYPE_KEY]) {
                title[TYPE_KEY] = it[TYPE_KEY]
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
                .collect { it[FLAT_TITLE] }
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
        def part = [partNumber, partName].grep().join(', ')
        if (part) {
            title[MAIN_TITLE] += "${title[MAIN_TITLE][-1] == '.' ? '' : '.'} $part"
        }
    }

    static String findTitlePart(List<Map> title, String prop) {
        // partName/partNumber is usually found in hasPart but not always
        def titlePart = title.findResult { Map t -> t[prop] ?: t[HAS_PART].findResult { it[prop] } }
        return asList(titlePart).find()
    }

    // Return the most common title for the best encodingLevel
    static def bestTitle(Collection<Doc> docs) {
        // Always keep title on existing linked work as is
        def linkedWorkTitle = docs.findResult { it.workIri() ? it.workData[HAS_TITLE] : null }
        if (linkedWorkTitle) {
            return linkedWorkTitle
        }

        def bestInstanceTitle = mostCommonHighestEncodingLevel(docs, this.&mostCommonInstanceTitle)
        def bestWorkTitle = mostCommonHighestEncodingLevel(docs, this.&mostCommonWorkTitle)

        def partNumber = findTitlePart(bestInstanceTitle, PART_NUMBER)
        def partName = findTitlePart(bestInstanceTitle, PART_NAME)

        def workTitleShape = { it.subMap([TYPE_KEY, MAIN_TITLE, SUBTITLE, TITLE_REMAINDER, SOURCE, 'marc:nonfilingChars']) }

        // Prefer existing work title over instance titles
        if (bestWorkTitle) {
            // Include part number in work title if present in instance titles.
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
            d.translationOf().findResult { it[HAS_TITLE] }?.findAll(isTitle)
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
            return t.collect { it.plus([(SOURCE): [d.instanceData.subMap(ID_KEY)]]) }
        }

        def instanceTitles = docs.collect { it.instanceTitle().findAll(isTitle) }
                .collect { dropGenericSubTitles(it) }

        if (instanceTitles.grep()) {
            def instanceTitleToDoc = [instanceTitles, docs].transpose().collectEntries()
            def best = mostCommon(instanceTitles.grep())
            // Source is picked arbitrary among the instances having the most common title
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

    static def isTitle = { it[TYPE_KEY] == TITLE }

    static String name(Map agent) {
        (agent[GIVEN_NAME] && agent[FAMILY_NAME])
                ? normalize("${agent[GIVEN_NAME]} ${agent[FAMILY_NAME]}")
                : agent[NAME] ? normalize("${agent[NAME]}") : null
    }

    // Cluster records that seem to describe the same work
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