package datatool.scripts.mergeworks

import org.apache.commons.lang3.StringUtils
import whelk.util.DocumentUtil
import whelk.util.Unicode

class Util {
    static def titleComponents = ['mainTitle', 'titleRemainder', 'subtitle', 'hasPart', 'partNumber', 'partName', 'marc:parallelTitle', 'marc:equalTitle']

    static def titleVariant = ['Title', 'ParallelTitle', 'VariantTitle', 'CoverTitle']

    private static Set<String> IGNORED_SUBTITLES = WorkJob.class.getClassLoader()
            .getResourceAsStream('merge-works/ignored-subtitles.txt')
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

    static List flatTitles(List hasTitle) {
        hasTitle.collect {
            def old = new TreeMap(it)

            if (it['subtitle']) {
                DocumentUtil.traverse(it['subtitle']) { value, path ->
                    if (path && value instanceof String && nonsenseSubtitle(value)) {
                        new DocumentUtil.Remove()
                    }
                }
            }

            def title = new TreeMap<>()
            title['flatTitle'] = normalize(Doc.flatten(old, titleComponents))
            if (it['@type']) {
                title['@type'] = it['@type']
            }

            title
        }
    }

    private static boolean nonsenseSubtitle(String s) {
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
            if (item[p] != null) {
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
                .collect { it['flatTitle']}
    }
}
