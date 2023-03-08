package whelk.util

import com.ibm.icu.text.Transliterator
import groovy.util.logging.Log4j2 as Log

import java.util.regex.Pattern

@Log
class Romanizer {
    /** Languages that use ALA-LOC "Asian Cyrillic - Multi-purpose transliteration for non-Slavic Cyrillic scripts"
     https://github.com/lcnetdev/transliterator/blob/main/scriptshifter/tables/data/index.yml

     Languages in the original list that have not been mapped to lang codes yet (because we don't have them in Libris):
     Abaza, Aisor, Chukchi, Dungan, Even, Evenki, Gagauz, Inuit, Khakass, Khanty, Komi-Permyak,
     Koryak, Lak, Lapp, Mansi, Molodstov, Mordvin, Nanai, Nenets, Nivkh, Shor, Permyak, Tabasaran,
     Tat (tat?), Tuva, Udekhe */
    private static List<String> ALA_LOC_NON_SLAVIC_CYRILLIC = [
            'abk', 'ady', 'alt', 'ava', 'bak', 'bua', 'che', 'chm', 'chv', 'dar',
            'inh', 'kaa', 'kbd', 'kom', 'krc', 'krl', 'kum', 'lez', 'lit', 'nog',
            'oss', 'rom', 'rum', 'rum', 'sah', 'sel', 'tut', 'udm', 'xal',
    ]

    private static final List<Transform> AUTO = [
            auto('be-Cyrl',  'be-Latn-t-be-Cyrl-m0-iso-1995', ['be-iso.txt', 'slavic-iso.txt']), 
            auto('bg-Cyrl',  'bg-Latn-t-bg-Cyrl-m0-iso-1995', ['bg-iso.txt', 'slavic-iso.txt']), 
            auto('el'     ,  'el-Latn-t-el-Grek-x0-btj', ['el-btj.txt']), 
            auto('grc'    ,  'grc-Latn-t-grc-Grek-x0-skr-1980', ['grc-skr.txt']), 
            auto('kk-Cyrl',  'kk-Latn-t-kk-Cyrl-m0-iso-1995', ['kk-iso.txt']), 
            auto('mk-Cyrl',  'mk-Latn-t-mk-Cyrl-m0-iso-1995', ['mk-iso.txt', 'slavic-iso.txt']), 
            auto('mn-Cyrl',  'mn-Latn-t-mn-Cyrl-x0-lessing', ['mn-lessing.txt']), 
            auto('ru-Cyrl',  'ru-Latn-t-ru-Cyrl-m0-iso-1995', ['ru-iso.txt', 'slavic-iso.txt']), 
            auto('sr-Cyrl',  'sr-Latn-t-sr-Cyrl-m0-iso-1995', ['sr-iso.txt', 'slavic-iso.txt']), 
            auto('uk-Cyrl',  'uk-Latn-t-uk-Cyrl-m0-iso-1995', ['uk-iso.txt', 'slavic-iso.txt']),

            // Converted from LOC mappings
            // TODO: investigate how well these handle case/capitalization
            auto('am-Ethi',  'am-Latn-t-am-Ethi-m0-alaloc', ['loc/am-Latn-t-am-Ethi-m0-alaloc.txt']), 
            auto('az-Cyrl',  'az-Latn-t-az-Cyrl-m0-alaloc', ['loc/az-Latn-t-az-Cyrl-m0-alaloc.txt']), 
            auto('chu'    ,  'chu-Latn-t-chu-Cyrs-m0-alaloc', ['loc/chu-Latn-t-chu-Cyrs-m0-alaloc.txt']), 
            auto('ka'     ,  'ka-Latn-t-ka-m0-alaloc', ['loc/ka-Latn-t-ka-m0-alaloc.txt']), 
            auto('hi-Deva',  'hi-Latn-t-hi-Deva-m0-alaloc', ['loc/hi-Latn-t-hi-Deva-m0-alaloc.txt']), 
            auto('hy-Armn',  'hy-Latn-t-hy-Armn-m0-alaloc', ['loc/hy-Latn-t-hy-Armn-m0-alaloc.txt']), 
            auto('kir-Cyrl', 'kir-Latn-t-kir-Cyrl-m0-alaloc', ['loc/kir-Latn-t-kir-Cyrl-m0-alaloc.txt']), 
            auto('mn-Mong',  'mn-Latn-t-mn-Mong-m0-alaloc', ['loc/mn-Latn-t-mn-Mong-m0-alaloc.txt']), 
            auto('tt-Cyrl',  'tt-Latn-t-tt-Cyrl-m0-alaloc', ['loc/tt-Latn-t-tt-Cyrl-m0-alaloc.txt']), 
            auto('tg-Cyrl',  'tg-Latn-t-tg-Cyrl-m0-alaloc', ['loc/tg-Latn-t-tg-Cyrl-m0-alaloc.txt']), 
            auto('tk-Cyrl',  'tk-Latn-t-tk-Cyrl-m0-alaloc', ['loc/tk-Latn-t-tk-Cyrl-m0-alaloc.txt']), 
            auto('uz-Cyrl',  'uz-Latn-t-uz-Cyrl-m0-alaloc', ['loc/uz-Latn-t-uz-Cyrl-m0-alaloc.txt']), 
            auto('zh-Hani',  'zh-Latn-t-zh-Hani-m0-alaloc', ['loc/zh-Latn-t-zh-Hani-m0-alaloc.txt']), 
    ] + alaLocNonSlavicCyrillic()

    private Map<String, List<Transform>> transliterators = [:]
    
    Romanizer(List<String> enabledTargetTags = AUTO.collect { it.targetTag() }) {
        enabledTargetTags.each { tag ->
            (AUTO.findAll{ it.targetTag() == tag } ?: [new Manual(tag)]).each(this::add)
        }
        log.info("Initialized with: ${transliterators.values().flatten().collect(Object::toString).sort()}")
    }
    
    Map<String, String> romanize(String s, String langTag) {
        (transliterators[langTag]
                ?: Unicode.guessIso15924ScriptCode(s)
                    .map {code -> transliterators["$langTag-$code"] }
                    .orElse([])
        ).collectEntries { [it.targetTag(), it.transform(s)]}
    }
    
    boolean isMaybeRomanizable(String langTag) {
        !isTransformed(langTag) && transliterators.keySet().any{ sourceTag -> sourceTag.startsWith(langTag) }
    }

    private void add(Transform transform) {
        transliterators.computeIfAbsent(transform.sourceTag(), s -> []).add(transform)
    }
    
    private static boolean isTransformed(String langTag) {
        langTag.contains('-t-')
    }

    private static Transform auto(String sourceTag, String targetTag, List<String> filenames) {
        Transliterator t = Transliterator.createFromRules(targetTag, filenames.collect(Romanizer::readFromResources).join('\n'), Transliterator.FORWARD)
        new Auto(sourceTag, t)
    }

    private static List<Transform> alaLocNonSlavicCyrillic() {
        ALA_LOC_NON_SLAVIC_CYRILLIC.collect { tag ->
            def from = "$tag-Cyrl".toString()
            def to = "${tag}-Latn-t-${tag}-Cyrl-m0-alaloc"
            auto(from, to, ['loc/und-Latn-t-und-Cyrl-m0-alaloc.txt'])
        }
    }

    private static String readFromResources(String filename) {
        Romanizer.class.getClassLoader().getResourceAsStream('romanizer/' + filename).getText("UTF-8")
    }

    interface Transform {
        String sourceTag()
        String targetTag()
        String transform(String s)
        default String toString() {"${getClass().getSimpleName().take(1)}(${sourceTag()} -> ${targetTag()})"}
    }

    static class Auto implements Transform {
        private String sourceTag
        private Transliterator transliterator

        Auto(String sourceTag, Transliterator transliterator) {
            this.sourceTag = sourceTag
            this.transliterator = transliterator
        }

        @Override
        String sourceTag() {
            return sourceTag
        }

        @Override
        String targetTag() {
            return transliterator.getID()
        }

        @Override
        String transform(String source) {
            return transliterator.transform(source)
        }
    }

    static class Manual implements Transform {
        private static final var SOURCE = Pattern.compile(".*-t-(.*?)(-\\p{Alpha}\\p{Digit}-.*)?\$")
        private String sourceTag
        private String targetTag

        Manual(String targetTag) {
            var m = SOURCE.matcher(targetTag)
            if (m.matches()) {
                this.sourceTag = m.group(1)
            } else {
                this.sourceTag = "---------"
                // TODO: throw or log
            }
            this.targetTag = targetTag
        }

        @Override
        String sourceTag() {
            return sourceTag
        }

        @Override
        String targetTag() {
            return targetTag
        }

        @Override
        String transform(String s) {
            return ""
        }
    }
}
