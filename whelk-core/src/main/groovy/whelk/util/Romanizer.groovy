package whelk.util

import com.ibm.icu.text.Transliterator

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

    private static final Map<String, List<Transliterator>> TRANSLITERATORS = [
            'be-Cyrl': [romanizer('be-Latn-t-be-Cyrl-m0-iso-1995', ['be-iso.txt', 'slavic-iso.txt'])],
            'bg-Cyrl': [romanizer('bg-Latn-t-bg-Cyrl-m0-iso-1995', ['bg-iso.txt', 'slavic-iso.txt'])],
            'el'     : [romanizer('el-Latn-t-el-Grek-x0-btj', ['el-btj.txt'])],
            'grc'    : [romanizer('grc-Latn-t-grc-Grek-x0-skr-1980', ['grc-skr.txt'])],
            'kk-Cyrl': [romanizer('kk-Latn-t-kk-Cyrl-m0-iso-1995', ['kk-iso.txt'])],
            'mk-Cyrl': [romanizer('mk-Latn-t-mk-Cyrl-m0-iso-1995', ['mk-iso.txt', 'slavic-iso.txt'])],
            'mn-Cyrl': [romanizer('mn-Latn-t-mn-Cyrl-x0-lessing', ['mn-lessing.txt'])],
            'ru-Cyrl': [romanizer('ru-Latn-t-ru-Cyrl-m0-iso-1995', ['ru-iso.txt', 'slavic-iso.txt'])],
            'sr-Cyrl': [romanizer('sr-Latn-t-sr-Cyrl-m0-iso-1995', ['sr-iso.txt', 'slavic-iso.txt'])],
            'uk-Cyrl': [romanizer('uk-Latn-t-uk-Cyrl-m0-iso-1995', ['uk-iso.txt', 'slavic-iso.txt'])],

            // Converted from LOC mappings
            // TODO: investigate how well these handle case/capitalization
            'am-Ethi' : [romanizer('am-Latn-t-am-Ethi-m0-alaloc', ['loc/am-Latn-t-am-Ethi-m0-alaloc.txt'])],
            'az-Cyrl' : [romanizer('az-Latn-t-az-Cyrl-m0-alaloc', ['loc/az-Latn-t-az-Cyrl-m0-alaloc.txt'])],
            'chu'     : [romanizer('chu-Latn-t-chu-Cyrs-m0-alaloc', ['loc/chu-Latn-t-chu-Cyrs-m0-alaloc.txt'])],
            'ka'      : [romanizer('ka-Latn-t-ka-m0-alaloc', ['loc/ka-Latn-t-ka-m0-alaloc.txt'])],
            'hi-Deva' : [romanizer('hi-Latn-t-hi-Deva-m0-alaloc', ['loc/hi-Latn-t-hi-Deva-m0-alaloc.txt'])],
            'hy-Armn' : [romanizer('hy-Latn-t-hy-Armn-m0-alaloc', ['loc/hy-Latn-t-hy-Armn-m0-alaloc.txt'])],
            'kir-Cyrl': [romanizer('kir-Latn-t-kir-Cyrl-m0-alaloc', ['loc/kir-Latn-t-kir-Cyrl-m0-alaloc.txt'])],
            'mn-Mong' : [romanizer('mn-Latn-t-mn-Mong-m0-alaloc', ['loc/mn-Latn-t-mn-Mong-m0-alaloc.txt'])],
            'tt-Cyrl' : [romanizer('tt-Latn-t-tt-Cyrl-m0-alaloc', ['loc/tt-Latn-t-tt-Cyrl-m0-alaloc.txt'])],
            'tg-Cyrl' : [romanizer('tg-Latn-t-tg-Cyrl-m0-alaloc', ['loc/tg-Latn-t-tg-Cyrl-m0-alaloc.txt'])],
            'tk-Cyrl' : [romanizer('tk-Latn-t-tk-Cyrl-m0-alaloc', ['loc/tk-Latn-t-tk-Cyrl-m0-alaloc.txt'])],
            'uz-Cyrl' : [romanizer('uz-Latn-t-uz-Cyrl-m0-alaloc', ['loc/uz-Latn-t-uz-Cyrl-m0-alaloc.txt'])],
            'zh-Hani' : [romanizer('zh-Latn-t-zh-Hani-m0-alaloc', ['loc/zh-Latn-t-zh-Hani-m0-alaloc.txt'])],
    ] + alaLocNonSlavicCyrillic()
    
    private static final Set<String> TAGS = TRANSLITERATORS.keySet() + TRANSLITERATORS.keySet().collect { it.split('-')[0] }
    
    Romanizer(List<String> enabledTransforms) {
            
    }
    
    static Set<String> romanizableLangTags() {
        return TAGS.asUnmodifiable()
    }
    
    static Map<String, String> romanize(String s, String langTag) {
        (transliterators(langTag)
                ?: Unicode.guessIso15924ScriptCode(s)
                    .map {code -> transliterators("$langTag-$code") }
                    .orElse([])
        ).collectEntries { [it.getID(), it.transform(s)]}
    }
 
    private static List<Transliterator> transliterators(String langTag) {
        TRANSLITERATORS.getOrDefault(langTag, [])
    }

    private static Transliterator romanizer(String id, List<String> filenames) {
        Transliterator.createFromRules(id, filenames.collect(Romanizer::readFromResources).join('\n'), Transliterator.FORWARD)
    }
    
    private static Transliterator manual(String id) {
        Transliterator.createFromRules(id, '', Transliterator.FORWARD)
    }

    private static Map<String, List<Transliterator>> alaLocNonSlavicCyrillic() {
        ALA_LOC_NON_SLAVIC_CYRILLIC.collectEntries { tag ->
            def from = "$tag-Cyrl".toString()
            def to = "${tag}-Latn-t-${tag}-Cyrl-m0-alaloc"
            [(from): [romanizer(to, ['loc/und-Latn-t-und-Cyrl-m0-alaloc.txt'])]]
        }
    }

    private static String readFromResources(String filename) {
        Romanizer.class.getClassLoader().getResourceAsStream('romanizer/' + filename).getText("UTF-8")
    }
}
