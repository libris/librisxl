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
            'be'     : [romanizer('be-Latn-t-be-Cyrl-m0-iso-1995', ['be-iso.txt', 'slavic-iso.txt'])],
            'bg'     : [romanizer('bg-Latn-t-bg-Cyrl-m0-iso-1995', ['bg-iso.txt', 'slavic-iso.txt'])],
            'el'     : [romanizer('el-Latn-t-el-Grek-x0-btj', ['el-btj.txt'])],
            'grc'    : [romanizer('grc-Latn-t-grc-Grek-x0-skr-1980', ['grc-skr.txt'])],
            // TODO: distinguish ISO for slavic languages vs ISO för non-slavic languages? Or describe them with the same entity?
            'kk'     : [romanizer('kk-Latn-t-kk-Cyrl-m0-iso-1995', ['kk-iso.txt'])],
            'mk'     : [romanizer('mk-Latn-t-mk-Cyrl-m0-iso-1995', ['mk-iso.txt', 'slavic-iso.txt'])],
            // TODO: change to mn-Cyrl ?
            'mn'     : [romanizer('mn-Latn-t-mn-Cyrl-x0-lessing', ['mn-lessing.txt'])],
            'ru'     : [romanizer('ru-Latn-t-ru-Cyrl-m0-iso-1995', ['ru-iso.txt', 'slavic-iso.txt'])],
            'sr'     : [romanizer('sr-Latn-t-sr-Cyrl-m0-iso-1995', ['sr-iso.txt', 'slavic-iso.txt'])],
            'uk'     : [romanizer('uk-Latn-t-uk-Cyrl-m0-iso-1995', ['uk-iso.txt', 'slavic-iso.txt'])],

            // Converted from LOC mappings
            // TODO: investigate how well these handle case/capitalization
            'am'     : [romanizer('am-Latn-t-am-m0-alaloc', ['loc/am-Latn-t-am-m0-alaloc.txt'])],
            'aze'    : [romanizer('aze-Latn-t-aze-Cyrl-m0-alaloc', ['loc/aze-Latn-t-aze-Cyrl-m0-alaloc.txt'])],
            'chu'    : [romanizer('chu-Latn-t-chu-m0-alaloc', ['loc/chu-Latn-t-chu-m0-alaloc.txt'])],
            'geo'    : [romanizer('geo-Latn-t-geo-m0-alaloc', ['loc/geo-Latn-t-geo-m0-alaloc.txt'])],
            'hin'    : [romanizer('hin-Latn-t-hin-Deva-m0-alaloc', ['loc/hin-Latn-t-hin-Deva-m0-alaloc.txt'])],
            'hy'     : [romanizer('hy-Latn-t-hy-m0-alaloc', ['loc/hy-Latn-t-hy-m0-alaloc.txt'])],
            'kir'    : [romanizer('kir-Latn-t-kir-Cyrl-m0-alaloc', ['loc/kir-Latn-t-kir-Cyrl-m0-alaloc.txt'])],
            'mn-Mong': [romanizer('mn-Latn-t-mn-Mong-m0-alaloc', ['loc/mn-Latn-t-mn-Mong-m0-alaloc.txt'])],
            'tat'    : [romanizer('tat-Latn-t-tat-Cyrl-m0-alaloc', ['loc/tat-Latn-t-tat-Cyrl-m0-alaloc.txt'])],
            'tgk'    : [romanizer('tgk-Latn-t-tgk-Cyrl-m0-alaloc', ['loc/tgk-Latn-t-tgk-Cyrl-m0-alaloc.txt'])],
            'tuk'    : [romanizer('tuk-Latn-t-tuk-Cyrl-m0-alaloc', ['loc/tuk-Latn-t-tuk-Cyrl-m0-alaloc.txt'])],
            'uzb'    : [romanizer('uzb-Latn-t-uzb-Cyrl-m0-alaloc', ['loc/uzb-Latn-t-uzb-Cyrl-m0-alaloc.txt'])],
    ] + alaLocNonSlavicCyrillic()
    
    static Map<String, String> romanize(String s, String langTag) {
        TRANSLITERATORS.getOrDefault(langTag, []).collectEntries {
            [it.getID(), it.transform(s)]
        }
    }

    static Set<String> romanizableLangTags() {
        TRANSLITERATORS.keySet().asUnmodifiable()
    }
    
    private static Transliterator romanizer(String id, List<String> filenames) {
        Transliterator.createFromRules(id, filenames.collect(Romanizer::readFromResources).join('\n'), Transliterator.FORWARD)
    }
    
    private static Map<String, List<Transliterator>> alaLocNonSlavicCyrillic() {
        ALA_LOC_NON_SLAVIC_CYRILLIC.collectEntries { tag ->
            def from = "$tag-Cyrl".toString()
            def to = "${tag}-Latn-t-${tag}-Cyrl-m0-alaloc"
            // TODO: Check if any of them are always is cyrillic?
            [ (from) : [romanizer(to, ['loc/und-Latn-t-und-Cyrl-m0-alaloc.txt'])]]
        }
    }

    private static String readFromResources(String filename) {
        Romanizer.class.getClassLoader().getResourceAsStream('romanizer/' + filename).getText("UTF-8")
    }
}
