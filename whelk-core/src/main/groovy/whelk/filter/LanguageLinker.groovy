package whelk.filter

import groovy.transform.CompileStatic
import whelk.util.DocumentUtil
import whelk.util.Statistics

@CompileStatic
class LanguageLinker extends BlankNodeLinker implements DocumentUtil.Linker {
    List ignoreCodes = []

    LanguageLinker(List ignoreCodes = [], Statistics stats = null) {
        super('Language', ['label', 'labelByLang', 'code', 'prefLabelByLang', 'altLabelByLang', 'hiddenLabelByLang', 'hiddenLabel', 'langCode', 'langCodeBib', 'langCodeFull', 'langCodeTerm', 'langTag'], stats)
        this.ignoreCodes = ignoreCodes
    }

    boolean linkLanguages(data, List<Map> disambiguationNodes = [], String key = 'language') {
        return DocumentUtil.findKey(data, key, DocumentUtil.link(this, disambiguationNodes))
    }

    //@Override
    boolean linkAll(data, String key = 'language') {
        super.linkAll(data, key)
    }

    @Override
    void addDefinition(Map definition) {
        String code = ((String) definition['code']).toLowerCase()
        if (ignoreCodes.contains(code)) {
            return
        }

        super.addDefinition(definition)
    }

    @Override
    protected List split(Object labelOrCode) {
        if (labelOrCode instanceof List) {
            return (List) labelOrCode
        }

        if (!(labelOrCode instanceof String)) {
            return []
        }
        String s = (String) labelOrCode

        // concatenated language labels, e.g. "Svenska & engelska"
        if (s ==~ /^(.*,)*.*( & | och | and ).*/) {
            return s.split(/,| & | och | and /) as List
        }

        // concatenated language codes, e.g "sweruseng", "swe ; rus ; eng"
        if (s ==~ /^(\w{3}\W*){2,}/) {
            def m = s =~ /(\w{3})\W*/
            List matches = []
            while (m.find()) {
                matches << m.group(1)
            }
            return matches
        }

        return []
    }
}