package whelk.filter

import whelk.util.DocumentUtil
import whelk.util.Statistics

class LanguageLinker extends BlankNodeLinker implements DocumentUtil.Linker {
    List ignoreCodes = []

    LanguageLinker(List ignoreCodes = [], Statistics stats = null) {
        super('Language', ['label', 'code', 'prefLabelByLang', 'altLabelByLang', 'hiddenLabelByLang', 'hiddenLabel', 'langCode', 'langCodeBib', 'langCodeFull', 'langCodeTerm'], stats)
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
        String code = definition['code'].toLowerCase()
        if (ignoreCodes.contains(code)) {
            return
        }

        super.addDefinition(definition)
    }

    @Override
    protected List split(labelOrCode) {
        if (labelOrCode instanceof List) {
            return labelOrCode
        }

        // concatenated language labels, e.g. "Svenska & engelska"
        if (labelOrCode ==~ /^(.*,)*.*( & | och | and ).*/) {
            return labelOrCode.split(/,| & | och | and /) as List
        }

        // concatenated language codes, e.g "sweruseng", "swe ; rus ; eng"
        if (labelOrCode ==~ /^(\w{3}\W*){2,}/) {
            def m = labelOrCode =~ /(\w{3})\W*/
            def matches = []
            while (m.find()) {
                matches << m.group(1)
            }
            return matches
        }

        return []
    }
}