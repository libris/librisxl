import static whelk.converter.marc.Util.asList
import whelk.util.DocumentUtil
import whelk.util.Statistics


BIB880 = 'marc:hasBib880'
FIELDREF = 'marc:fieldref'
FIELDREFS = [FIELDREF, 'marc:bib035-fieldref', 'marc:bib041-fieldref',
             'marc:bib250-fieldref', 'marc:hold035-fieldref']

IGNORE_KEYS = [TYPE, 'year', 'endYear', 'startYear', 'date', 'originDate',
               'lifeSpan', 'code', 'value', 'version', 'acquisitionTerms',
               'editionStatementRemainder',
               'marc:version', 'marc:publicationDistributionEtcOfOriginal',
               'marc:otherPhysicalDetails', 'marc:nonfilingChars',
               'marc:systemDetailsNote', 'marc:assigningSource',
               'marc:sourceOfInformation', 'marc:introductoryPhrase',
               'marc:numeration', 'marc:titlesAndOtherWordsAssociatedWithAName',
               'marc:publicNote', 'marc:formSubheading',
               'marc:mediaTerm'] as Set


broken880Log = getReportWriter("broken-880s")

stats = new Statistics()
Runtime.getRuntime().addShutdownHook {
    def out = getReportWriter("stats")
    stats.print((Object) out)
    out.flush()
}

whelk = null

/* Edge Cases * /
selectByIds(['x7pd48f3v94zgtvf', 'bvnpkwtn09v6svk', '3mfhp21f5gfh5n0',
             '9tm0qsvm2dx8kj4', 's3gxks22qvcgs741', '09pb67nxxbbnndt2'])
/* Main */
selectBySqlWhere("""
data#>>'{@graph,1,$BIB880}' NOTNULL
AND collection = 'bib'
""") // about 33451 records
/**/ { data ->
    whelk = data.whelk
    def (record, instance, work) = data.graph

    work = work ?: instance.instanceOf
    if (work.size() == 1 && work[ID]) {
        // TODO: if this ever happens; process linked work here to handle the
        // case where any 880:s in this instance refer to work details!
        assert false, "Problem: work linked from ${instance[ID]} may need bib 880 data from instance!"
    }

    Map bib880Map = asList(instance.remove(BIB880)).withIndex().collectEntries { map, i ->
        ["880-${i < 9 ? '0' : ''}${i + 1}" as String, map]
    }
    boolean changed = false
    Closure handle880Ref = { ref, path ->
        def bib880 = bib880Map[ref]
        def marcJson = null
        try {
            marcJson = bib880 instanceof Map ? bib880ToMarcJson(bib880) : bib880
        } catch (Exception e) {
            broken880Log.println(data.doc.shortId)
            return
        }
        def marc = [leader: "00887cam a2200277 a 4500", fields: [marcJson]]
        def mfconverter = data.whelk.marcFrameConverter
        def converted = mfconverter.runConvert(marc)[GRAPH][1..-1]

        def owner = data.graph
        for (p in path[0..-1]) owner = owner[p]

        if (mergeAltLanguage(converted, data.graph[1..-1], owner, work.language)) {
            changed = true
            return new DocumentUtil.Remove()
        } else {
            stats.increment('failed-paths', path.join('.'))
        }
    }
    FIELDREFS.each {
        DocumentUtil.findKey(data.graph, it, handle880Ref)
    }
    if (changed) {
        data.scheduleSave()
    }
}

def bib880ToMarcJson(Map bib880) {
    def parts = bib880['marc:partList']
    def tag = parts[0]['marc:fieldref'].split('-')[0]
    return [(tag): [
        ind1: bib880['marc:bib880-i1'],
        ind2: bib880['marc:bib880-i2'],
        subfields: parts[1..-1].collect {
            def subfields = it.collect { key, value ->
                [(key.replace('marc:bib880-', '')): value]
            }
            return subfields.size() == 1 ? subfields[0] : subfields
        }
    ]]
}

boolean mergeAltLanguage(converted, parent, owner, language) {
    def lang = language.findResult { it[ID] }?.split('/')?.getAt(-1)
        ?: language.findResult { it.code }?.trim()
        ?: 'und'
    // TODO: lookup shortest lang-code from our Language defs
    return addAltLang(parent, parent, owner, converted, lang)
}

boolean addAltLang(parent, existing, owner, data, lang, key = null) {
    if (data instanceof String) {
        if (data == existing || key in IGNORE_KEYS) {
            return true
        }
        if (parent instanceof List) parent = parent[0]
        if (parent instanceof Map) {
            if (ID in parent && parent.size() == 1) {
                stats.increment('linked-auths', parent[ID])
                return true
            }
            String keyByLang = whelk.jsonld.langContainerAlias[key]
            if (!keyByLang) {
                // TODO: never fall back and just *drop* 880?
                keyByLang = whelk.jsonld.langContainerAlias[key]
            }

            def byLang = parent.get(keyByLang, [:])

            def destScript = "Lat" // Assumed!
            def transLangTag = "$lang-$destScript-t-$lang"
            // TODO:
            // sysCode = sysCodByLang[lang]
            // + "-$origScript-m0-$sysCode"
            byLang[transLangTag] = parent[key] // TODO: parent.remove(key)

            byLang[lang] = data

            stats.increment('used-keys', keyByLang)
            if (!whelk.jsonld.vocabIndex.containsKey(keyByLang)) {
                stats.increment('unknown-keys', keyByLang)
            }
            return true
        }
    } else if (data instanceof List) {
        def ok = true
        int i = 0
        def selected = data.findAll { it == owner }
        if (selected.size() == 0) selected = data
        def iter = selected.iterator()
        for (it in iter) {
            def t = existing instanceof List ? existing[i++] : existing
            if (addAltLang(parent, t, owner, it, lang, key)) {
                iter.remove()
            } else {
                ok = false
            }
        }
        return ok
    } else if (data instanceof Map) {
        def ok = true
        def iter = data.iterator()
        for (it in iter) {
            if (it.key in existing) {
                if (addAltLang(existing, existing[it.key], owner, it.value, lang, it.key)) {
                    iter.remove()
                } else {
                    ok = false
                }
            }
        }
        return ok
    }
}
