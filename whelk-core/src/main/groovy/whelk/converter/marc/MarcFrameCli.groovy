package whelk.converter.marc

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.filter.LinkFinder


List fpaths
def cmd = "convert"

if (args.length > 1) {
    cmd = args[0]
    fpaths = args[1..-1]
} else {
    fpaths = args[0..-1]
}

def converter = new MarcFrameConverter()
addSystemComponents(converter)

for (fpath in fpaths) {
    def source = converter.mapper.readValue(new File(fpath), Map)
    def result = null

    if (cmd == "revert") {
        if (converter.ld) {
            System.err.println "Validating JSON-LD ..."
            def errors = converter.ld.validate(source)
            if (errors) {
                System.err.println "JSON-LD validation errors:"
                errors.each System.err.&println
            } else {
                System.err.println "OK"
            }
        }
        result = converter.runRevert(source)
    } else {
        def extraData = null
        if (source.oaipmhSetSpecs) {
            extraData = [oaipmhSetSpecs: source.remove('oaipmhSetSpecs')]
        }
        result = converter.runConvert(source, fpath, extraData)
        if (converter.linkFinder) {
            def doc = new Document(result)
            converter.linkFinder.normalizeIdentifiers(doc)
            result = doc.data
        }
    }

    if (fpaths.size() > 1)
        println "SOURCE: ${fpath}"
    try {
        println converter.mapper.writeValueAsString(result)
    } catch (e) {
        System.err.println "Error in result:"
        System.err.println result
        throw e
    }
}

void addSystemComponents(converter) {
    if ('xl.secret.properties' in System.properties) {
        def whelk = Whelk.createLoadedCoreWhelk()
        converter.linkFinder = new LinkFinder(whelk.storage)
        converter.ld = whelk.jsonld
    } else {
        addJsonLd(converter)
    }
}

static void addJsonLd(converter) {
    def defsbuild = System.env.defsbuild ?: '../../definitions/build'

    def contextFile = new File("$defsbuild/vocab/context.jsonld")
    assert contextFile.exists(), "Misssing context file: ${contextFile}"

    def vocabFile = new File("$defsbuild/vocab.jsonld")
    assert vocabFile.exists(), "Misssing vocab file: ${vocabFile}"

    def displayFile = new File("$defsbuild/vocab/display.jsonld")
    assert displayFile.exists(), "Missing display file: ${displayFile}"

    def contextData = converter.mapper.readValue(contextFile, Map)
    def displayData = converter.mapper.readValue(displayFile, Map)
    def vocabData = converter.mapper.readValue(vocabFile, Map)
    converter.ld = new JsonLd(contextData, displayData, vocabData)
}
