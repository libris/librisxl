package whelk.converter.marc

import whelk.Document
import whelk.JsonLd
import whelk.JsonLdValidator
import whelk.JsonLdValidator.Error
import whelk.Whelk
import whelk.filter.LinkFinder

import static whelk.util.Jackson.mapper

def cmd = "convert"
def perf = 0

List fpaths = args[0..-1]

if (fpaths[0] ==~ /\d+/) {
    perf = fpaths[0] as int
    fpaths = fpaths[1..-1]
}

if (fpaths.size() > 1) {
    cmd = fpaths[0]
    fpaths = fpaths[1..-1]
}

if (perf) {
    fpaths = fpaths * perf
    System.err.println "Measuring performance of ${fpaths.size()} ${cmd} runs..."
}

def converter = new MarcFrameConverter()
addSystemComponents(converter)

def start = new Date().time

for (fpath in fpaths) {
    def source = converter.mapper.readValue(new File(fpath), Map)
    def result = null

    if (cmd == "revert") {
        if (!perf && converter.ld) {
            reportValidation(converter, source)
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

    def s = null
    try {
        s = converter.mapper.writeValueAsString(result)
    } catch (e) {
        System.err.println "Error in result:"
        System.err.println result
        throw e
    }

    if (!perf) {
        if (fpaths.size() > 1) println "SOURCE: ${fpath}"
        println s
    }
}

if (perf) {
    def stop = new Date().time - start
    System.err.println "Performance for ${perf} ${cmd} runs: ${stop / perf} ms on average"
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

    def contextFile = new File("$defsbuild/sys/context/kbv.jsonld")
    assert contextFile.exists(), "Misssing context file: ${contextFile}"

    def vocabFile = new File("$defsbuild/vocab.jsonld")
    assert vocabFile.exists(), "Misssing vocab file: ${vocabFile}"

    def displayFile = new File("$defsbuild/vocab/display.jsonld")
    assert displayFile.exists(), "Missing display file: ${displayFile}"

    def contextData = mapper.readValue(contextFile, Map)
    def displayData = mapper.readValue(displayFile, Map)
    def vocabData = mapper.readValue(vocabFile, Map)
    def locales = ['sv', 'en']

    converter.ld = new JsonLd(contextData, displayData, vocabData, locales)
}

static void reportValidation(converter, source) {
    System.err.println "Validating JSON-LD ..."
    def validator = JsonLdValidator.from(converter.ld)
    List<Error> errors = validator.validateAll(source)
    if (errors) {
        System.err.println "JSON-LD validation errors:"
        errors.each{
            System.err.println it.toStringWithPath()
        }
    } else {
        System.err.println "OK"
    }
}
