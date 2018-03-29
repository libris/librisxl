package whelk.converter.marc

import whelk.JsonLd
import whelk.component.PostgreSQLComponent
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

for (fpath in fpaths) {
    def source = converter.mapper.readValue(new File(fpath), Map)
    def result = null

    if (cmd == "revert") {
        addJsonLd(converter)
        result = converter.runRevert(source)
    } else {
        def extraData = null
        if (source.oaipmhSetSpecs) {
            addLinkFinder(converter)
            extraData= [oaipmhSetSpecs: source.remove('oaipmhSetSpecs')]
        } else {
            addJsonLd(converter)
        }
        result = converter.runConvert(source, fpath, extraData)
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

def addLinkFinder(converter) {
    if (converter.linkFinder)
        return
    def whelkname = "whelk_dev"
    def pgsql = whelkname ?  new PostgreSQLComponent("jdbc:postgresql:$whelkname", "lddb"): null
    def linkFinder = pgsql ? new LinkFinder(pgsql) : null
    converter.linkFinder = linkFinder
    // TODO: add JsonLd with data from psql!
}

static void addJsonLd(converter) {
    if (converter.ld)
        return

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
