package whelk.converter.marc

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

def addLinkFinder(converter) {
    if (converter.linkFinder)
        return
    def whelkname = "whelk_dev"
    def pgsql = whelkname ?  new PostgreSQLComponent("jdbc:postgresql:$whelkname", "lddb"): null
    def linkFinder = pgsql ? new LinkFinder(pgsql) : null
    converter.linkFinder = linkFinder
}

for (fpath in fpaths) {
    def source = converter.mapper.readValue(new File(fpath), Map)
    def result = null
    if (cmd == "revert") {
        result = converter.runRevert(source)
    } else {
        def extraData = null
        if (source.oaipmhSetSpecs) {
            addLinkFinder(converter)
            extraData= [oaipmhSetSpecs: source.remove('oaipmhSetSpecs')]
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
