package whelk.converter.marc


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
        result = converter.runRevert(source)
    } else {
        def extraData = null
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
