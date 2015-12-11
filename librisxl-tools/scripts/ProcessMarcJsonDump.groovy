import whelk.converter.marc.MarcFrameConverter

def sourceDump = args[0]
def destDump = args[1]

def converter = new MarcFrameConverter()
def mapper = converter.mapper

new File(destDump).withPrintWriter('UTF-8') { out ->
    def i = 0
    new File(sourceDump).eachLine('UTF-8') {
        def source = mapper.readValue(it, Map)
        def result = null
        try {
            result = converter.runConvert(source, source['_extra'])
        } catch (Exception e) {
            println "ERROR in data:"
            println it
            throw e
        }
        out.println(mapper.writeValueAsString(result))
        println "Processed ${i++} lines"
    }
}
