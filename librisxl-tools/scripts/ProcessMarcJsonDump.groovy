import whelk.converter.marc.MarcFrameConverter

def sourceDump = args[0]
def destDump = args[1]

def converter = new MarcFrameConverter()
def mapper = converter.mapper

new File(destDump).withPrintWriter('UTF-8') { out ->
    def i = 0
    long startTime = System.currentTimeMillis()
    def tsv = sourceDump.endsWith('.tsv')
    new File(sourceDump).eachLine('UTF-8') {
        def tab1 = it.indexOf('\t')
        def data = tsv? it.substring(tab1, it.indexOf('\t', tab1 + 1)) : it
        def source = mapper.readValue(data, Map)
        def result = null
        try {
            result = converter.runConvert(source, source['_extra'])
        } catch (Exception e) {
            println "ERROR in data:"
            println it
            throw e
        }
        def outdata = mapper.writeValueAsString(result)
        out.println(outdata)


        long elapsed = (System.currentTimeMillis() - startTime) / 1000
        if (i++ && i % 100 == 0) {
            println "Processed ${i} docs (${i / (elapsed ?: 1)} docs / s)"
        }
    }
}
