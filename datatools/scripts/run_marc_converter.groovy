import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*

def path = args[0]
def slug = (path =~ /\/(\w+\/\w+)\.json$/)[0][1]
def recordType = args[2]
def id = "tag:data.kb.se,2012:/${slug}"
def source = new BasicDocument().withIdentifier(id).withData(new File(path).text)

def conv = new MarcCrackerIndexFormatConverter(recordType)
def result = conv.convert(source)

println groovy.json.JsonOutput.prettyPrint(result.dataAsString)
