package whelk.converter.marc

import java.nio.file.Files
import java.nio.file.Paths

import whelk.Document
import whelk.JsonLd
import whelk.JsonLdValidator
import whelk.JsonLdValidator.Error
import whelk.ResourceCache
import whelk.TypeCategoryNormalizer
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

if (cmd == "cachebytype") {
  var whelk = Whelk.createLoadedSearchWhelk()
  var start = new Date().time
  for (type in ['Category', 'marc:EnumeratedTerm']) {
    System.err.println "getByType: ${type}"
    whelk.resourceCache.getByType(type)
  }
  System.err.println("took ${new Date().time - start} ms")
  var resourceCacheByTypeFile = new File(fpaths[0])
  mapper.writerWithDefaultPrettyPrinter().writeValue(resourceCacheByTypeFile, whelk.resourceCache.byTypeCache)
  return
}

var doValidate = !perf
var prettyMarc = false

List items = fpaths

if (cmd.endsWith("revert-templates")) {
  cmd = cmd.replace('-templates', '')
  doValidate = false
  def templates = mapper.readValue(new File(fpaths[0]), Map)
  def only = fpaths.size() > 1 ? fpaths[1..-1] as Set : null
  items = templates.instance.findResults { name, tplt ->
    if (only && name !in only) return
    def data = tplt.value.record
    data.mainEntity = tplt.value.mainEntity
    return [ id: name, data: data ]
  }
}

if (cmd.endsWith("revert-lines")) {
  cmd = cmd.replace('-lines', '')
  doValidate = false
  items = Files.lines(Paths.get(fpaths[0])).map(line -> {
    def data = mapper.readValue(line, Map)
    def id = data['@graph'][0]['@id']
    return [ id: id, data: data ]
  })
}

if (cmd.startsWith("pretty-revert")) {
  cmd = cmd.replace('pretty-', '')
  prettyMarc = true
}

var converter = newMarcFrameConverter()

if (cmd == "save-typemappings") {
  var catTypeNormalizer = new TypeCategoryNormalizer(converter.resourceCache)

  var outfile = new File(fpaths[0])
  var mappings = [
      typeToCategory: catTypeNormalizer.typeToCategory,
      preferredCategory: catTypeNormalizer.preferredCategory,
      categoryMatches: catTypeNormalizer.categoryMatches,
  ]
  mapper.writerWithDefaultPrettyPrinter().writeValue(outfile, mappings)
  return
}

if (perf) {
    fpaths = fpaths * perf
    System.err.println "Measuring performance of ${fpaths.size()} ${cmd} runs..."
}

var start = new Date().time

for (item in items) {
    def source
    def sourceId
    if (item instanceof String) {
      source = mapper.readValue(new File(item), Map)
      sourceId = item
    } else {
      source = item.data
      sourceId = item.id
    }

    def result = null

    if (cmd == "revert") {
        if (doValidate && converter.ld) {
            reportValidation(converter, source)
        }
        result = converter.runRevert(source)
    } else if (cmd == "pre-revert") {
        source = converter.getRevertForm(source)
        var marcRuleSet = converter.conversion.getRuleSetFromJsonLd(source)
        converter.conversion.unmodifyData(marcRuleSet, source)
        result = source
    } else {
        Map extraData = null
        if (source.oaipmhSetSpecs) {
            extraData = [oaipmhSetSpecs: source.remove('oaipmhSetSpecs')]
        }

        if (cmd == "pre-convert") {
            System.err.println(cmd.toUpperCase() + ':')
            converter.conversion.marcRuleSets['bib'].postProcSteps = []
        }

        result = converter.runConvert(source, sourceId, extraData)
        if (converter.linkFinder) {
            var doc = new Document(result)
            converter.linkFinder.normalizeIdentifiers(doc)
            result = doc.data
        }
    }

    def s = null
    if (prettyMarc) {
      s = toPrettyMarc(result)
    } else {
      try {
          s = converter.mapper.writeValueAsString(result)
      } catch (e) {
          System.err.println "Error in result:"
          System.err.println result
          throw e
      }
    }

    if (!perf) {
        if (items.size() > 1) println "SOURCE: ${sourceId}"
        println s
    }
}

if (perf) {
    def stop = new Date().time - start
    System.err.println "Performance for ${perf} ${cmd} runs: ${stop / perf} ms on average"
}


static MarcFrameConverter newMarcFrameConverter() {
    if (System.properties.getProperty('xl.secret.properties')) {
        def whelk = Whelk.createLoadedCoreWhelk()
        return whelk.getMarcFrameConverter()
    }

    def defsBuildDir = System.properties.get('xl.definitions.builddir')?: '../../definitions/build'
    System.err.println("NOTE: using xl.definitions.builddir static files: ${defsBuildDir}")
    def jsonld = getLocalJsonLd(defsBuildDir)

    def resourceCache = new ResourceCache(jsonld)
    String resourceCacheDir = System.properties.get('xl.resourcecache.dir')
    System.err.println("NOTE: using xl.resourcecache.dir static files: ${resourceCacheDir}")
    if (resourceCacheDir != null) {
      var byTypeCacheFile = new File("${resourceCacheDir}/typecache.json")
      resourceCache.byTypeCache = mapper.readValue(byTypeCacheFile, Map)
    }

    return new MarcFrameConverter(null, jsonld, resourceCache)
}

static JsonLd getLocalJsonLd(defsBuildDir) {
    def contextFile = new File("$defsBuildDir/sys/context/kbv.jsonld")
    assert contextFile.exists(), "Misssing context file: ${contextFile}"

    def vocabFile = new File("$defsBuildDir/vocab.jsonld")
    assert vocabFile.exists(), "Misssing vocab file: ${vocabFile}"

    def displayFile = new File("$defsBuildDir/vocab/display.jsonld")
    assert displayFile.exists(), "Missing display file: ${displayFile}"

    def contextData = mapper.readValue(contextFile, Map)
    def displayData = mapper.readValue(displayFile, Map)
    def vocabData = mapper.readValue(vocabFile, Map)
    def locales = ['sv', 'en']

    return new JsonLd(contextData, displayData, vocabData, locales)
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

static String toPrettyMarc(Map result) {
  var sb = new StringBuilder()
  sb << "000\t${result.leader}\n"
  for (field in result.fields) {
    field.each { tag, tv ->
      sb << "${tag}\t"
      if (tv instanceof Map) {
        sb << "${tv.ind1} "
        sb << "${tv.ind2}\t"
        tv.subfields.eachWithIndex { subf, i ->
          if (i) sb << "\t"
          subf.each { code, cv ->
            sb << "\$${code} ${cv}"
          }
        }
        sb << "\n"
      } else {
        sb << "${tv}\n"
      }
    }
  }
  return sb.toString()
}
