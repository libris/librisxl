import java.util.stream.Collectors
import java.util.regex.Pattern

import whelk.Whelk
import static whelk.util.Jackson.mapper

interface UsingJsonKeys {
  static final var ID = '@id'
  static final var TYPE = '@type'
  static final var VALUE = '@value'
  static final var ANNOTATION = "@annotation"
}

class DefinitionsData implements UsingJsonKeys {

  static final var MARC = "https://id.kb.se/marc/"
  static final var SAOGF = "https://id.kb.se/term/saogf/"
  static final var BARNGF = "https://id.kb.se/term/barngf/"
  static final var KBRDA = "https://id.kb.se/term/rda/"

  Map typeToContentType = [:]
  Map genreFormImpliesFormMap = [:]
  Map carrierMediaMap = [:]

  Map complexTypeMap = [:]
  Map impliedContentTypes = [:]

  // Mixes subclasses and subconcepts
  Map<String, Set<String>> baseEqualOrSubMap = [ // matchesMap
    (MARC + 'DirectElectronic'): [MARC + 'ChipCartridge'] as Set,
    (MARC + 'Novel'): [SAOGF + 'Romaner'] as Set,
    'Audio': ['PerformedMusic', 'SpokenWord'] as Set,
  ]

  Map<String, String> contentToTypeMap = [:]

  DefinitionsData(File scriptDir) {
    // FIXME: Process type and gf definitions loaded into XL and remove these hard-coded mappings!
    var f = new File(scriptDir, "mappings.json")
    Map mappings = mapper.readValue(f, Map)
    typeToContentType = mappings.get('typeToContentType')
    genreFormImpliesFormMap = mappings.get('genreFormImpliesFormMap')
    carrierMediaMap = mappings.get('carrierMediaMap')

    impliedContentTypes = mappings.get('impliedContentTypes')
    complexTypeMap = mappings.get('complexTypeMap')

    typeToContentType.each { rtype, ctype ->
      if (ctype instanceof Map) {
        // FIXME: guessing!
        ctype = ctype['unionOf'][0]
      }
      contentToTypeMap[ctype] = rtype
    }
  }

  boolean matches(ArrayList<Map> typelikes, String value) {
    /* TODO:
    if (termKey in ld.vocabIndex) {
        return ld.getSubClasses(termKey).findResults{ ld.toTermId(it) }
    } else {
        return whelk.relations.followReverseBroader(value).collect()
    }
    */
    for (Map ref : typelikes) {
      if ((ref.containsKey(ID) && ref.get(ID).contains(value))) {
        return true
      }
    }
    return false
  }

  List reduceSymbols(List symbols) {
    return reduceSymbols(symbols, null)
  }

  List reduceSymbols(List symbols, String expectedtype) {
    List mostSpecific = symbols.stream().filter(x -> !(symbols.stream().anyMatch(y -> isbaseof(x, y)))).collect(Collectors.toList())
    return mostSpecific
  }

  boolean isbaseof(Object x, Object y) {
    if (x instanceof Map && y instanceof Map) {
      var subterms = baseEqualOrSubMap[x[ID]]
      if (subterms) {
        return y[ID] in subterms
      }
    }
    return false
  }

}

class TypeMappings extends DefinitionsData {

  TypeMappings(File scriptDir) { super(scriptDir) }

  boolean fixMarcLegacyType(Map instance, Map work) {
    var changed = false

    var itype = (String) instance[TYPE]

    if (itype == 'Map') {
      if (work[TYPE] == 'Cartography') {
        instance[TYPE] = 'Instance'
        // TODO: *may* be digital (online); *maybe* not a 2d-map... (a few Volume, a few Object)?
        if (!instance['carrierType']) {
          instance['carrierType'] = [ [(ID): "${MARC}Sheet" as String] ]
        }
        work[TYPE] = 'SingleMap'
        // TODO: drop instance['genreForm'] marc:MapATwoDimensionalMap, add kbgf:Map ?
        changed = true
      }
    } else if (itype == 'Globe') {
      if (work[TYPE] == 'Cartography') {
        instance[TYPE] = 'Object'
        work[TYPE] = 'Globe'
        changed = true
      }
    }

    def cleanupTypes =  [
      'ProjectedImageInstance': ['ProjectedImage'],
      'MovingImageInstance': ['MovingImage'],
      'KitInstance': ['Kit'],
      'NotatedMusicInstance': ['NotatedMusic'],
      'TextInstance': ['Text', ['Volume', 'Electronic']],
      'StillImageInstance': ['StillImage', ['Sheet', 'DigitalResource']],
      // TODO: seeAbove 'Map': ['SingleMap', ['Sheet', 'DigitalResource']],
      'GlobeInstance': ['Globe', 'Object'],
    ]

    var mappedTypes = cleanupTypes[itype]
    if (mappedTypes) {
      work[TYPE] = mappedTypes[0]
      if (false && mappedTypes.size() > 1) {
        // FIXME: unless implied! (Not even needed? Better types seem to be computed at least for test data...)
        if (mappedTypes[1] instanceof List) {
          // FIXME: check if non-electronic before assuming so?
          // instance[TYPE] = mappedTypes[1][0]
        } else {
          assert mappedTypes[1] instanceof String
          instance[TYPE] = mappedTypes[1]
        }
      } else { // failed...
        instance[TYPE] = 'Instance'
      }
      changed = true
    }

    return changed
  }

  boolean convertIssuanceType(Map instance, Map work) {
    // TODO: check genres and heuristics (some Serial are mistyped!)
    var issuancetype = (String) instance.remove("issuanceType")
    if (!issuancetype) {
      return false
    }

    if (issuancetype == 'SubCollection') {
      issuancetype = 'Collection'
    }

    if (issuancetype == 'Monograph') {
      instance['issuanceType'] = 'SingleUnit'
      return true
    } else if (issuancetype == 'ComponentPart') {
      // FIXME: or remove and add "isPartOf": {"@type": "Resource"} unless  implied?
      // instance[TYPE] += issuancetype
      instance['issuanceType'] = 'SingleUnit'
      return true
    } else {
      if ('contentType' in work) {
        // assert work['contentType'] == instance.get(TYPE)
        // assert work[TYPE] == issuancetype
      } else {
        work['contentType'] = []
      }

      def wtype = work.get(TYPE)

      if (wtype == 'Manuscript') {
        work.get('genreForm', []) << [(ID): SAOGF + 'Handskrifter']
      } else if (wtype == 'Cartography') {
        if ( ! work['contentType'].any { it[ID].startsWith(KBRDA + 'Cartographic') } ) {
          work['contentType'] << [(ID): KBRDA + 'CartographicImage'] // TODO: good enough guess?
        }
      } else if (wtype == 'MixedMaterial') {
        // FIXME:? assert work['contentType'].size() > 1
      } else {
        def mappedContentType = typeToContentType[wtype]
        if (mappedContentType instanceof Map) {
          // FIXME: guessing!
          mappedContentType = mappedContentType['unionOf'][0]
        }
        assert mappedContentType, "Unable to map ${wtype} to contentType or genreForm"
        work['contentType'] << [(ID): mappedContentType]
      }

      work[TYPE] = issuancetype
      instance['issuanceType'] = 'MultipleUnits'
      // TODO:
      // Or move instance[TYPE] to instance['carrierType'] unless implied, and set instance[TYPE] = 'Multipart'?
      // Examples (e.g. "Samling av trycksaker"):
      // select ?crt1 ?crt2 (count(?g) as ?count)  (sample(?g) as ?sample) {
      //  graph ?g { ?s kbv:carrierType ?crt1, ?crt2 . FILTER(isIRI(?crt1) && isIRI(?crt2) && ?crt1 > ?crt2) }
      //  } order by desc(?count)
      return true
    }
  }

}

class TypeNormalizer implements UsingJsonKeys {

  Whelk whelk
  TypeMappings mappings

  TypeNormalizer(Whelk whelk, File scriptDir) {
    this.whelk = whelk
    mappings = new TypeMappings(scriptDir)
  }

  boolean normalize(Map instance, Map work) {
    var changed = false

    changed |= mappings.fixMarcLegacyType(instance, work)

    changed |= foldTypeOfSingleUnitWork(work)
    changed |= foldTypeOfSingleUnitInstance(instance)

    if (instance.containsKey("issuanceType")) {
      // work.genreForm marc:MapBoundAsPartOfAnotherWork
      changed |= mappings.convertIssuanceType(instance, work)
    }

    if (instance['issuanceType'] == 'MultipleUnits') {
      // TODO: Or always do this, and move itype to ... carrierType? genreForm on instance?
      if (instance[TYPE] == 'Instance') {
        instance.put(TYPE, "Archival")  // FIXME: not necessarily; can we have a basic "MultipleObjects"...?
      }
    }

    return changed
  }

  boolean foldTypeOfSingleUnitWork(Map work) {
    var refSize = work.containsKey(ANNOTATION) ? 2 : 1
    if (work.containsKey(ID) && work.size() == refSize) {
      return false
    }

    var changed = false

    var worktype = (String) work.get(TYPE)

    var contenttypes = (List) mappings.reduceSymbols(asList(work.get("contentType")))
    if (contenttypes.size() == 1) {
      String ctypeid = contenttypes.get(0).get(ID)
      // TODO: change this. This currently drops contenttypes if implied by wtype;
      // but we need to map wrype+contenttype combo to a name, both ways (store both for search?)...
      var contentbasictype = mappings.contentToTypeMap[ctypeid]
      if (contentbasictype &&
        (contentbasictype == worktype || contentbasictype in mappings.baseEqualOrSubMap[worktype])
      ) {
        if (contentbasictype != worktype) {
          work.put(TYPE, contentbasictype)
          changed = true
        }
        work.remove("contentType")
        changed = true
      }
    }

    var genreforms = (List) mappings.reduceSymbols(asList(work.get("genreForm")))
    /* TODO: drop from picklist...?
    var gfPicklist = genreforms.stream().filter(
        (it) -> (it.containsKey(ID) &&
                 !(it.get(ID).startsWith(SAOGF) || it.get(ID).startsWith(BARNGF)))
      ).collect(Collectors.toList())
    */

    var formsToTypeMap = mappings.complexTypeMap.get(worktype)
    if (formsToTypeMap) {
      var possibleForm = genreforms.findResult { mappings.genreFormImpliesFormMap[it[ID]] }
      if (possibleForm) {
        var complexType = formsToTypeMap.get(possibleForm)
        if (complexType) {
          work.put(TYPE, complexType)
          var impliedCTs = mappings.impliedContentTypes[complexType] as Set
          if (impliedCTs && contenttypes.every { it[ID] in impliedCTs }) {
            work.remove("contentType")
          }
          changed = true
        }
      }
    }

    if (genreforms.removeIf { !it[ID] && it['prefLabel'] == 'DAISY'} ) {
      // TODO: assert isA(instance[TYPE], 'SoundRecording')
      work.put(TYPE, "AudioBook")
      changed = true
    }


    if (genreforms.size() > 0) {
      work.put("genreForm", genreforms)
      changed = true
    } else {
      work.remove("genreForm")
      changed = true
    }

    if (contenttypes.size() > 0 && work.containsKey("contentType")) {
      work.put("contentType", contenttypes)
      changed = true
    } else {
      work.remove("contentType")
      changed = true
    }

    return changed
  }

  boolean foldTypeOfSingleUnitInstance(Map instance) {
    var MARC = DefinitionsData.MARC
    var KBRDA = DefinitionsData.KBRDA

    var changed = false

    var itype = instance.get(TYPE)

    var mediatypes = mappings.reduceSymbols(asList(instance["mediaType"]), "MediaType")
    var carriertypes = mappings.reduceSymbols(asList(instance["carrierType"]), "CarrierType")

    var impliedMediaIds = carriertypes.findResults { mappings.carrierMediaMap[it[ID]] } as Set
    if (mediatypes.every { it[ID] in impliedMediaIds }) {
      instance.remove("mediaType")
      changed = true
    }

    var isElectronic = itype == "Electronic"
    var isSoundRecording = itype == "SoundRecording"
    var isVideoRecording = itype == "VideoRecording"
    var isTactile = itype == "Tactile"

    var isVolume = mappings.matches(carriertypes, "Volume") || looksLikeVolume(instance)

    if ((isElectronic && mappings.matches(carriertypes, "Online"))) {
      carriertypes = carriertypes.stream().filter((x) -> !x.getOrDefault(ID, "").contains("Online")).collect(Collectors.toList())
      if ((carriertypes.size() > 1 || !(mappings.matches(carriertypes, "Electronic")))) {
        instance.put("carrierType", carriertypes)
      } else {
        instance.remove("carrierType")
      }
      instance.put(TYPE, "DigitalResource")
      changed = true
    }

    if (isSoundRecording && dropReundantString(instance, "marc:mediaTerm", ~/(?i)ljudupptagning/)) {
      changed = true
    }

    if (isVideoRecording && dropReundantString(instance, "marc:mediaTerm", ~/(?i)videoupptagning/)) {
      changed = true
    }

    if (isTactile && dropReundantString(instance, "marc:mediaTerm", ~/(?i)punktskrift/)) {
      if (isVolume) {
        instance.put(TYPE, "BrailleVolume")
      } else {
        instance.put(TYPE, "BrailleResource")
      }
      changed = true
    }

    def tuples = [
      [isElectronic, ["ChipCartridge"], "ChipCartridge"],
      [isSoundRecording, ["${MARC}SoundDisc", "${KBRDA}AudioDisc"], "AudioDisc"],
      [isSoundRecording, ["${MARC}SoundCassette"], "AudioCassette"],
      [isVideoRecording, ["${MARC}VideoDisc", "${MARC}VideoMaterialType-d", "${KBRDA}Videodisc"], "VideoDisc"]
    ]
    for (tuple in tuples) {
      def (isIt, matchTokens, useType) = tuple
      List gotMatches = matchTokens.findAll { mappings.matches(carriertypes, it) }
      if (isIt && gotMatches.size() > 0) {
        isElectronic = true
        instance.put(TYPE, useType)
        changed = true
        if (carriertypes.size() == gotMatches.size()) {
          instance.remove("carrierType")
        }
      }
    }

    if (instance.get(TYPE) == "Instance") {
      if ((carriertypes.size() == 1 && mappings.matches(carriertypes, "Electronic"))) {
        isElectronic = true
        instance.put(TYPE, "Electronic")
        instance.remove("carrierType")
        changed = true
      } else if (isVolume) {
        instance.put(TYPE, "Volume")
        changed = true
      } else {
        /* ... */
      }
    }

    var probablyPrint = assumedToBePrint(instance)

    List instanceGfs = asList(instance.get("genreForm"))
    List reducedGfs = instanceGfs.stream().filter((it) -> !it[ID] != MARC + 'Print').collect(Collectors.toList())

    if (isElectronic) {

        if (dropReundantString(instance, "marc:mediaTerm", ~/(?i)elektronisk (resurs|utgåva)/)) {
          changed = true
        }

        if (reducedGfs.removeIf { it['prefLabel'] == 'E-böcker'}) {
          // TODO: assert isA(work[TYPE], 'Book')
          instance.put(TYPE, "EBook")
          changed = true
        }

    } else {

      if (itype == "Print" && isVolume) {
        instance.put(TYPE, "PrintedVolume")
        changed = true
      } else if (itype == "Instance") {
        if (isVolume) {
          if (probablyPrint) {
            instance.put(TYPE, "PrintedVolume")
            changed = true
            // TODO: if marc:RegularPrintReproduction, add production a Reproduction?
          } else {
            instance.put(TYPE, "Volume")
            changed = true
          }
        } else {
          if (probablyPrint) {
            if (mappings.matches(carriertypes, "Sheet")) {
              instance.put(TYPE, "PrintedSheet")
            } else {
              instance.put(TYPE, "Print") // TODO: may be PartOfPrint ?
            }
            changed = true
          } else {
            if (mappings.matches(carriertypes, "Sheet")) {
              instance.put(TYPE, "Sheet")
              if (dropReundantString(instance, "marc:mediaTerm", ~/(?i)affisch/)) {
                // TODO: work.genreForm << [(ID): SAOGF + 'Poster']? (and work[TYPE] = IllustratedWork | StillImage?)
              }
              changed = true
            } else {
              // instance.put(TYPE, "Monograph") // TODO: do better! (Physical?)
            }
          }
        }
      }

      if (carriertypes.size() == 1) {
        instance.remove("carrierType")
        changed = true
      }

    }

    if (reducedGfs.size() == 0) {
      instance.remove("genreForm")
      changed = true
    } else {
      instance.put("genreForm", reducedGfs)
      changed = true
    }

    var toDrop = [KBRDA + "Volume", MARC + "Braille", MARC + "TacMaterialType-b"] as Set
    carriertypes = carriertypes.findAll { !toDrop.contains(it.get(ID)) }
    if (carriertypes == null) {
      instance.remove("carrierType")
    } else {
      instance.put("carrierType", carriertypes)
    }

    return changed
  }

  static boolean dropReundantString(Map instance, String propertyKey, Pattern pattern) {
    if (instance.get(propertyKey)?.matches(pattern)) {
      instance.remove(propertyKey)
      return true
    }
    return false
  }

  static boolean assumedToBePrint(Map instance) {
    // TODO: carrierType == marc:RegularPrint || marc:RegularPrintReproduction
    // TODO: this is added to AudioCD:s etc.!
    return (instance.get("identifiedBy").stream().anyMatch(
        x -> x.get(TYPE).equals("ISBN"))
      ) || instance.containsKey("publication")
  }

  static boolean looksLikeVolume(Map instance) {
    for (extent in asList(instance.get("extent"))) {
      for (label in asList(extent.get("label"))) {
        if (label instanceof String && label.matches(/(?i).*\s(s|p|sidor|pages|vol(ym(er)?)?)\b\.?/)) {
          return true
        }
      }
    }
    return false
  }

  static List asList(Object o) {
    return (o instanceof List ? (List) o : o == null ? [] : [o])
  }

}

// NOTE: Since instance and work types may co-depend; fetch work and normalize
// that in tandem. We store work ids in memory to avoid converting again.
// TODO: Instead, normalize linked works first, then instances w/o linked works?
convertedWorks = java.util.concurrent.ConcurrentHashMap.newKeySet()

typeNormalizer = new TypeNormalizer(getWhelk(), scriptDir)

process { def doc, Closure loadWorkItem ->
  def (record, instance) = doc.graph

  if ('instanceOf' in instance) {
    if (ID !in instance.instanceOf) {
      def work = instance.instanceOf
      if (work instanceof List && work.size() == 1) {
        work = work[0]
      }

      var changed = typeNormalizer.normalize(instance, work)

      if (changed) doc.scheduleSave()
    } else {
      def loadedWorkId = instance.instanceOf[ID]
      // TODO: refactor very hacky solution...
      loadWorkItem(loadedWorkId) { workIt ->
        def (workRecord, work) = workIt.graph

        var changed = typeNormalizer.normalize(instance, work)

        if (changed) {
          doc.scheduleSave()
          if (loadedWorkId !in convertedWorks) workIt.scheduleSave()
        }
        convertedWorks << loadedWorkId
      }
    }

  }
}
