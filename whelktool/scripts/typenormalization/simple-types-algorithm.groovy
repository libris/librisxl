package typenormalization

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

  static final var KBGF = "https://id.kb.se/term/kbgf/"

  Whelk whelk

  // TODO: see also e.g. 'Map' and 'Globe' in fixMarcLegacyType (used only if wtype is Cartography)
  Map cleanupTypes =  [:]

  Map typeToContentType = [:]
  Map carrierMediaMap = [:]

  // TODO: Unused; COULD be used to add more concrete category (which MAY be required for revert?)
  Map genreFormImpliesFormMap = [:]
  Map complexTypeMap = [:]

  // Mixes subclasses and subconcepts
  Map<String, Set<String>> baseEqualOrSubMap = [ // matchesMap
    (MARC + 'DirectElectronic'): [MARC + 'ChipCartridge'] as Set,
    (MARC + 'Novel'): [SAOGF + 'Romaner'] as Set,
    'Audio': ['PerformedMusic', 'SpokenWord'] as Set,
  ]

  Map<String, String> contentToTypeMap = [:]

  DefinitionsData(Whelk whelk, File scriptDir) {
    // FIXME: Process type and gf definitions loaded into XL and remove these hard-coded mappings!
    var f = new File(scriptDir, "mappings.json")
    Map mappings = mapper.readValue(f, Map)

    cleanupTypes = mappings.get('cleanupTypes')

    typeToContentType = mappings.get('typeToContentType')
    carrierMediaMap = mappings.get('carrierMediaMap')

    genreFormImpliesFormMap = mappings.get('genreFormImpliesFormMap')
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
    for (Map ref : typelikes) {
      if ((ref.containsKey(ID) && ref.get(ID).contains(value))) {
        return true
      }
    }
    return false
  }

  List reduceSymbols(List symbols, String expectedType = null) {
    List mostSpecific = symbols.stream()
      .filter(x -> !(symbols.stream().anyMatch(y -> isbaseof(x, y))))
      .collect(Collectors.toList())
    return mostSpecific.toSet().toList()
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

  TypeMappings(Whelk whelk, File scriptDir) { super(whelk, scriptDir) }

  boolean fixMarcLegacyType(Map instance, Map work) {
    var changed = false

    var itype = (String) instance[TYPE]

    if (itype == 'Map') {
      if (work[TYPE] == 'Cartography') {
        instance[TYPE] = 'Instance'
        // TODO: *may* be digital (online); *maybe* not a 2d-map... (a few Volume, a few Object)?
        if (!instance['carrierType']) {
          instance['carrierType'] = [ [(ID): MARC + 'Sheet'] ]
        }
        work[TYPE] = 'CartographicImage' // MapImage
        // TODO: drop instance['genreForm'] marc:MapATwoDimensionalMap, add kbgf:Map ?
        changed = true
      }
    } else if (itype == 'Globe') {
      if (work[TYPE] == 'Cartography') {
        instance[TYPE] = 'Object'
        work[TYPE] = 'CartographicObject' // MapGlobe
        changed = true
      }
    }

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

    if (issuancetype == 'SerialComponentPart') {
      issuancetype = 'ComponentPart'
    }

    work[TYPE] = issuancetype

    if (issuancetype == 'Monograph' || issuancetype == 'Integrating') {
      instance['issuanceType'] = 'SingleUnit'
    } else if (issuancetype == 'ComponentPart') {
      // FIXME: or remove and add "isPartOf": {"@type": "Resource"} unless  implied?
      // instance[TYPE] += issuancetype
      instance['issuanceType'] = 'SingleUnit'
    } else {
      instance['issuanceType'] = 'MultipleUnits'
      // TODO:
      // Or instance[TYPE] += 'MultipleUnits'?
    }

    return true
  }

}

class TypeNormalizer implements UsingJsonKeys {

  static MARC = DefinitionsData.MARC
  static KBRDA = DefinitionsData.KBRDA
  static KBGF = DefinitionsData.KBGF

  TypeMappings mappings

  TypeNormalizer(TypeMappings mappings) {
    this.mappings = mappings
  }

  boolean normalize(Map instance, Map work) {
    var changed = false

    changed |= mappings.fixMarcLegacyType(instance, work)

    changed |= simplifyWorkType(work)
    changed |= simplfyInstanceType(instance)
    changed |= mappings.convertIssuanceType(instance, work)

    if (changed) {
      reorder(work)
      reorder(instance)
    }

    return changed
  }

  void reorder(Map thing) {
    var copy = thing.clone()
    thing.clear()
    for (var key : [ID, TYPE, 'sameAs', 'category', 'issuanceType']) {
      if (key in copy) thing[key] = copy[key]
    }
    thing.putAll(copy)
  }

  boolean simplifyWorkType(Map work) {
    var refSize = work.containsKey(ANNOTATION) ? 2 : 1
    if (work.containsKey(ID) && work.size() == refSize) {
      return false
    }

    var changed = false
    List<String> categories = []

    def wtype = work.get(TYPE)

    if (wtype == 'Manuscript') {
      work.get('genreForm', []) << [(ID): SAOGF + 'Handskrifter']
    } else if (wtype == 'Cartography') {
      if ( ! work['contentType'].any { it[ID].startsWith(KBRDA + 'Cartographic') } ) {
        work.get('contentType', []) << [(ID): KBRDA + 'CartographicImage'] // TODO: good enough guess?
      }
    } else if (wtype == 'MixedMaterial') {
    } else {
      def mappedContentType = mappings.typeToContentType[wtype]
      if (mappedContentType instanceof Map) {
        // FIXME: guessing! Use non-RDA category instead
        mappedContentType = mappedContentType['unionOf'][0]
      }
      // assert mappedContentType, "Unable to map ${wtype} to contentType or genreForm"
      if (mappedContentType) {
        work.get('contentType', []) << [(ID): mappedContentType]
      }
    }

    var contenttypes = mappings.reduceSymbols(asList(work.get("contentType")))
    var genreforms = mappings.reduceSymbols(asList(work.get("genreForm")))

    categories += genreforms + contenttypes

    if (genreforms.removeIf { !it[ID] && it['prefLabel'] == 'DAISY'} ) {
      categories << [(ID): KBGF + 'Audiobook']
    }

    if (categories.size() > 0) {
      work.put("category", mappings.reduceSymbols(categories))
      work.remove("genreForm")
      work.remove("contentType")
      changed = true
    }

    return changed
  }

  boolean simplfyInstanceType(Map instance) {
    var changed = false
    List<String> categories = []

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
      carriertypes = carriertypes.stream()
        .filter((x) -> !x.getOrDefault(ID, "").contains("Online"))
        .collect(Collectors.toList())
      if (carriertypes.size() == 0) {
        categories << [(ID): KBRDA + 'OnlineResource']
      }
      instance.put(TYPE, "DigitalResource")
      changed = true
    } else {
      instance.put(TYPE, "PhysicalResource")
      changed = true
    }

    if (isSoundRecording && dropReundantString(instance, "marc:mediaTerm", ~/(?i)ljudupptagning/)) {
      changed = true
    }

    if (isVideoRecording && dropReundantString(instance, "marc:mediaTerm", ~/(?i)videoupptagning/)) {
      changed = true
    }

    var isBraille = dropReundantString(instance, "marc:mediaTerm", ~/(?i)punktskrift/)

    var toDrop = [KBRDA + "Volume", MARC + "Braille", MARC + "TacMaterialType-b"] as Set
    var nonBrailleCarrierTypes = carriertypes.findAll { !toDrop.contains(it.get(ID)) }
    if (nonBrailleCarrierTypes.size() < carriertypes.size()) {
      isBraille = true
      carriertypes = nonBrailleCarrierTypes
    }

    categories += carriertypes

    if (isTactile && isBraille) {
      if (isVolume) {
        categories << [(ID): KBGF + 'BrailleVolume']
      } else {
        categories << [(ID): KBGF + 'BrailleResource']
      }
      changed = true
    }

    // TODO: should work with regular reduceSymbols ...
    def tuples = [
      [isElectronic, ["ChipCartridge"], "ChipCartridge"],
      [isSoundRecording, [MARC + 'SoundDisc', KBRDA + 'AudioDisc']],
      [isSoundRecording, [MARC + 'SoundCassette', KBGF + 'AudioCassette']],
      [isVideoRecording, [MARC + 'VideoDisc', "${MARC}VideoMaterialType-d", KBRDA + 'Videodisc']]
    ]
    for (tuple in tuples) {
      def (isIt, matchTokens) = tuple
      List gotMatches = matchTokens.findAll { mappings.matches(carriertypes, it) }
      if (isIt && gotMatches.size() > 0) {
        isElectronic = true
        categories << [(ID): matchTokens[-1]]
        changed = true
      }
    }

    if (instance.get(TYPE) == "Instance") {
      if ((carriertypes.size() == 1 && mappings.matches(carriertypes, "Electronic"))) {
        isElectronic = true
      }
    }

    var probablyPrint = assumedToBePrint(instance)

    List instanceGfs = asList(instance.get("genreForm"))
    List reducedGfs = instanceGfs.stream()
        .filter((it) -> !it[ID] != MARC + 'Print')
        .collect(Collectors.toList())

    if (isElectronic) {

        if (dropReundantString(instance, "marc:mediaTerm", ~/(?i)elektronisk (resurs|utgåva)/)) {
          changed = true
        }

        if (reducedGfs.removeIf { it['prefLabel'] == 'E-böcker'}) {
          categories << [(ID): KBRDA + 'EBook']
          changed = true
        }

    } else {

      if (itype == "Print") {
        if (isVolume) {
          categories << [(ID): KBGF + 'PrintedVolume']
        } else {
          categories << [(ID): KBGF + 'Print']
        }
        changed = true
      } else if (itype == "Instance") {
        if (isVolume) {
          if (probablyPrint) {
            categories << [(ID): KBGF + 'PrintedVolume']
            changed = true
            // TODO: if marc:RegularPrintReproduction, add production a Reproduction?
          } else {
            categories << [(ID): KBRDA + 'Volume']
            changed = true
          }
        } else {
          if (probablyPrint) {
            if (mappings.matches(carriertypes, "Sheet")) {
              categories << [(ID): KBGF + 'PrintedSheet']
            } else {
              categories << [(ID): KBGF + 'Print'] // TODO: may be PartOfPrint ?
            }
            changed = true
          } else {
            if (mappings.matches(carriertypes, "Sheet")) {
              categories << [(ID): KBRDA + 'Sheet']
              if (dropReundantString(instance, "marc:mediaTerm", ~/(?i)affisch/)) {
                categories << [(ID): SAOGF + 'Poster']
              }
              changed = true
            }
          }
        }
      }

    }

    if (categories.size() > 0) {
      instance.put("category", mappings.reduceSymbols(categories))
      instance.remove("genreForm")
      instance.remove("carrierType")
      changed = true
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
      ) || instance.containsKey("publication") // TODO: publication is sometimes used on Manuscripts
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

typeNormalizer = new TypeNormalizer(new TypeMappings(getWhelk(), scriptDir))

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
