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

  //Parse system property "addCategory" as a boolean, default to false if none given
  boolean addCategory = Boolean.parseBoolean(System.getProperty("addCategory")) ?: false
  static MARC = DefinitionsData.MARC
  static KBRDA = DefinitionsData.KBRDA
  static KBGF = DefinitionsData.KBGF

  TypeMappings mappings

  TypeNormalizer(TypeMappings mappings) {
    this.mappings = mappings
  }

  // ----- Normalization action-----
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


  // ----- Work action-----
  boolean simplifyWorkType(Map work) {
    var refSize = work.containsKey(ANNOTATION) ? 2 : 1
    if (work.containsKey(ID) && work.size() == refSize) {
      return false
    }

    var changed = false

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

    if (genreforms.removeIf { !it[ID] && it['prefLabel'] == 'DAISY' }) {
      genreforms << [(ID): KBGF + 'Audiobook']
    }

    // If we want to use the new property "category"
    if (addCategory) {
      List<String> categories = []
      categories += genreforms + contenttypes
      work.remove("genreForm")
      work.remove("contentType")

      if (categories.size() > 0) {
        work.put("category", mappings.reduceSymbols(categories))
        changed = true
      }
    }

    return changed
  }


  // ----- Instance action -----
  boolean simplfyInstanceType(Map instance) {
    var changed = false

    var itype = instance.get(TYPE)

    // Get instance GenreForms and remove ones that are marc/Print
    List instanceGfs = asList(instance.get("genreForm")).stream().
            filter((it) -> !it[ID] != MARC + 'Print').collect(Collectors.toList())
    // FIXME Why replace GF "E-böcker" with KBRDA EBook?
    //if (instanceGfs.removeIf { it['prefLabel'] == 'E-böcker'}) {
    //  categories << [(ID): KBRDA + 'EBook']
    //changed = true
    //}

    // Only keep the most specific mediaTypes and carrierTypes
    List mediatypes = mappings.reduceSymbols(asList(instance["mediaType"]), "MediaType")
    List carriertypes = mappings.reduceSymbols(asList(instance["carrierType"]), "CarrierType")

    // Remove the mediaType if it can be inferred by the carrierType
    var impliedMediaIds = carriertypes.findResults { mappings.carrierMediaMap[it[ID]] } as Set
    if (mediatypes.every { it[ID] in impliedMediaIds }) {
      instance.remove("mediaType")
      changed = true
    }

    // Store information about the old instanceType
    // In the case of volume, it is inferred
    var isElectronic = itype == "Electronic"
    var isSoundRecording = itype == "SoundRecording"
    var isVideoRecording = itype == "VideoRecording"
    var isTactile = itype == "Tactile"
    var isVolume = mappings.matches(carriertypes, "Volume") || looksLikeVolume(instance)

    // FIXME Which of these are only true for the "category" scenario?

    // If the resource is electronic and has at least on carrierType that is "Online"
    if ((isElectronic && mappings.matches(carriertypes, "Online"))) {
      // FIXME Understand what is going on here
      // Is the purpose of this to make sure that 1) there is minimal duplication between instanceType and carrierType,
      // but that there is always a carrier type (even if this means duplication)?
      carriertypes = carriertypes.stream()
        .filter((x) -> !x.getOrDefault(ID, "").contains("Online"))
        .collect(Collectors.toList())
      if (carriertypes.size() == 0) {
        // FIXME make sure this carries through to categories
        carriertypes << [(ID): KBRDA + 'OnlineResource']
      }

      // Apply new instance types DigitalResource and PhysicalResource
      // FIXME For readability, could this happen before the carrier/media type cleanup?
      //  Old instancetype is already stored in itype
      instance.put(TYPE, "DigitalResource")
      changed = true
    } else {
      instance.put(TYPE, "PhysicalResource")
      changed = true
    }

    // Remove redundant MARC mediaTerms if the information is given by the old itype
    if (isSoundRecording && dropReundantString(instance, "marc:mediaTerm", ~/(?i)ljudupptagning/)) {
      changed = true
    }
    if (isVideoRecording && dropReundantString(instance, "marc:mediaTerm", ~/(?i)videoupptagning/)) {
      changed = true
    }
    var isBraille = dropReundantString(instance, "marc:mediaTerm", ~/(?i)punktskrift/)

    // Clean up some Braille-related terms
    var toDrop = [KBRDA + "Volume", MARC + "Braille", MARC + "TacMaterialType-b"] as Set
    var nonBrailleCarrierTypes = carriertypes.findAll { !toDrop.contains(it.get(ID)) }
    if (nonBrailleCarrierTypes.size() < carriertypes.size()) {
      isBraille = true
      carriertypes = nonBrailleCarrierTypes
    }

    // FIXME check that this carrie over to categories
    if (isTactile && isBraille) {
      if (isVolume) {
        instanceGfs << [(ID): KBGF + 'BrailleVolume']
      } else {
        instanceGfs << [(ID): KBGF + 'BrailleResource']
      }
      changed = true
    }

    // FIXME Clarify the logic/outcome here
    // TODO: should work with regular reduceSymbols ...
    def carrierTuples = [
      [isElectronic, ["ChipCartridge"], "ChipCartridge"],
      [isSoundRecording, [MARC + 'SoundDisc', KBRDA + 'AudioDisc']],
      [isSoundRecording, [MARC + 'SoundCassette', KBGF + 'AudioCassette']],
      [isVideoRecording, [MARC + 'VideoDisc', "${MARC}VideoMaterialType-d", KBRDA + 'Videodisc']]
    ]
    for (carrierTuple in carrierTuples) {
      def (isIt, matchTokens) = carrierTuple
      List gotMatches = matchTokens.findAll { mappings.matches(carriertypes, it) }
      if (isIt && gotMatches.size() > 0) {
        isElectronic = true
        // FIXME can we add this to carriertypes instead of categories?
        carriertypes << [(ID): matchTokens[-1]]
        changed = true
      }
    }

    // This overlaps with "instance instance" cleanup
    if (instance.get(TYPE) == "Instance") {
      if ((carriertypes.size() == 1 && mappings.matches(carriertypes, "Electronic"))) {
        isElectronic = true
      }
    }

    var probablyPrint = assumedToBePrint(instance)

    if (isElectronic) {
        if (dropReundantString(instance, "marc:mediaTerm", ~/(?i)elektronisk (resurs|utgåva)/)) {
          changed = true
        }

    } else {

      if (itype == "Print") {
        if (isVolume) {
          instanceGfs << [(ID): KBGF + 'PrintedVolume']
        } else {
          instanceGfs << [(ID): KBGF + 'Print']
        }
        changed = true
      } else if (itype == "Instance") {
        if (isVolume) {
          if (probablyPrint) {
            instanceGfs << [(ID): KBGF + 'PrintedVolume']
            changed = true
            // TODO: if marc:RegularPrintReproduction, add production a Reproduction?
          } else {
            instanceGfs << [(ID): KBRDA + 'Volume']
            changed = true
          }
        } else {
          if (probablyPrint) {
            if (mappings.matches(carriertypes, "Sheet")) {
              instanceGfs << [(ID): KBGF + 'PrintedSheet']
            } else {
              instanceGfs << [(ID): KBGF + 'Print'] // TODO: may be PartOfPrint ?
            }
            changed = true
          } else {
            if (mappings.matches(carriertypes, "Sheet")) {
              instanceGfs << [(ID): KBRDA + 'Sheet']
              if (dropReundantString(instance, "marc:mediaTerm", ~/(?i)affisch/)) {
                instanceGfs << [(ID): SAOGF + 'Poster']
              }
              changed = true
            }
          }
        }
      }

    }

    // If we want to use the new property "category"
    if (addCategory) {
      List<String> categories = []

      categories += instanceGfs
      categories += carriertypes

      instance.remove("genreForm")
      instance.remove("carrierType")

      if (categories.size() > 0) {
        instance.put("category", mappings.reduceSymbols(categories))
        changed = true
      }
    }

    return changed
  }


  // ----- Helper methods -----
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

  void reorder(Map thing) {
    var copy = thing.clone()
    thing.clear()
    for (var key : [ID, TYPE, 'sameAs', 'category', 'issuanceType']) {
      if (key in copy) thing[key] = copy[key]
    }
    thing.putAll(copy)
  }

}

// ----- Main action -----

// NOTE: Since instance and work types may co-depend; fetch work and normalize
// that in tandem. We store work ids in memory to avoid converting again.
// TODO: Instead, normalize linked works first, then instances w/o linked works?
convertedWorks = java.util.concurrent.ConcurrentHashMap.newKeySet()

typeNormalizer = new TypeNormalizer(new TypeMappings(getWhelk(), scriptDir))
if (typeNormalizer.addCategory) {
  System.out.println("\n---\nAdding new property: category\nRemoving properties: genreForm, contentType, carrierType\n---\n")
}
else {
  System.out.println("Keeping properties genreForm, contentType, and carrierType.")
}

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
