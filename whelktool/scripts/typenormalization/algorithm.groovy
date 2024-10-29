import java.util.stream.Collectors

import static whelk.util.Jackson.mapper

class TypeNormalizer {
  static final var GRAPH = '@graph'
  static final var ID = '@id'
  static final var TYPE = '@type'
  static final var VALUE = '@value'
  static final var ANNOTATION = "@annotation"
  static final var KBV = "https://id.kb.se/vocab/"

  // FIXME: Use definitions data (loaded into XL) and remove these hard-coded mappings!

  static final var MARC = "https://id.kb.se/marc/"
  static final var SAO = "https://id.kb.se/term/sao/"
  static final var BARN = "https://id.kb.se/term/barn/"
  static final var SAOGF = "https://id.kb.se/term/saogf/"
  static final var BARNGF = "https://id.kb.se/term/barngf/"
  static final var KBRDA = "https://id.kb.se/term/rda/"
  static final var TGM = "https://id.kb.se/term/gmgpc/swe/"

  // Mixes subclasses and subconcepts
  static Map<String, Set<String>> baseEqualOrSubMap = [ // matchesMap
    (MARC + 'DirectElectronic'): [MARC + 'ChipCartridge'] as Set,
    (MARC + 'Novel'): [SAOGF + 'Romaner'] as Set,
    'Audio': ['PerformedMusic', 'SpokenWord'] as Set,
  ]

  static var coordinationFormMap = [
      ("${MARC}Atlas" as String): 'AtlasForm',
      'https://id.kb.se/term/gmgpc/swe/Atlaser': 'AtlasForm',
      ("${MARC}Novel" as String): 'BookForm',
      ("${MARC}Fiction" as String): 'Fiction',
      ("${SAOGF}Romaner" as String): 'BookForm',
      ("${SAOGF}Pop-up-b%C3%B6cker" as String): 'PopupBook',
      ("${SAOGF}%C3%85rsb%C3%B6cker" as String): 'Yearbook',
      ("${SAOGF}Miniatyrb%C3%B6cker" as String): 'BookForm',
      ("${SAOGF}Mekaniska%20b%C3%B6cker" as String): 'InteractiveBook',
      ("${SAOGF}Pysselb%C3%B6cker" as String): 'BookForm',
      ("${SAOGF}Målarb%C3%B6cker" as String): 'BookForm',
      ("${SAOGF}Guideb%C3%B6cker" as String): 'BookForm',
      ("${SAOGF}Pop-up-b%C3%B6cker" as String): 'InteractiveBook',
      ("${SAOGF}Kokb%C3%B6cker" as String): 'BookForm',
      ("${BARNGF}Kapitelb%C3%B6cker" as String): 'BookForm',
      'https://id.kb.se/term/barngf/Bilderb%C3%B6cker': 'TextAndImagesBook',
  ]

  static var complexTypeMap = [
      'Text': ['BookForm': 'WrittenBook', 'TextAndImagesBook': 'TextAndImagesBook'],
      'Cartography': ['AtlasForm': 'Atlas'],
      'Audio': ['BookForm': 'AudioBook', 'Fiction': 'AudioBook'],
      'Tactile': ['BookForm': 'TactileBook'],
  ]

  static var impliedContentTypes = [
      'Atlas': ["${KBRDA}CartographicImage" as String, "${KBRDA}Text" as String] as Set
  ]

  static Map<String, String> contentToTypeMap = [
    (KBRDA + "Text"): "Text",
    (KBRDA + "NotatedMusic"): "NotatedMusic",
    (KBRDA + "PerformedMusic"): "PerformedMusic",
    (KBRDA + "SpokenWord"): "SpokenWord",
    // 'StillImage' ...
    (KBRDA + "TwoDimensionalMovingImage"): 'MovingImage',
  ]

  /*
  prefix : <https://id.kb.se/vocab/>
  prefix kbrda: <https://id.kb.se/term/rda/>
  select ?carrier ?media where { ?carrier a :CarrierType ; :broader ?media }
  */
  static Map<String, String> carrierMediaMap = [
    'https://id.kb.se/term/rda/OverheadTransparency': 'https://id.kb.se/term/rda/Projected',
    'https://id.kb.se/term/rda/OnlineResource': 'https://id.kb.se/term/rda/Computer',
    'https://id.kb.se/term/rda/Sheet': 'https://id.kb.se/term/rda/Unmediated',
    'https://id.kb.se/term/rda/Volume': 'https://id.kb.se/term/rda/Unmediated',
    'https://id.kb.se/term/rda/AudioDisc': 'https://id.kb.se/term/rda/Audio',
    'https://id.kb.se/term/rda/ComputerDisc': 'https://id.kb.se/term/rda/Computer',
    'https://id.kb.se/term/rda/MicrofilmReel': 'https://id.kb.se/term/rda/Microform',
    'https://id.kb.se/term/rda/Audiocassette': 'https://id.kb.se/term/rda/Audio',
    'https://id.kb.se/term/rda/Slide': 'https://id.kb.se/term/rda/Projected',
    'https://id.kb.se/term/rda/Object': 'https://id.kb.se/term/rda/Unmediated',
    'https://id.kb.se/term/rda/StereographCard': 'https://id.kb.se/term/rda/Stereographic',
    'https://id.kb.se/term/rda/Card': 'https://id.kb.se/term/rda/Unmediated',
    'https://id.kb.se/term/rda/Videodisc': 'https://id.kb.se/term/rda/Video',
    'https://id.kb.se/term/rda/SoundTrackReel': 'https://id.kb.se/term/rda/Audio',
    'https://id.kb.se/term/rda/ComputerChipCartridge': 'https://id.kb.se/term/rda/Computer',
    'https://id.kb.se/term/rda/Microfiche': 'https://id.kb.se/term/rda/Microform',
    'https://id.kb.se/term/rda/ComputerCard': 'https://id.kb.se/term/rda/Computer',
    'https://id.kb.se/term/rda/Videocassette': 'https://id.kb.se/term/rda/Video',
    'https://id.kb.se/term/rda/Roll': 'https://id.kb.se/term/rda/Unmediated',
    'https://id.kb.se/term/rda/MicrofilmRoll': 'https://id.kb.se/term/rda/Microform',
    'https://id.kb.se/term/rda/ComputerDiscCartridge': 'https://id.kb.se/term/rda/Computer',
    'https://id.kb.se/term/rda/FilmCartridge': 'https://id.kb.se/term/rda/Projected',
    'https://id.kb.se/term/rda/VideotapeReel': 'https://id.kb.se/term/rda/Video',
    'https://id.kb.se/term/rda/ApertureCard': 'https://id.kb.se/term/rda/Microform',
    'https://id.kb.se/term/rda/Filmslip': 'https://id.kb.se/term/rda/Projected',
    'https://id.kb.se/term/rda/Filmstrip': 'https://id.kb.se/term/rda/Projected',
    'https://id.kb.se/term/rda/Flipchart': 'https://id.kb.se/term/rda/Unmediated',
    'https://id.kb.se/term/rda/Microopaque': 'https://id.kb.se/term/rda/Microform',
    'https://id.kb.se/term/rda/MicroscopeSlide': 'https://id.kb.se/term/rda/Microscopic',
    'https://id.kb.se/term/rda/AudioCartridge': 'https://id.kb.se/term/rda/Audio',
    'https://id.kb.se/term/rda/AudioCylinder': 'https://id.kb.se/term/rda/Audio',
    'https://id.kb.se/term/rda/AudioRoll': 'https://id.kb.se/term/rda/Audio',
    'https://id.kb.se/term/rda/AudiotapeReel': 'https://id.kb.se/term/rda/Audio',
    'https://id.kb.se/term/rda/ComputerTapeCartridge': 'https://id.kb.se/term/rda/Computer',
    'https://id.kb.se/term/rda/ComputerTapeCassette': 'https://id.kb.se/term/rda/Computer',
    'https://id.kb.se/term/rda/ComputerTapeReel': 'https://id.kb.se/term/rda/Computer',
    'https://id.kb.se/term/rda/FilmCassette': 'https://id.kb.se/term/rda/Projected',
    'https://id.kb.se/term/rda/FilmReel': 'https://id.kb.se/term/rda/Projected',
    'https://id.kb.se/term/rda/FilmRoll': 'https://id.kb.se/term/rda/Projected',
    'https://id.kb.se/term/rda/FilmstripCartridge': 'https://id.kb.se/term/rda/Projected',
    'https://id.kb.se/term/rda/MicroficheCassette': 'https://id.kb.se/term/rda/Microform',
    'https://id.kb.se/term/rda/MicrofilmCartridge': 'https://id.kb.se/term/rda/Microform',
    'https://id.kb.se/term/rda/MicrofilmCassette': 'https://id.kb.se/term/rda/Microform',
    'https://id.kb.se/term/rda/MicrofilmSlip': 'https://id.kb.se/term/rda/Microform',
    'https://id.kb.se/term/rda/StereographDisc': 'https://id.kb.se/term/rda/Stereographic',
    'https://id.kb.se/term/rda/VideoCartridge': 'https://id.kb.se/term/rda/Video',
  ]

  static List reduceSymbols(List symbols) {
    return reduceSymbols(symbols, null)
  }

  static List reduceSymbols(List symbols, String expectedtype) {
    List mostSpecific = symbols.stream().filter(x -> !(symbols.stream().anyMatch(y -> isbaseof(x, y)))).collect(Collectors.toList())
    return mostSpecific
  }

  static boolean isbaseof(Object x, Object y) {
    if (x instanceof Map && y instanceof Map) {
      var subterms = baseEqualOrSubMap[x[ID]]
      if (subterms) {
        return y[ID] in subterms
      }
    }
    return false
  }

  static boolean matches(ArrayList<Map> typelikes, String value) {
    for (Map ref : typelikes) {
      if ((ref.containsKey(ID) && ref.get(ID).contains(value))) {
        return true
      }
    }
    return false
  }

  static boolean normalize(Map instance, Map work) {
    var modified = simplifyTyped(instance, work)
    return modified
  }

  static boolean simplifyTyped(Map instance, Map work) {
    var modified = false

    var itype = (String) instance[TYPE]

    if (itype == 'Map') {
      if (work[TYPE] == 'Cartography') {
        instance[TYPE] = 'Instance'
        if (!instance['carrierType']) {
          instance['carrierType'] = [ [(ID): "${MARC}Sheet" as String] ]
        }
        work[TYPE] = 'SingleMap'
        // TODO: drop instance['genreForm'] marc:MapATwoDimensionalMap
        modified = true
      }
    } else if (itype == 'Globe') {
      if (work[TYPE] == 'Cartography') {
        instance[TYPE] = 'Object'
        work[TYPE] = 'Globe'
        modified = true
      }
    }

    modified |= simplifySingleunitInstance(instance)
    modified |= simplifySingleunitWork(work)

    if (instance.containsKey("issuanceType")) {
      // work.genreForm marc:MapBoundAsPartOfAnotherWork
      modified |= convertIssuancetype(instance, work)
    }

    return modified
  }

  static boolean convertIssuancetype(Map instance, Map work) {
    var collectiontype = (String) instance.remove("issuanceType")
    if (!collectiontype) {
      return false
    }
    if (collectiontype.equals("ComponentPart")) {
        instance[TYPE] += collectiontype
    } else if ((!collectiontype.equals("Monograph"))) {
      // TODO: check genres and heuristics (some Serial are mistyped!)
      if ('collectsType' in work) {
        assert work['collectsType'] == instance.get(TYPE)
        assert work[TYPE] == collectiontype
      } else {
        work['collectsType'] = work.get(TYPE)
        work[TYPE] = collectiontype
        // FIXME:
        // move instance['carrierType'] to 'collectsType' and set instance[TYPE] = 'Multipart'?
        // Examples (e.g. "Samling av trycksaker"):
        // select ?crt1 ?crt2 (count(?g) as ?count)  (sample(?g) as ?sample) {
        //  graph ?g { ?s kbv:carrierType ?crt1, ?crt2 . FILTER(isIRI(?crt1) && isIRI(?crt2) && ?crt1 > ?crt2) }
        //  } order by desc(?count)
        return true
      }
    }
    return false
  }

  static boolean simplifySingleunitWork(Map work) {
    var refSize = work.containsKey(ANNOTATION) ? 2 : 1
    if (work.containsKey(ID) && work.size() == refSize) {
      return false
    }

    var changed = false

    var worktype = (String) work.get(TYPE)

    // TODO: change this. This currently drops contenttypes; but we need to map its combo to a name, both ways.
    var contenttypes = (List) reduceSymbols(asList(work.get("contentType")))
    if (contenttypes.size() == 1) {
      String ctypeid = contenttypes.get(0).get(ID)
      var contentbasictype = contentToTypeMap[ctypeid]
      if (contentbasictype &&
        (contentbasictype == worktype || contentbasictype in baseEqualOrSubMap[worktype])
      ) {
        if (contentbasictype != worktype) {
          work.put(TYPE, contentbasictype)
          changed = true
        }
        work.remove("contentType")
        changed = true
      }
    }

    var genreforms = (List) reduceSymbols(asList(work.get("genreForm")))
    // TODO: drop from picklist...?
    var gfPicklist = genreforms.stream().filter(
        (it) -> (it.containsKey(ID) &&
                 !(it.get(ID).startsWith(SAOGF) || it.get(ID).startsWith(BARNGF)))
      ).collect(Collectors.toList())

    var formsToTypeMap = complexTypeMap.get(worktype)
    if (formsToTypeMap) {
      var coordinationForm = genreforms.findResult { coordinationFormMap[it[ID]] }
      if (coordinationForm) {
        var complexType = formsToTypeMap.get(coordinationForm)
        if (complexType) {
          work.put(TYPE, complexType)
          var impliedCTs = impliedContentTypes[complexType]
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

  static boolean simplifySingleunitInstance(Map instance) {
    var type = instance.get(TYPE)

    var mediatypes = reduceSymbols(asList(instance["mediaType"]), "MediaType")
    var carriertypes = reduceSymbols(asList(instance["carrierType"]), "CarrierType")

    var changed = false

    var impliedMediaIds = carriertypes.findResults { carrierMediaMap[it[ID]] } as Set
    if (mediatypes.every { it[ID] in impliedMediaIds }) {
      instance.remove("mediaType")
      changed = true
    }

    var isElectronic = type == "Electronic"
    if ((isElectronic && matches(carriertypes, "Online"))) {
      carriertypes = carriertypes.stream().filter((x) -> !x.getOrDefault(ID, "").contains("Online")).collect(Collectors.toList())
      if ((carriertypes.size() > 1 || !(matches(carriertypes, "Electronic")))) {
        instance.put("carrierType", carriertypes)
      } else {
        instance.remove("carrierType")
      }
      instance.put(TYPE, "DigitalResource")
      changed = true
    }

    var isSoundRecording = instance[TYPE] == "SoundRecording"
    var isVideoRecording = instance[TYPE] == "VideoRecording"
    def tuples = [
      [isElectronic, ["ChipCartridge"], "ChipCartridge"],
      [isSoundRecording, ["${MARC}SoundDisc", "${KBRDA}AudioDisc"], "AudioDisc"],
      [isSoundRecording, ["${MARC}SoundCassette"], "AudioCassette"],
      [isVideoRecording, ["${MARC}VideoDisc", "${MARC}VideoMaterialType-d", "${KBRDA}Videodisc"], "VideoDisc"]
    ]
    for (tuple in tuples) {
      def (isIt, matchTokens, useType) = tuple
      List gotMatches = matchTokens.findAll { matches(carriertypes, it) }
      if (isIt && gotMatches.size() > 0) {
        isElectronic = true
        instance.put(TYPE, useType)
        changed = true
        if (carriertypes.size() == gotMatches.size()) {
          instance.remove("carrierType")
        }
      }
    }

    var isVolume = matches(carriertypes, "Volume") || looksLikeVolume(instance)

    if (instance.get(TYPE) == "Instance") {
      if ((carriertypes.size() == 1 && matches(carriertypes, "Electronic"))) {
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

    if (!isElectronic) {
      if (type == "Print" && isVolume) {
        instance.put(TYPE, "PrintedVolume")
        changed = true
      } else if (type == "Instance") {
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
            if (matches(carriertypes, "Sheet")) {
              instance.put(TYPE, "PrintedSheet")
            } else {
              instance.put(TYPE, "Print") // TODO: may be PartOfPrint ?
            }
            changed = true
          } else {
            if (matches(carriertypes, "Sheet")) {
              instance.put(TYPE, "Sheet")
            } else {
              instance.put(TYPE, "Monograph")
            }
            changed = true
          }
        }
      }

      if (carriertypes.size() == 1) {
        instance.remove("carrierType")
        changed = true
      }
    }

    List instanceGfs = asList(instance.get("genreForm"))
    List reducedGfs = instanceGfs.stream().filter((it) -> !it.get(ID).equals("${MARC}Print")).collect(Collectors.toList())

    if (isElectronic && reducedGfs.removeIf { it['prefLabel'] == 'E-böcker'}) {
      // TODO: assert isA(work[TYPE], 'Book')
      instance.put(TYPE, "EBook")
      changed = true
    }

    if (reducedGfs.size() == 0) {
      instance.remove("genreForm")
      changed = true
    } else {
      instance.put("genreForm", reducedGfs)
      changed = true
    }

    var mediaterm = (String) instance.get("marc:mediaTerm")
    if (mediaterm) {
      if (mediaterm.toLowerCase() == "affisch") {
        instance.remove("marc:mediaTerm")
        if (matches(carriertypes, "Sheet")) {
          // TODO: work.genreForm = kbvgf:Poster (implies work.type = IllustratedWork | StillImage)
          instance.put(TYPE, "Sheet")
        }
        changed = true
      } else if (isElectronic && mediaterm.matches(/(?i)elektronisk (resurs|utgåva)/)) {
        instance.remove("marc:mediaTerm")
        changed = true
      } else if ((isSoundRecording && mediaterm.matches(/(?i)ljudupptagning/))) {
        instance.remove("marc:mediaTerm")
        changed = true
      } else if ((isVideoRecording && mediaterm.matches(/(?i)videoupptagning/))) {
        instance.remove("marc:mediaTerm")
        changed = true
      } else if (instance.get(TYPE).equals("Tactile") && mediaterm.matches(/(?i)punktskrift/)) {
        if (isVolume) {
          instance.put(TYPE, "BrailleVolume")
          changed = true
        } else {
          instance.put(TYPE, "BrailleResource")
          changed = true
        }
        var toDrop = [KBRDA + "Volume", MARC + "Braille", MARC + "TacMaterialType-b"] as Set
        carriertypes = carriertypes.findAll { !toDrop.contains(it.get(ID)) }
        if (carriertypes == null) {
          instance.remove("carrierType")
        } else {
          instance.put("carrierType", carriertypes)
        }
        instance.remove("marc:mediaTerm")
        changed = true
      }
    }

    return changed
  }

  static boolean assumedToBePrint(Map instance) {
    // TODO: carrierType == marc:RegularPrint || marc:RegularPrintReproduction
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

process { def doc, Closure loadWorkItem ->
  def (record, instance) = doc.graph

  if ('instanceOf' in instance) {
    if (ID !in instance.instanceOf) {
      def work = instance.instanceOf
      if (work instanceof List && work.size() == 1) {
        work = work[0]
      }

      var modified = TypeNormalizer.normalize(instance, work)

      if (modified) doc.scheduleSave()
    } else {
      def loadedWorkId = instance.instanceOf[ID]
      // TODO: refactor very hacky solution...
      loadWorkItem(loadedWorkId) { workIt ->
        def (workRecord, work) = workIt.graph

        var modified = TypeNormalizer.normalize(instance, work)

        if (modified) {
          doc.scheduleSave()
          if (loadedWorkId !in convertedWorks) workIt.scheduleSave()
        }
        convertedWorks << loadedWorkId
      }
    }

  }
}
