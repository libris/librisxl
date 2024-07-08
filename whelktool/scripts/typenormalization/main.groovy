import java.util.stream.Collectors

import static whelk.util.Jackson.mapper

class TypeNormalizer {
  static final var GRAPH = '@graph'
  static final var ID = '@id'
  static final var TYPE = '@type'
  static final var VALUE = '@value'
  static final var ANNOTATION = "@annotation"
  static final var KBV = "https://id.kb.se/vocab/"

  static final var MARC = "https://id.kb.se/marc/"
  static final var SAO = "https://id.kb.se/term/sao/"
  static final var BARN = "https://id.kb.se/term/barn/"
  static final var SAOGF = "https://id.kb.se/term/saogf/"
  static final var BARNGF = "https://id.kb.se/term/barngf/"
  static final var KBRDA = "https://id.kb.se/term/rda/"
  static final var TGM = "https://id.kb.se/term/gmgpc/swe/"

  // Mixes subxlasses and subconcepts
  static Map<String, Set<String>> baseEqualOrSubMap = [
    (MARC + 'DirectElectronic'): [MARC + 'ChipCartridge'] as Set,
    (MARC + 'Novel'): [SAOGF + 'Romaner'] as Set,
    'Audio': ['PerformedMusic', 'SpokenWord'] as Set,
  ]

  static Map<String, String> contentToTypeMap = [
    (KBRDA + "Text"): "Text",
    (KBRDA + "NotatedMusic"): "NotatedMusic",
    (KBRDA + "PerformedMusic"): "PerformedMusic",
    (KBRDA + "SpokenWord"): "SpokenWord",
    // 'StillImage' ...
    (KBRDA + "TwoDimensionalMovingImage"): 'MovingImage',
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
    if (itype == 'Map' || itype == 'Globe') {
      instance[TYPE] = itype + 'Instance'
      if (work[TYPE] == 'Cartography') {
        work[TYPE] = itype
        modified = true
      }
    }

    modified |= simplifySingleunitInstance(instance)
    modified |= simplifySingleunitWork(work)

    if (instance.containsKey("issuanceType")) {
      // work.genreForm marc:MapBoundAsPartOfAnotherWork
      modified |= convertIssuancetype(instance, work)
    }

    // modified |= simplifyAssociatedMedia(instance)  // TODO: move to separate normalization.

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

    var rtype = (String) work.get(TYPE)
    var genreforms = (List) reduceSymbols(asList(work.get("genreForm")))
    // TODO: drop from picklist...?
    var gfPicklist = genreforms.stream().filter(
        (it) -> (it.containsKey(ID) &&
                 !(it.get(ID).startsWith(SAOGF) || it.get(ID).startsWith(BARNGF)))
      ).collect(Collectors.toList())

    var contenttypes = (List) reduceSymbols(asList(work.get("contentType")))

    if (contenttypes.size() == 1) {
      String ctypeid = contenttypes.get(0).get(ID)
      var contentbasictype = contentToTypeMap[ctypeid]
      if (contentbasictype &&
        (contentbasictype == rtype || contentbasictype in baseEqualOrSubMap[rtype])
      ) {
        if (contentbasictype != rtype) {
          work.put(TYPE, contentbasictype)
        }
        work.remove("contentType")
      }
    }

    /* ... */
    if (
        [
          "${SAOGF}Romaner",
          "${MARC}Novel",
          "${MARC}Fiction" /*only for Audio*/,
        ].find { iri -> genreforms.find { it[ID] == iri } }
        || matches(genreforms, "b%C3%B6cker")
    ) {
      if (work.get(TYPE) == "Text") {
        work.put(TYPE, "WrittenBook")
      } else if (work.get(TYPE) == "Audio") {
        work.put(TYPE, "Audiobook")
      } else if (work.get(TYPE) == "Tactile") {
        work.put(TYPE, "TactileBook")
      }
    }

    if (genreforms.size() > 0) {
      work.put("genreForm", genreforms)
    } else {
      work.remove("genreForm")
    }

    if (contenttypes.size() > 0 && work.containsKey("contentType")) {
      work.put("contentType", contenttypes)
    } else {
      work.remove("contentType")
    }

    // FIXME: if any change was actually made!
    return true
  }

  static boolean simplifySingleunitInstance(Map instance) {
    var type = instance.get(TYPE)
    // TODO: remove these if implied by type or carrier:
    //var mediatypes = reduceSymbols(asList(instance["mediaType"]), "MediaType")
    var carriertypes = reduceSymbols(asList(instance["carrierType"]), "CarrierType")
    var isSoundRecording = instance[TYPE] == "SoundRecording"
    var isVideoRecording = instance[TYPE] == "VideoRecording"
    var isElectronic = type == "Electronic"
    if ((isElectronic && matches(carriertypes, "Online"))) {
      carriertypes = carriertypes.stream().filter((x) -> !x.getOrDefault(ID, "").contains("Online")).collect(Collectors.toList())
      if ((carriertypes.size() > 1 || !(matches(carriertypes, "Electronic")))) {
        instance.put("carrierType", carriertypes)
      } else {
        instance.remove("carrierType")
      }
      instance.put(TYPE, "DigitalResource")
    }

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
      } else if (isVolume) {
        instance.put(TYPE, "Volume")
      } else {
        /* ... */
      }
    }

    if (!isElectronic) {
      if (carriertypes.size() == 1) {
        instance.remove("carrierType")
      }
      if (type == "Print" && isVolume) {
        instance.put(TYPE, "PrintedVolume")
      } else if (type == "Instance") {
        if (!(isVolume)) {
          instance.put(TYPE, "Monograph")
        } else if (assumedToBePrint(instance)) {
          instance.put(TYPE, "PrintedVolume")
          // TODO: if marc:RegularPrintReproduction, add production a Reproduction?
        } else {
          instance.put(TYPE, "Volume")
        }
      }
    }

    List instanceGfs = asList(instance.get("genreForm"))
    var mediaterm = (String) instance.get("marc:mediaTerm")
    if (mediaterm) {
      if (mediaterm.toLowerCase() == "affisch") {
        instance.remove("marc:mediaTerm")
        assert matches(carriertypes, "Sheet")
        // TODO: work.genreForm = kbvgf:Poster (implies work.type = IllustratedWork | StillImage)
        instance.put(TYPE, "Sheet")
        List reducedGfs = instanceGfs.stream().filter((it) -> !it.get(ID).equals("${MARC}Print")).collect(Collectors.toList())
        if (reducedGfs.size() == 0) {
          instance.remove("genreForm")
        } else {
          instance.put("genreForm", reducedGfs)
        }
      } else if (isElectronic && mediaterm.matches(/(?i)elektronisk (resurs|utgåva)/)) {
        instance.remove("marc:mediaTerm")
      } else if ((isSoundRecording && mediaterm.matches(/(?i)ljudupptagning/))) {
        instance.remove("marc:mediaTerm")
      } else if ((isVideoRecording && mediaterm.matches(/(?i)videoupptagning/))) {
        instance.remove("marc:mediaTerm")
      } else if (instance.get(TYPE).equals("Tactile") && mediaterm.matches(/(?i)punktskrift/)) {
        if (isVolume) {
          instance.put(TYPE, "BrailleVolume")
        } else {
          instance.put(TYPE, "BrailleResource")
        }
        var toDrop = [KBRDA + "Volume", MARC + "Braille", MARC + "TacMaterialType-b"] as Set
        carriertypes = carriertypes.findAll { !toDrop.contains(it.get(ID)) }
        if (carriertypes == null) {
          instance.remove("carrierType")
        } else {
          instance.put("carrierType", carriertypes)
        }
        instance.remove("marc:mediaTerm")
      }
    }

    // FIXME: if any change was actually made!
    return true
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

  static boolean simplifyAssociatedMedia(Map instance) {
    var modified = false

    for (tuple in [
      ["associatedMedia", "hasRepresentation"],
      ["isPrimaryTopicOf", "associatedMedia"]
    ]) {
      def (givenRel, mediaRel) = tuple
      for (Object qualification : asList(instance.get(givenRel))) {
        def uri = qualification.get("uri")
        if (uri) {
          if (uri instanceof Map) {
            uri = ((Map) uri).get(VALUE)
          }
          asList(qualification.getOrDefault("cataloguersNote", "")).each {
            if (it.toLowerCase().contains("digipic")) mediaRel = "image"
          }
          if (uri instanceof List) {
            assert uri.size() == 1
            uri = uri[0]
          }
          var mediaObj = [(ID): uri]
          instance.put(mediaRel, mediaObj)
          var scope = qualification.get("appliesTo")
          Map annot
          if ((scope instanceof Map && scope.containsKey("label"))) {
            if (!mediaObj.containsKey(ANNOTATION)) mediaObj.put(ANNOTATION, [:])
            annot = (Map) mediaObj.get(ANNOTATION)
            annot.put("label", ((Map) scope).get("label"))
          }
          String note = qualification.get("marc:publicNote")
          if (note) {
            if (!mediaObj.containsKey(ANNOTATION)) mediaObj.put(ANNOTATION, [:])
            annot = (Map) mediaObj.get(ANNOTATION)
            annot.put("comment", note)
          }
        }
        continue
      }
      instance.remove(givenRel)
      modified = true
    }

    return modified
  }

  static List asList(Object o) {
    return (o instanceof List ? (List) o : o == null ? [] : [o])
  }

}

// TODO: if instance and work codepend; fetch work and normalize that in tandem;
// if so, store work id in mem to avoid converting again?
// (or is the mem-overhead worse than read(+attempting to convert) twice?)
convertedWorks = java.util.concurrent.ConcurrentHashMap.newKeySet()

boolean debug = true

PrintWriter fullInput = debug ? getReportWriter("full-input.ndjson") : null
PrintWriter fullOutput = debug ? getReportWriter("full-output.ndjson") : null
var debugPreChange = { instance, work ->
  if (debug) {
    fullInput.println mapper.writeValueAsString(instance)
    def loadedWorkId = instance.instanceOf[ID]
    if (loadedWorkId && loadedWorkId !in convertedWorks) {
      fullInput.println mapper.writeValueAsString(work)
    }
  }
}
var debugPostChange = { instance, work ->
  if (debug) {
    def loadedWorkId = instance.instanceOf[ID]
    fullOutput.println mapper.writeValueAsString(instance)
    if (loadedWorkId && loadedWorkId !in convertedWorks) {
      fullOutput.println mapper.writeValueAsString(work)
    }
  }
}

File examplesFile = new File(scriptDir, 'examples.txt')
List ids = examplesFile.iterator().findResults {
  (it =~ /^[^#]*<([^>]+?(?:([^\/#]+)#it)?)>/).findResult { m, iri, xlid -> xlid }
}

//ids = ['n117254ll8sv2bnm']

selectByIds(ids) {
  def (record, instance) = it.graph

  if ('instanceOf' in instance) {
    if (ID !in instance.instanceOf) {
      def work = instance.instanceOf

      debugPreChange(instance, work)
      var modified = TypeNormalizer.normalize(instance, work)
      debugPostChange(instance, work)

      if (modified) it.scheduleSave()
    } else {
      def loadedWorkId = instance.instanceOf[ID]
      selectByIds([loadedWorkId]) { workIt ->
        def (workRecord, work) = workIt.graph

        debugPreChange(instance, work)
        var modified = TypeNormalizer.normalize(instance, work)
        debugPostChange(instance, work)

        if (modified) {
          it.scheduleSave()
          if (loadedWorkId !in convertedWorks) workIt.scheduleSave()
        }
        convertedWorks << loadedWorkId
      }
    }

  }
}
