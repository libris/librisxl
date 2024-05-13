/*
$ java -Dxl.secret.properties=../DEV2-secret.properties -jar build/libs/whelktool.jar --dry-run --step scripts/typenormalization/main.groovy
*/

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

  static Map<String, Set<String>> baseEqualOrSubMap = [
    "${MARC}DirectElectronic": ["${MARC}ChipCartridge"] as Set,
    "${MARC}Novel": ["${SAOGF}Romaner"] as Set,
    "Audio": ["PerformedMusic", "SpokenWord"],
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
    List mostSpecific = symbols.stream().filter((x) -> !(symbols.stream().anyMatch(y -> isbaseof(x, y)))).collect(Collectors.toList())
    return mostSpecific
  }

  static boolean isbaseof(Object x, Object y) {
    if (x instanceof Map && x instanceof Map) {
      return y[ID] in baseEqualOrSubMap[x[ID]]
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

  static void normalize(Map instance, Map work) {
    interpretClassification(instance)
    interpretClassification(work)
    simplifyTyped(instance, work)
  }

  static void simplifyTyped(Map instance, Map work) {
    var isNonSingleunit = false
    if (instance.containsKey("issuanceType")) {
      isNonSingleunit = convertIssuancetype(instance, work)
    }
    if (isNonSingleunit) {
      return
    }

    simplifySingleunitInstance(instance)
    // simplifyAssociatedMedia(instance)  // TODO: move to separate normalization.

    simplifySingleunitWork(work)
  }

  static void interpretClassification(Map thing) {
    /* ... */
  }

  static boolean convertIssuancetype(Map instance, Map work) {
    var collectiontype = (String) instance.remove("issuanceType")
    if ((collectiontype && !collectiontype.equals("Monograph"))) {
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

  static void simplifySingleunitWork(Map work) {
    var refSize = work.containsKey(ANNOTATION) ? 2 : 1
    if (work.containsKey(ID) && work.size() == refSize) {
      return
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
        work.put(TYPE, "AudioBook")
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
  }

  static void simplifySingleunitInstance(Map instance) {
    var type = instance.get(TYPE)
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
      instance.put(TYPE, "DigitalDocument")
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
        } else if ((instance.get("identifiedBy").stream().anyMatch(x -> x.get(TYPE).equals("ISBN")))
                   || instance.containsKey("publication")) {
          instance.put(TYPE, "PrintedVolume")
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
        instance.put(TYPE, "PosterSheet")
        List reducedGfs = instanceGfs.stream().filter((it) -> !it.get(ID).equals("${MARC}Print")).collect(Collectors.toList())
        if (reducedGfs == null) {
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

  static void simplifyAssociatedMedia(Map instance) {
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
    }
  }

  static List asList(Object o) {
    return (o instanceof List ? (List) o : o == null ? [] : [o])
  }

}

File examplesFile = new File(scriptDir, 'examples.txt')
List ids = examplesFile.iterator().findResults {
  (it =~ /^[^#]*<([^>]+?(?:([^\/#]+)#it)?)>/).findResult { m, iri, xlid -> xlid }
}

boolean debug = true

PrintWriter fullInput = debug ? getReportWriter("full-input.ndjson") : null
PrintWriter fullOutput = debug ? getReportWriter("full-output.ndjson") : null

// TODO: if instance and work codepend; fetch work and normalize that in tandem;
// if so, store work id in mem to avoid converting again?
// (or is the mem-overhead worse than read(+attempting to convert) twice?)
convertedWorks = java.util.concurrent.ConcurrentHashMap.newKeySet()

selectByIds(ids) {
  def (record, instance) = it.graph
  def work = null
  def loadedWorkId = null

  if ('instanceOf' in instance) {
    if (ID !in instance.instanceOf) {
      work = instance.instanceOf
    } else {
      loadedWorkId = instance.instanceOf[ID]
      // TODO: normalize both in this block?
      //selectByIds([instance.instanceOf[ID]]) {
      //  ...
      //  workItem.scheduleSave()
      //}
      def workRecordData = load(loadedWorkId)
      work = workRecordData[GRAPH][1]
    }

    if (debug) {
      fullInput.println mapper.writeValueAsString(instance)
      if (loadedWorkId && loadedWorkId !in convertedWorks) {
        fullInput.println mapper.writeValueAsString(work)
      }
    }

    TypeNormalizer.normalize(instance, work)

    if (debug) {
      fullOutput.println mapper.writeValueAsString(instance)
      if (loadedWorkId && loadedWorkId !in convertedWorks) {
        fullOutput.println mapper.writeValueAsString(work)
        convertedWorks << loadedWorkId
      }
    }

    //it.scheduleSave()
  }
}
