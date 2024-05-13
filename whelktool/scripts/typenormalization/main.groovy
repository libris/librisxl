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

  static void normalize(Map thing) {
    interpretClassification(thing)
    simplifyTyped(thing)
  }

  static void simplifyTyped(Map thing) {
    var isNonSingleunit = false
    if (thing.containsKey("issuanceType")) {
      isNonSingleunit = convertIssuancetype(thing)
    }
    if (isNonSingleunit) {
      return
    }
    if (thing.containsKey("instanceOf")) {
      simplifySingleunitInstance(thing)
      simplifyAssociatedMedia(thing)  // TODO: move to separate normalization.
      simplifySingleunitWork(thing.get("instanceOf"), thing)
    }
    if (thing.containsKey("itemOf")) {
      /* ... */
    } else {
      /* ... */
      simplifySingleunitWork(thing)
    }
  }

  static void interpretClassification(Map thing) {
    /* ... */
  }

  static boolean convertIssuancetype(Map thing) {
    var collectiontype = (String) thing.remove("issuanceType")
    if ((collectiontype && !collectiontype.equals("Monograph"))) {
      thing['collectsType'] = thing.get(TYPE)
      thing[TYPE] = collectiontype
      return true
    }
    return false
  }

  static void simplifySingleunitWork(Map thing, Map simpleinstance=null) {
    var refSize = thing.containsKey(ANNOTATION) ? 2 : 1
    if (thing.containsKey(ID) && thing.size() == refSize) {
      return
    }

    var rtype = (String) thing.get(TYPE)
    var genreforms = (List) reduceSymbols(asList(thing.get("genreForm")))
    // TODO: drop from picklist...?
    var gfPicklist = genreforms.stream().filter(
        (it) -> (it.containsKey(ID) &&
                 !(it.get(ID).startsWith(SAOGF) || it.get(ID).startsWith(BARNGF)))
      ).collect(Collectors.toList())

    var contenttypes = (List) reduceSymbols(asList(thing.get("contentType")))

    if (contenttypes.size() == 1) {
      String ctypeid = contenttypes.get(0).get(ID)
      var contentbasictype = contentToTypeMap[ctypeid]
      if (contentbasictype &&
        (contentbasictype == rtype || contentbasictype in baseEqualOrSubMap[rtype])
      ) {
        if (contentbasictype != rtype) {
          thing.put(TYPE, contentbasictype)
        }
        thing.remove("contentType")
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
      if (thing.get(TYPE) == "Text") {
        thing.put(TYPE, "WrittenBook")
      } else if (thing.get(TYPE) == "Audio") {
        thing.put(TYPE, "AudioBook")
      } else if (thing.get(TYPE) == "Tactile") {
        thing.put(TYPE, "TactileBook")
      }
    }

    if (genreforms.size() > 0) {
      thing.put("genreForm", genreforms)
    } else {
      thing.remove("genreForm")
    }

    if (contenttypes.size() > 0 && thing.containsKey("contentType")) {
      thing.put("contentType", contenttypes)
    } else {
      thing.remove("contentType")
    }
  }

  static void simplifySingleunitInstance(Map thing) {
    var type = thing.get(TYPE)
    var carriertypes = reduceSymbols(asList(thing["carrierType"]), "CarrierType")
    var isSoundRecording = thing[TYPE] == "SoundRecording"
    var isVideoRecording = thing[TYPE] == "VideoRecording"
    var isElectronic = type == "Electronic"
    if ((isElectronic && matches(carriertypes, "Online"))) {
      carriertypes = carriertypes.stream().filter((x) -> !x.getOrDefault(ID, "").contains("Online")).collect(Collectors.toList())
      if ((carriertypes.size() > 1 || !(matches(carriertypes, "Electronic")))) {
        thing.put("carrierType", carriertypes)
      } else {
        thing.remove("carrierType")
      }
      thing.put(TYPE, "DigitalDocument")
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
        thing.put(TYPE, useType)
        if (carriertypes.size() == gotMatches.size()) {
          thing.remove("carrierType")
        }
      }
    }

    var isVolume = matches(carriertypes, "Volume") || looksLikeVolume(thing)

    if (thing.get(TYPE) == "Instance") {
      if ((carriertypes.size() == 1 && matches(carriertypes, "Electronic"))) {
        isElectronic = true
        thing.put(TYPE, "Electronic")
        thing.remove("carrierType")
      } else if (isVolume) {
        thing.put(TYPE, "Volume")
      } else {
        /* ... */
      }
    }

    if (!isElectronic) {
      if (carriertypes.size() == 1) {
        thing.remove("carrierType")
      }
      if (type == "Print" && isVolume) {
        thing.put(TYPE, "PrintedVolume")
      } else if (type == "Instance") {
        if (!(isVolume)) {
          thing.put(TYPE, "Monograph")
        } else if ((thing.get("identifiedBy").stream().anyMatch(x -> x.get(TYPE).equals("ISBN")))
                   || thing.containsKey("publication")) {
          thing.put(TYPE, "PrintedVolume")
        } else {
          thing.put(TYPE, "Volume")
        }
      }
    }

    List instanceGfs = asList(thing.get("genreForm"))
    var mediaterm = (String) thing.get("marc:mediaTerm")
    if (mediaterm) {
      if (mediaterm.toLowerCase() == "affisch") {
        thing.remove("marc:mediaTerm")
        assert matches(carriertypes, "Sheet")
        thing.put(TYPE, "PosterSheet")
        List reducedGfs = instanceGfs.stream().filter((it) -> !it.get(ID).equals("${MARC}Print")).collect(Collectors.toList())
        if (reducedGfs == null) {
          thing.remove("genreForm")
        } else {
          thing.put("genreForm", reducedGfs)
        }
      } else if (isElectronic && mediaterm.matches(/(?i)elektronisk (resurs|utgåva)/)) {
        thing.remove("marc:mediaTerm")
      } else if ((isSoundRecording && mediaterm.matches(/(?i)ljudupptagning/))) {
        thing.remove("marc:mediaTerm")
      } else if ((isVideoRecording && mediaterm.matches(/(?i)videoupptagning/))) {
        thing.remove("marc:mediaTerm")
      } else if (thing.get(TYPE).equals("Tactile") && mediaterm.matches(/(?i)punktskrift/)) {
        if (isVolume) {
          thing.put(TYPE, "BrailleVolume")
        } else {
          thing.put(TYPE, "BrailleResource")
        }
        var toDrop = [KBRDA + "Volume", MARC + "Braille", MARC + "TacMaterialType-b"] as Set
        carriertypes = carriertypes.findAll { !toDrop.contains(it.get(ID)) }
        if (carriertypes == null) {
          thing.remove("carrierType")
        } else {
          thing.put("carrierType", carriertypes)
        }
        thing.remove("marc:mediaTerm")
      }
    }
  }

  static boolean looksLikeVolume(Map thing) {
    for (extent in asList(thing.get("extent"))) {
      for (label in asList(extent.get("label"))) {
        if (label instanceof String && label.matches(/(?i).*\s(s|p|sidor|pages|vol(ym(er)?)?)\b\.?/)) {
          return true
        }
      }
    }
    return false
  }

  static void simplifyAssociatedMedia(Map thing) {
    for (tuple in [
      ["associatedMedia", "hasRepresentation"],
      ["isPrimaryTopicOf", "associatedMedia"]
    ]) {
      def (givenRel, mediaRel) = tuple
      for (Object qualification : asList(thing.get(givenRel))) {
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
          thing.put(mediaRel, mediaObj)
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
      thing.remove(givenRel)
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

seenWorks = java.util.concurrent.ConcurrentHashMap.newKeySet()

selectByIds(ids) {
  def (record, thing) = it.graph

  if (debug) fullInput.println mapper.writeValueAsString(thing)
  TypeNormalizer.normalize(thing)
  if (debug) fullOutput.println mapper.writeValueAsString(thing)

  if ('instanceOf' in thing) {
    // TODO: if instance and work codepend; fetch work and normalize that in tandem;
    // if so, store work id in mem to avoid converting again?
    // (or is the mem-overhead worse than read(+attempting to convert) twice?)
    if (ID !in thing.instanceOf) {
      TypeNormalizer.normalize(thing.instanceOf)
    } else {
      def workId = thing.instanceOf[ID]
      if (workId !in seenWorks) {
        seenWorks << workId

        //selectByIds([thing.instanceOf[ID]]) {
        //}
        def workRecordData = load(workId)
        def work = workRecordData[GRAPH][1]

        if (debug) fullInput.println mapper.writeValueAsString(work)
        TypeNormalizer.normalize(work)
        if (debug) fullOutput.println mapper.writeValueAsString(work)
      }
    }
  }

  //it.scheduleSave()
}
