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
    Map cleanupTypes = [:]

    Map typeToContentType = [:]
    Map carrierMediaMap = [:]

    // TODO: Unused; COULD be used to add more concrete category (which MAY be required for revert?)
    Map genreFormImpliesFormMap = [:]
    Map complexTypeMap = [:]

    // Mixes subclasses and subconcepts
    Map<String, Set<String>> baseEqualOrSubMap = [
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
                    instance['carrierType'] = [[(ID): MARC + 'Sheet']]
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

    /**
     * - Replace deprecated issuanceType values
     * - Assign issuanceType value to work type
     * - Assign either "SingleUnit" or "MultipelUnit" to instance issuanceType
     */
    boolean convertIssuanceType(Map instance, Map work) {
        // TODO: check genres and heuristics (some Serial are mistyped!)
        var issuancetype = (String) instance.remove("issuanceType")
        if (!issuancetype) {
            return false
        }

        if (issuancetype == 'SubCollection') {
            issuancetype = 'Collection'
        }

        // FIXME: Decide if we want to keep componentPart as work type!
        if (issuancetype == 'SerialComponentPart') {
            issuancetype = 'ComponentPart'
        }

        work[TYPE] = issuancetype

        if (issuancetype == 'Monograph' || issuancetype == 'Integrating') {
            instance['issuanceType'] = 'SingleUnit'
        } else if (issuancetype == 'ComponentPart') {
            // FIXME: or remove and add "isPartOf": {"@type": "Resource"} unless implied?
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

    /** Normalization action */
    boolean normalize(Map instance, Map work) {
        var changed = false

        changed |= mappings.fixMarcLegacyType(instance, work)

        changed |= simplifyWorkType(work)
        changed |= simplfyInstanceType(instance)

        changed |= mappings.convertIssuanceType(instance, work)

        if (changed) {
            reorderForReadability(work)
            reorderForReadability(instance)
        }

        return changed
    }

    /**
     * Work action.
     * - Retain information from old work type by assigning a mapped contentType or genreForm.
     * - Reduce redundancy by removing genreForms and contentTypes that can be inferred from more specific ones.
     * - Optionally, replace properties contentType and genreForm with new property category.
     */
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
            if (!work['contentType'].any { it[ID].startsWith(KBRDA + 'Cartographic') }) {
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

        var contentTypes = mappings.reduceSymbols(asList(work.get("contentType")))
        var workGenreForms = mappings.reduceSymbols(asList(work.get("genreForm")))

        if (workGenreForms.removeIf { !it[ID] && it['prefLabel'] == 'DAISY' }) {
            workGenreForms << [(ID): KBGF + 'Audiobook']
        }

        if (addCategory) {
            List<String> categories = []
            categories += workGenreForms + contentTypes
            work.remove("genreForm")
            work.remove("contentType")

            if (categories.size() > 0) {
                // Further reduce, e.g. removing contentTypes implied by workGenreForms.
                work.put("category", mappings.reduceSymbols(categories))
                changed = true
            }
        } else {
            // Put back the reduced GFs and contentTypes
            if (workGenreForms.size() > 0) {
              work.put("genreForm", workGenreForms)
            }
            if (contentType.size() > 0) {
              work.put("contentType", contentTypes)
            }
        }

        return changed
    }

    /**
     * - Clean up and enrich carrierType and mediaType
     * - Assign one of instance types PhysicalResource/DigitalResource
     * - Clean up and enrich genreForm, carrierType and mediaType again. TODO: Can we do this just once?
     */
    boolean simplfyInstanceType(Map instance) {
        var changed = false

        var itype = instance.get(TYPE)

        // Only keep the most specific mediaTypes and carrierTypes
        List mediaTypes = mappings.reduceSymbols(asList(instance["mediaType"]), "MediaType")
        List carrierTypes = mappings.reduceSymbols(asList(instance["carrierType"]), "CarrierType")

        // Remove the mediaType if it can be inferred by the carrierType
        // FIXME: If it cannot be inferred from the carrierType, do we want to put it back?
        var impliedMediaIds = carrierTypes.findResults { mappings.carrierMediaMap[it[ID]] } as Set
        if (mediaTypes.every { it[ID] in impliedMediaIds }) {
            instance.remove("mediaType")
            changed = true
        }

        // Store information about the old instanceType
        // In the case of volume, it may also be inferred
        var isElectronic = itype == "Electronic"
        var isSoundRecording = itype == "SoundRecording"
        var isVideoRecording = itype == "VideoRecording"
        var isTactile = itype == "Tactile"

        var isVolume = mappings.matches(carrierTypes, "Volume") || looksLikeVolume(instance)

        // ----- Section: set Simple instanceType -----
        /**
         * The part right below applies the new simple instance types Digital/Physical
         */
        // If the resource is electronic and has at least on carrierType that contains "Online"
        if ((isElectronic && mappings.matches(carrierTypes, "Online"))) {
            // FIXME Understand what is going on here - can we move to cleanup section?
            // Is the purpose of this to make sure that 1) there is minimal duplication between instanceType and carrierType,
            // but that there is always a carrier type (even if this means duplication)?
            carrierTypes = carrierTypes.findAll { !it.getOrDefault(ID, "").contains("Online") }
            if (carrierTypes.size() == 0) {
                carrierTypes << [(ID): KBRDA + 'OnlineResource']
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

        // ----- Section: clean up GF and carrierType -----

        // NOTE: We will put non-RDA "carriers" in instanceGenreForms...
        List instanceGenreForms = instance.get("genreForm"))

        var isBraille = dropRedundantString(instance, "marc:mediaTerm", ~/(?i)punktskrift/)

        // Remove old Braille terms and replace them with KBGF terms
        var toDrop = [KBRDA + "Volume", MARC + "Braille", MARC + "TacMaterialType-b"] as Set
        var nonBrailleCarrierTypes = carrierTypes.findAll { !toDrop.contains(it.get(ID)) }
        if (nonBrailleCarrierTypes.size() < carrierTypes.size()) {
            isBraille = true
            carrierTypes = nonBrailleCarrierTypes
        }

        if (isTactile && isBraille) {
            if (isVolume) {
                instanceGenreForms << [(ID): KBGF + 'BrailleVolume']
            } else {
                instanceGenreForms << [(ID): KBGF + 'BrailleResource']
            }
            changed = true
        }

        // NOTE: Replacing unlinked "E-böcker" with linked, tentative kbgf:EBook
        if (instanceGenreForms.removeIf { it['prefLabel'] == 'E-böcker'}) {
          instanceGenreForms << [(ID): KBGF + 'EBook']
          changed = true
        }

        // If an instance has a certain (old) type which implies physical electronic carrier
        // and carrierTypes corroborating this, assume it is electronic
        // TODO: should work with regular reduceSymbols ...
        var carrierTuples = [
                [isElectronic, [KBRDA + 'ComputerChipCartridge']],
                [isSoundRecording, [MARC + 'SoundDisc', KBRDA + 'AudioDisc']],
                [isSoundRecording, [MARC + 'SoundCassette', KBGF + 'AudioCassette']], // FIXME [680ba995]: Not defined in KBRDA.
                [isVideoRecording, [MARC + 'VideoDisc', MARC + 'VideoMaterialType-d', KBRDA + 'Videodisc']]
        ]
        for (carrierTuple in carrierTuples) {
            var (isIt, matchTokens) = carrierTuple
            var gotMatches = matchTokens.findAll { mappings.matches(carrierTypes, it) }
            if (isIt && gotMatches.size() > 0) {
                isElectronic = true
                // FIXME: see 680ba995.
                carrierTypes << [(ID): matchTokens[-1]]
                changed = true
            }
        }

        // If old instance type is "instance" and there is a carrierType that contains "Electronic"
        // Assume that it is electronic
        if (instance.get(TYPE) == "Instance") {
            if ((carrierTypes.size() == 1 && mappings.matches(carrierTypes, "Electronic"))) {
                isElectronic = true
            }
        }

        var probablyPrint = assumedToBePrint(instance)

        // TODO: is this handled by reduceSymbols? Also, probablyPrint?
        //instanceGenreForms.findAll { it[ID] != MARC + 'Print' }

        // Remove redundant MARC mediaTerms if the information is given by the old itype
        if (isSoundRecording && dropRedundantString(instance, "marc:mediaTerm", ~/(?i)ljudupptagning/)) {
            changed = true
        }

        if (isVideoRecording && dropRedundantString(instance, "marc:mediaTerm", ~/(?i)videoupptagning/)) {
            changed = true
        }

        if (isElectronic && if (dropRedundantString(instance, "marc:mediaTerm", ~/(?i)elektronisk (resurs|utgåva)/)) {
            changed = true
        }

        if (!isElectronic) {

            if (itype == "Print") {
                if (isVolume) {
                    instanceGenreForms << [(ID): KBGF + 'PrintedVolume']
                } else {
                    instanceGenreForms << [(ID): KBGF + 'Print']
                }
                changed = true
            } else if (itype == "Instance") {
                if (isVolume) {
                    if (probablyPrint) {
                        instanceGenreForms << [(ID): KBGF + 'PrintedVolume']
                        changed = true
                        // TODO: if marc:RegularPrintReproduction, add production a Reproduction?
                    } else {
                        instanceGenreForms << [(ID): KBRDA + 'Volume']
                        changed = true
                    }
                } else {
                    if (probablyPrint) {
                        if (mappings.matches(carrierTypes, "Sheet")) {
                            instanceGenreForms << [(ID): KBGF + 'PrintedSheet']
                        } else {
                            instanceGenreForms << [(ID): KBGF + 'Print'] // TODO: may be PartOfPrint ?
                        }
                        changed = true
                    } else {
                        if (mappings.matches(carrierTypes, "Sheet")) {
                            instanceGenreForms << [(ID): KBRDA + 'Sheet']
                            if (dropRedundantString(instance, "marc:mediaTerm", ~/(?i)affisch/)) {
                                instanceGenreForms << [(ID): SAOGF + 'Poster']
                            }
                            changed = true
                        }
                    }
                }
            }

        }

        if (addCategory) {
            List<String> categories = []

            categories += instanceGenreForms
            categories += mediaTypes
            categories += carrierTypes

            instance.remove("genreForm")
            instance.remove("carrierType")

            if (categories.size() > 0) {
                instance.put("category", mappings.reduceSymbols(categories))
                changed = true
            }
        } else {
            var explicitMediaTypes = mediaTypes.findAll { it[ID] !in impliedMediaIds }
            if (explicitMediaTypes.size() > 0) {
              instance.put("mediaType", explicitMediaTypes)
            }
            if (carrierType.size() > 0) {
              instance.put("carrierType", carrierTypes)
            }
            if (instanceGenreForms.size() > 0) {
              instance.put("genreForm", instanceGenreForms)
            }
          }

        return changed
    }


    // ----- Typenormalizer helper methods -----
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

    static boolean dropRedundantString(Map instance, String propertyKey, Pattern pattern) {
        if (instance.get(propertyKey)?.matches(pattern)) {
            instance.remove(propertyKey)
            return true
        }
        return false
    }

    static List asList(Object o) {
        return (o instanceof List ? (List) o : o == null ? [] : [o])
    }

    protected static void reorderForReadability(Map thing) {
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
    System.out.println("Normalizing using property: category")
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
