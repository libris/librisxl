import java.util.stream.Collectors
import java.util.regex.Pattern

import whelk.Whelk
import static whelk.util.Jackson.mapper

interface UsingJsonKeys {
    static final var ID = '@id'
    static final var TYPE = '@type'
    static final var VALUE = '@value'
    static final var ANNOTATION = '@annotation'
}

class TypeMappings implements UsingJsonKeys {

    static final var MARC = "https://id.kb.se/marc/"
    static final var SAOGF = "https://id.kb.se/term/saogf/"
    static final var BARNGF = "https://id.kb.se/term/barngf/"
    static final var KBRDA = "https://id.kb.se/term/rda/"

    static final var KTG = "https://id.kb.se/term/ktg/"

    Whelk whelk

    Map cleanupInstanceTypes = [
      'SoundRecording': [category: 'https://id.kb.se/term/ktg/SoundStorageMedium', workCategory: 'https://id.kb.se/term/ktg/Audio'],  // 170467
      'VideoRecording': [category: 'https://id.kb.se/term/ktg/VideoStorageMedium', workCategory: 'https://id.kb.se/term/ktg/MovingImage'],  // 20515
      'Map': [category: 'https://id.kb.se/term/rda/Sheet', workCategory: 'https://id.kb.se/rda/CartographicImage'],  // 12686
      'Globe': [category: 'https://id.kb.se/term/rda/Object', workCategory: 'https://id.kb.se/term/ktg/Globe'],  // 74
      'StillImageInstance': [category: 'https://id.kb.se/term/rda/Sheet', workCategory: 'https://id.kb.se/term/ktg/StillImage'], // 54954
      'TextInstance': [category: 'https://id.kb.se/term/rda/Volume' , workCategory: 'https://id.kb.se/rda/Text'], // 301
    ]

    Map<String, String> typeToCategory = [:]
    Map<String, String> preferredCategory = [:]
    Map<String, List<String>> categoryMatches = [:]

    TypeMappings(Whelk whelk, File scriptDir) {
        // TODO: Replace this generated json (see makemappings.groovy) by runtime that logic on startup!
        var f = new File(scriptDir, "mappings.json")
        Map mappings = mapper.readValue(f, Map)

        typeToCategory = mappings.get('typeToCategory')
        preferredCategory = mappings.get('preferredCategory')
        categoryMatches = mappings.get('categoryMatches')

        assert isImpliedBy([(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/rda/Volume'])
        assert isImpliedBy([(ID): 'https://id.kb.se/term/rda/Volume'], [(ID): 'https://id.kb.se/term/ktg/PrintedVolume'])
        assert isImpliedBy([(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/ktg/PrintedVolume'])
        assert isImpliedBy([(ID): 'https://id.kb.se/term/rda/Sheet'], [(ID): 'https://id.kb.se/term/ktg/PrintedSheet'])

        assert reduceSymbols([[(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/rda/Volume']]) == [[(ID): 'https://id.kb.se/term/rda/Volume']]
        assert reduceSymbols([[(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/ktg/PrintedVolume']]) == [[(ID): 'https://id.kb.se/term/ktg/PrintedVolume']]
        assert reduceSymbols([['@id':'https://id.kb.se/marc/Print'], ['@id':'https://id.kb.se/term/ktg/PrintedSheet'], ['@id':'https://id.kb.se/term/rda/Unmediated'], ['@id':'https://id.kb.se/term/rda/Sheet']]) == [['@id':'https://id.kb.se/term/ktg/PrintedSheet']]
    }

    boolean matches(ArrayList<Map> typelikes, String value) {
        for (Map ref : typelikes) {
            if ((ref.containsKey(ID) && ref.get(ID).contains(value))) {
                return true
            }
        }
        return false
    }

    // Any kind of broad/matches base...
    boolean isImpliedBy(Object x, Object y) {
        String xId = x instanceof Map ? x[ID] : x
        String yId = y instanceof Map ? y[ID] : y

        if (xId == yId) {
            return true
        }

        List bases = categoryMatches[yId]
        for (var base in bases) {
            if (isImpliedBy(xId, base)) {
                return true
            }
        }

        return false
    }

    List reduceSymbols(List symbols) {
        symbols = symbols.stream()
                .map(x -> preferredCategory.containsKey(x[ID]) ? [(ID): preferredCategory[x[ID]]] : x)
                .collect(Collectors.toList())

        Set mostSpecific = symbols.stream()
                .filter(x -> !(symbols.stream().anyMatch(y -> x[ID] != y[ID] && isImpliedBy(x, y))))
                .collect(Collectors.toSet())

        return mostSpecific.toList()
    }

    boolean anyImplies(List<Map> categories, String symbol) {
        for (var category in categories) {
            if (isImpliedBy(symbol, category)) {
                return true
            }
        }
        return false
    }

    /**
     * Replace complex legacy types with structures that this algorithm will subsequently normalize.
     */
    boolean fixMarcLegacyType(Map instance, Map work) {
        var changed = false

        var itype = (String) instance[TYPE]
        def mappedCategory = typeToCategory[itype]
        if (mappedCategory) {
            instance.get('carrierType', []) << [(ID): mappedCategory]
            changed = true
        } else {
            var mappedTypes = cleanupInstanceTypes[itype]
            if (mappedTypes) {
                instance.get('carrierType', []) << [(ID): mappedTypes.category]
                work.get('genreForm', []) << [(ID): mappedTypes.workCategory]
                changed = true
            }
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

        // TODO: Decide if we want to keep componentPart as work type!
        if (issuancetype == 'SerialComponentPart') {
            issuancetype = 'ComponentPart'
        }

        work[TYPE] = issuancetype

        if (issuancetype == 'Monograph' || issuancetype == 'Integrating') {
            instance['issuanceType'] = 'SingleUnit'
        } else if (issuancetype == 'ComponentPart') {
            // TODO: or remove and add "isPartOf": {"@type": "Resource"} unless implied?
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

    static MARC = TypeMappings.MARC
    static SAOGF = TypeMappings.SAOGF
    static KBRDA = TypeMappings.KBRDA
    static KTG = TypeMappings.KTG

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

        // FIXME: recursively normalize all subnodes (hasPart, relatedTo, etc.)!

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
            // TODO: replace or map to ktg:MixedMaterial ?
        } else {
            def mappedCategory = mappings.typeToCategory[wtype]
            // assert mappedCategory, "Unable to map ${wtype} to contentType or genreForm"
            if (mappedCategory) assert mappedCategory instanceof String
            if (mappedCategory) {
                work.get('contentType', []) << [(ID): mappedCategory]
            }
        }

        var contentTypes = mappings.reduceSymbols(asList(work.get("contentType")))
        var workGenreForms = mappings.reduceSymbols(asList(work.get("genreForm")))

        if (workGenreForms.removeIf { !it[ID] && it['prefLabel'] == 'DAISY' }) {
            workGenreForms << [(ID): KTG + 'Audiobook']
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
            if (contentTypes.size() > 0) {
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
        List mediaTypes = mappings.reduceSymbols(asList(instance["mediaType"]))
        List carrierTypes = mappings.reduceSymbols(asList(instance["carrierType"]))

        // TODO: Remove *all* uses of itype here? See e.g. AbstractElectronic
        // below, which should already have been computed, making this check
        // obsolete. OTOH, the fixMarcLegacyType does this, which might be a
        // step too far?

        // Store information about the old instanceType
        // In the case of volume, it may also be inferred
        var isElectronic = itype == "Electronic"

        // ----- Section: set Simple instanceType -----
        /**
         * The part right below applies the new simple instance types Digital/Physical
         */
        // If the resource is electronic and has at least on carrierType that contains "Online"
        if ((isElectronic && mappings.matches(carrierTypes, "Online"))) {
            // TODO Understand what is going on here - can we move to cleanup section?
            // Is the purpose of this to make sure that 1) there is minimal duplication between instanceType and carrierType,
            // but that there is always a carrier type (even if this means duplication)?
            carrierTypes = carrierTypes.findAll { !it.getOrDefault(ID, "").contains("Online") }
            if (carrierTypes.size() == 0) {
                carrierTypes << [(ID): KBRDA + 'OnlineResource']
            }

            // Apply new instance types DigitalResource and PhysicalResource
            // TODO For readability, could this happen before the carrier/media type cleanup?
            //  Old instancetype is already stored in itype
            instance.put(TYPE, "DigitalResource")
            changed = true
        } else {
            instance.put(TYPE, "PhysicalResource")
            changed = true
        }

        // ----- Section: clean up GF and carrierType -----

        // NOTE: We will put non-RDA "carriers" in instanceGenreForms...
        List instanceGenreForms = asList(instance.get("genreForm"))

        var isBraille = dropRedundantString(instance, "marc:mediaTerm", ~/(?i)punktskrift/)
        var isTactile = itype == "Tactile"
        var isVolume = mappings.matches(carrierTypes, "Volume") || looksLikeVolume(instance)

        // Remove old Braille terms and replace them with KTG terms
        var toDrop = [KBRDA + "Volume", MARC + "Braille", MARC + "TacMaterialType-b"] as Set
        var nonBrailleCarrierTypes = carrierTypes.findAll { !toDrop.contains(it.get(ID)) }
        if (nonBrailleCarrierTypes.size() < carrierTypes.size()) {
            isBraille = true
            carrierTypes = nonBrailleCarrierTypes
        }

        if (isTactile && isBraille) {
            if (isVolume) {
                instanceGenreForms << [(ID): KTG + 'BrailleVolume']
            } else {
                instanceGenreForms << [(ID): KTG + 'BrailleResource']
            }
            changed = true
        }

        // NOTE: Replacing unlinked "E-böcker" with linked, tentative kbgf:EBook
        if (instanceGenreForms.removeIf { it['prefLabel'] == 'E-böcker'}) {
          instanceGenreForms << [(ID): KTG + 'EBook']
          changed = true
        }

        var isSoundRecording = mappings.anyImplies(carrierTypes, 'https://id.kb.se/term/ktg/SoundStorageMedium')
        var isVideoRecording = mappings.anyImplies(carrierTypes, 'https://id.kb.se/term/ktg/VideoStorageMedium')

        // If an instance has a certain (old) type which implies physical electronic carrier
        // and carrierTypes corroborating this:
        if (mappings.anyImplies(carrierTypes, 'https://id.kb.se/term/ktg/AbstractElectronic')) {
            // TODO: if PhysicalResource, precisize category to ElectronicStorageMedium ...
            isElectronic = true // assume it is electronic
        }

        // If old instance type is "instance" and there is a carrierType that contains "Electronic"
        // Assume that it is electronic
        if (instance.get(TYPE) == "Instance") {
            if ((carrierTypes.size() == 1 && mappings.matches(carrierTypes, "Electronic"))) {
                isElectronic = true
            }
        }

        var probablyPrint = assumedToBePrint(instance)

        // Remove redundant MARC mediaTerms if the information is implied by computed category:
        if (isSoundRecording && dropRedundantString(instance, "marc:mediaTerm", ~/(?i)ljudupptagning/)) {
            changed = true
        }

        if (isVideoRecording && dropRedundantString(instance, "marc:mediaTerm", ~/(?i)videoupptagning/)) {
            changed = true
        }

        if (isElectronic && dropRedundantString(instance, "marc:mediaTerm", ~/(?i)elektronisk (resurs|utgåva)/)) {
            changed = true
        }

        // TODO: instead, for Monograph, fold overlapping categories into common specific category...
        // e.g. [Print, Volume] => PrintedVolume
        if (!isElectronic) {

            if (itype == "Print") {
                if (isVolume) {
                    instanceGenreForms << [(ID): KTG + 'PrintedVolume']
                } else {
                    instanceGenreForms << [(ID): KTG + 'Print']
                }
                changed = true
            } else if (itype == "Instance") {
                if (isVolume) {
                    if (probablyPrint) {
                        instanceGenreForms << [(ID): KTG + 'PrintedVolume']
                        changed = true
                        // TODO: if marc:RegularPrintReproduction, add production a Reproduction?
                    } else {
                        instanceGenreForms << [(ID): KBRDA + 'Volume']
                        changed = true
                    }
                } else {
                    if (probablyPrint) {
                        if (mappings.matches(carrierTypes, "Sheet")) {
                            instanceGenreForms << [(ID): KTG + 'PrintedSheet']
                        } else {
                            instanceGenreForms << [(ID): KTG + 'Print'] // TODO: may be PartOfPrint ?
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
            List<Map> categories = []

            categories += instanceGenreForms
            categories += mediaTypes
            categories += carrierTypes

            instance.remove("genreForm")
            instance.remove("carrierType")
            instance.remove("mediaType")

            if (categories.size() > 0) {
                categories = mappings.reduceSymbols(categories)
                instance.put("category", categories)
                changed = true
            }
        } else {
            // Remove the mediaType if it can be inferred by the carrierType
            var impliedMediaIds = carrierTypes.findResults { mappings.categoryMatches[it[ID]] }.flatten() as Set
            if (mediaTypes.every { it[ID] in impliedMediaIds }) {
                instance.remove("mediaType")
                changed = true
            }
            // FIXME: If it cannot be inferred from the carrierType, do we want to put it back?
            var explicitMediaTypes = mediaTypes.findAll { it[ID] !in impliedMediaIds }
            if (explicitMediaTypes.size() > 0) {
              instance.put("mediaType", explicitMediaTypes)
            }

            if (carrierTypes.size() > 0) {
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
