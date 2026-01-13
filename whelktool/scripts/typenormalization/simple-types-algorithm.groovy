import whelk.util.DocumentUtil

import java.util.stream.Collectors
import java.util.regex.Pattern

import whelk.Whelk
import whelk.TypeCategoryNormalizer

import static whelk.JsonLd.TYPE_KEY
import static whelk.util.Jackson.mapper
import static whelk.JsonLd.isLink

def missingCategoryLog = getReportWriter("missing_category_log.tsv")

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
    boolean replaceIssuanceTypes = Boolean.parseBoolean(System.getProperty("replaceIssuanceTypes")) ?: false

    Whelk whelk

    // TODO Should Sound & Video storage medium -- here and further down! -- be replaced with exact matches RDA media type audio and video?
    Map cleanupInstanceTypes = [
      'SoundRecording': [category: 'https://id.kb.se/term/ktg/SoundStorageMedium', workCategory: 'https://id.kb.se/term/ktg/Audio'],  // 170467
      'VideoRecording': [category: 'https://id.kb.se/term/ktg/VideoStorageMedium', workCategory: 'https://id.kb.se/term/ktg/MovingImage'],  // 20515
      'Map': [category: 'https://id.kb.se/term/rda/Sheet', workCategory: 'https://id.kb.se/term/rda/CartographicImage'],  // 12686
      'Globe': [category: 'https://id.kb.se/term/rda/Object', workCategory: 'https://id.kb.se/term/ktg/Globe'],  // 74
      'StillImageInstance': [category: 'https://id.kb.se/term/rda/Sheet', workCategory: 'https://id.kb.se/term/rda/StillImage'], // 54954
      'TextInstance': [category: 'https://id.kb.se/term/rda/Volume' , workCategory: 'https://id.kb.se/term/rda/Text'], // 301
    ]

    Map<String, String> typeToCategory = [:]
    Map<String, String> preferredCategory = [:]
    Map<String, List<String>> categoryMatches = [:]

    TypeMappings(Whelk whelk, File scriptDir) {
        // Store reference to Whelk so other helpers can use it
        this.whelk = whelk

        var catTypeNormalizer = new TypeCategoryNormalizer(whelk.resourceCache)
        typeToCategory = catTypeNormalizer.typeToCategory
        preferredCategory = catTypeNormalizer.preferredCategory
        categoryMatches = catTypeNormalizer.categoryMatches

        // TODO Adjust this logic to always add carrier types as separate terms?
        assert isImpliedBy([(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/rda/Volume'])

        // Commented out references to composite ktg instance categories
        // assert isImpliedBy([(ID): 'https://id.kb.se/term/rda/Volume'], [(ID): 'https://id.kb.se/term/ktg/PrintedVolume'])
        // assert isImpliedBy([(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/ktg/PrintedVolume'])
        // assert isImpliedBy([(ID): 'https://id.kb.se/term/rda/Sheet'], [(ID): 'https://id.kb.se/term/ktg/PrintedSheet'])

        assert reduceSymbols([[(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/rda/Volume']]) == [[(ID): 'https://id.kb.se/term/rda/Volume']]

        // Commented out references to composite ktg instance categories
        // assert reduceSymbols([[(ID): 'https://id.kb.se/term/rda/Unmediated'], [(ID): 'https://id.kb.se/term/ktg/PrintedVolume']]) == [[(ID): 'https://id.kb.se/term/ktg/PrintedVolume']]
        // assert reduceSymbols([['@id':'https://id.kb.se/marc/Print'], ['@id':'https://id.kb.se/term/ktg/PrintedSheet'], ['@id':'https://id.kb.se/term/rda/Unmediated'], ['@id':'https://id.kb.se/term/rda/Sheet']]) == [['@id':'https://id.kb.se/term/ktg/PrintedSheet']]

        // Commented out implications between RDA and SAOGF
        // assert isImpliedBy([(ID):'https://id.kb.se/term/rda/StillImage'], [(ID):'https://id.kb.se/term/saogf/Bilder'])
        // assert !isImpliedBy([(ID):'https://id.kb.se/term/saogf/Bilder'], [(ID):'https://id.kb.se/term/rda/StillImage'])
        // assert reduceSymbols([[(ID):'https://id.kb.se/term/rda/StillImage'], [(ID):'https://id.kb.se/term/saogf/Bilder']]) == [[(ID):'https://id.kb.se/term/saogf/Bilder']]
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
    boolean isImpliedBy(Object x, Object y, Set visited = new HashSet()) {
        String xId = x instanceof Map ? x[ID] : x
        String yId = y instanceof Map ? y[ID] : y

        if (xId == null || yId == null) {
          return false
        }

        if (xId == yId) {
            return true
        }

        visited << yId

        List bases = categoryMatches[yId]
        for (var base in bases) {
            if (base !in visited) {
              if (isImpliedBy(xId, base, visited + new HashSet())) {
                  return true
              }
            }
        }

        return false
    }

    List reduceSymbols(List symbols) {
        symbols = symbols.stream()
                .map(x -> x.containsKey(ID) && preferredCategory.containsKey(x[ID]) ? [(ID): preferredCategory[x[ID]]] : x)
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
    boolean fixMarcLegacyInstanceType(Map instance, Map work) {
        var changed = false

        var itype = (String) instance[TYPE]
        def mappedCategory = itype ? typeToCategory[itype] : null
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

        // If there is no issuanceType, set type to the generic Work
        if (!issuancetype) {
            // Special handling for Signe works with issuanceType as work property
            var workIssuanceType = (String) work.remove("issuanceType")
            if (workIssuanceType) {
                issuancetype = workIssuanceType
            } else {
                issuancetype = 'Work'
            }
        }

        // Already new type of value == already normalized:
        if (issuancetype == 'SingleUnit' || issuancetype == 'MultipleUnits') {
            return false
        }

        if (issuancetype == 'SubCollection') {
            issuancetype = 'Collection'
        }

        // Remove ComponentPart as a work/issuance type, retaining the information with an instance category
        if (issuancetype == 'SerialComponentPart' || issuancetype == 'ComponentPart') {
            instance.get('category', []) << [(ID): KTG + 'ComponentPart']
            issuancetype = 'Monograph'
        }

        // Set the new work type
        work[TYPE] = issuancetype

        if (replaceIssuanceTypes) {
            // Add new values to issuanceTyoe
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
        }

        return true
    }

}

class TypeNormalizer implements UsingJsonKeys {

    //Parse system property "addCategory" and "replaceIssuanceTypes" as a boolean, default to false if none given
    boolean addCategory = Boolean.parseBoolean(System.getProperty("addCategory")) ?: false

    static MARC = TypeMappings.MARC
    static SAOGF = TypeMappings.SAOGF
    static KBRDA = TypeMappings.KBRDA
    static KTG = TypeMappings.KTG

    TypeMappings mappings
    def missingCategoryLog


    TypeNormalizer(TypeMappings mappings, missingCategoryLog) {
        this.mappings = mappings
        this.missingCategoryLog = missingCategoryLog
    }

    /** Normalization action */
    boolean normalize(Map instance, Map work, boolean recursive = true) {
        var changed = false

        var oldItype = instance.get(TYPE)
        var oldWtype = work.get(TYPE)

        changed |= mappings.fixMarcLegacyInstanceType(instance, work)
        changed |= simplifyInstanceType(instance)
        changed |= moveInstanceGenreFormsToWork(instance, work)
        changed |= simplifyWorkType(work)



        changed |= mappings.convertIssuanceType(instance, work)

        if (recursive) {
            changed |= normalizeLocalEntity(instance)
            changed |= normalizeLocalEntity(work)
        }

        if (changed) {
            reorderForReadability(work)
            reorderForReadability(instance)
        }
        if (!instance.category || instance.category.isEmpty()){
            missingCategoryLog.println("${instance[ID]}\tNo INSTANCE categories after reducing work / instance types\t${oldWtype} / ${oldItype}")
        }
        if (!work.category || work.category.isEmpty()){
            missingCategoryLog.println("${instance[ID]}\tNo WORK categories after reducing work / instance types\t${oldWtype} / ${oldItype}")
        }

        return changed
    }

    boolean normalizeLocalEntity(Map entity) {
        boolean changed = false
        DocumentUtil.traverse(entity) { value, path ->
            if (!path.isEmpty() && !path.contains('instanceOf') && !path.contains('hasInstance')) {
                // If the part is a Work or subclass thereof
                if (value instanceof Map && mappings.whelk.jsonld.isSubClassOf(value.get(TYPE), "Work")) {
                    changed |= normalize([:], value, false)
                }
                // If the part is an instance or subclass thereof
                else if (value instanceof Map && mappings.whelk.jsonld.isSubClassOf(value.get(TYPE), "Instance")) {
                    changed |= normalize(value, [:], false)

                    // Special handling for when the part is Electronic
                    if ((value.get(TYPE) == "PhysicalResource") && (entity.get(TYPE) == "DigitalResource")) {
                        value.put(TYPE, "DigitalResource")
                        if (value.category.size() == 0) {
                            value.category.removeAll { it['@id'] == "https://id.kb.se/term/ktg/ElectronicStorageMedium" }
                            value.remove("category")
                        }
                        changed = true
                    }
                }
            }
            return new DocumentUtil.Nop()
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

        if (wtype == 'ManuscriptText') {
            work.get('genreForm', []) << [(ID): SAOGF + 'Handskrifter']
        }
        else if (wtype == 'ManuscriptNotatedMusic') {
            work.get('genreForm', []) << [(ID): SAOGF + 'Handskrifter']
            work.get('contentType', []) << [(ID): KBRDA + 'NotatedMusic']
        }
        else if (wtype == 'Cartography') {
            if (!work['contentType'].any { it[ID]?.startsWith(KBRDA + 'Cartographic') }) {
                work.get('contentType', []) << [(ID): KBRDA + 'CartographicImage'] // TODO: good enough guess?
            }
        }
        else {
            def mappedCategory = wtype ? mappings.typeToCategory[wtype] : null
            //assert mappedCategory, "Unable to map ${wtype} to contentType or genreForm"
            if (mappedCategory) {
                assert mappedCategory instanceof String
                if (mappedCategory) {
                    work.get('contentType', []) << [(ID): mappedCategory]
                }
            }
        }

        var contentTypes = mappings.reduceSymbols(asList(work.get("contentType")))
        var workGenreForms = mappings.reduceSymbols(asList(work.get("genreForm")))

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
     * - Assign one of instance types PhysicalResource/DigitalResource
     * - Clean up and enrich genreForm, carrierType and mediaType again
     */
    boolean simplifyInstanceType(Map instance) {
        var changed = false

        var itype = instance.get(TYPE)

        // Only keep the most specific mediaTypes and carrierTypes
        List mediaTypes = mappings.reduceSymbols(asList(instance["mediaType"]))
        List carrierTypes = mappings.reduceSymbols(asList(instance["carrierType"]))
        // Fetch any GF terms
        List instanceGenreForms = asList(instance.get("genreForm"))

        // FIXME: What do the comments below mean? Still relevant?
        // TODO: Remove *all* uses of itype here? See e.g. AbstractElectronic
        // below, which should already have been computed, making this check obsolete.
        // OTOH, the fixMarcLegacyInstanceType does this, which might be a step too far?

        // Store information about the old instanceType
        // In the case of volume, it may also be inferred
        var isElectronic = itype == "Electronic"
        var isVolume = mappings.matches(carrierTypes, "Volume") || looksLikeVolume(instance)

        // If an instance has a certain (old) type which implies physical electronic carrier
        // and carrierTypes corroborating this:
        if (mappings.anyImplies(carrierTypes, 'https://id.kb.se/term/ktg/AbstractElectronic')) {
            isElectronic = true // assume it is electronic
        }

        // FIXME When would the below be true?
        // If old instance type is "instance" and there is a carrierType that contains "Electronic"
        // Assume that it is electronic
        if (instance.get(TYPE) == "Instance") {
            if ((carrierTypes.size() == 1 && mappings.matches(carrierTypes, "Electronic"))) {
                isElectronic = true
            }
        }

        // ----- Section: Set new instanceType -----
        /**
         * The part right below applies the new simple instance types Digital/Physical
         */
        // If the resource is electronic and has at least on carrierType that contains "Online"
        if ((isElectronic && mappings.matches(carrierTypes, "Online"))) {
            // Apply new instance types DigitalResource and PhysicalResource
            instance.put(TYPE, "DigitalResource")

            // FIXME What is the desired outcome below? Removing RDA "OnlineResource" if there are other carrierTypes?
            // FIXME Find all CarrierTypes expect "Online". If there are none, add "Online".
            // FIXME Commented out since we are being careful about removing RDA terms.
            // Add/clean up carrierTypes
            //carrierTypes = carrierTypes.findAll { !it.getOrDefault(ID, "").contains("Online") }
            //if (carrierTypes.size() == 0) {
            //    carrierTypes << [(ID): KBRDA + 'OnlineResource']
            //}
            //changed = true
        } else {
            instance.put(TYPE, "PhysicalResource")
            changed = true
        }


        // ----- Section: Clean up Tactile and Braille -----
        var isTactile = itype == "Tactile"
        var isBraille = dropRedundantString(instance, "marc:mediaTerm", ~/(?i)punktskrift/)

        var toDrop = [MARC + "Braille", MARC + "TacMaterialType-b"] as Set
        var nonBrailleCarrierTypes = carrierTypes.findAll { !toDrop.contains(it.get(ID)) }
        if (nonBrailleCarrierTypes.size() < carrierTypes.size()) {
            isBraille = true
            carrierTypes = nonBrailleCarrierTypes
        }

        if (isTactile && isBraille) {
            carrierTypes << [(ID): KTG + 'Braille']
            if (isVolume) {
                carrierTypes << [(ID): KBRDA + 'Volume']
            }
            changed = true
        }


        // ----- Section: Clean up Electronic -----

        var isSoundRecording = mappings.anyImplies(carrierTypes, 'https://id.kb.se/term/ktg/SoundStorageMedium')
        var isVideoRecording = mappings.anyImplies(carrierTypes, 'https://id.kb.se/term/ktg/VideoStorageMedium')


        // If something has old itype Electronic and new itype PhysicalResource,
        // we can assume it id an electronic storage medium
        if (isElectronic && (instance.get(TYPE, '') == 'PhysicalResource')) {
            carrierTypes << [(ID): KTG + 'ElectronicStorageMedium']
            changed = true
        }

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
        if (instanceGenreForms.removeIf { it['prefLabel'] == 'E-böcker'}) {
            carrierTypes << [(ID): KTG + 'EBook']
            changed = true
        }


        // ----- Section: Clean up Print and other non-electronic -----

        var probablyPrint = assumedToBePrint(instance)

        if (!isElectronic) {

            // If it is or looks like a volume
            if (isVolume) {
                carrierTypes << [(ID): KBRDA + 'Volume']
                changed = true
            }

            // If its type is Print, or seems like it should be Print
            if (itype == "Print" || probablyPrint) {
                carrierTypes << [(ID): KTG + 'Print']
                changed = true
            }

            // If its type is Instance (very few of those left in Libris)
            if (itype == "Instance") {
                // ALREADY DONE If it is presumably print, add print
                // ALREADY DONE If it is or looks like a volume, add RDA Volume
                // If it is not a volume, add Sheet
                if (!isVolume && mappings.matches(carrierTypes, "Sheet")) {
                    carrierTypes << [(ID): KBRDA + 'Sheet']
                    // Replace local MARC term with linked SAOGF
                    if (dropRedundantString(instance, "marc:mediaTerm", ~/(?i)affisch/)) {
                        carrierTypes << [(ID): SAOGF + 'Poster']
                    }
                    changed = true
                }
            }

        }

//        if (!isElectronic) {
//
//            if (itype == "Print") {
//                if (isVolume) {
//                    carrierTypes << [(ID): KTG + 'PrintedVolume']
//                } else {
//                    carrierTypes << [(ID): KTG + 'Print']
//                }
//                changed = true
//            } else if (itype == "Instance") {
//                if (isVolume) {
//                    if (probablyPrint) {
//                        carrierTypes << [(ID): KTG + 'PrintedVolume']
//                        changed = true
//                        // TODO: if marc:RegularPrintReproduction, add production a Reproduction?
//                    } else {
//                        carrierTypes << [(ID): KBRDA + 'Volume']
//                        changed = true
//                    }
//                } else {
//                    if (probablyPrint) {
//                        // TODO Add Print + Sheet as separate terms?
//                        if (mappings.matches(carrierTypes, "Sheet")) {
//                            carrierTypes << [(ID): KTG + 'PrintedSheet']
//                        } else {
//                            carrierTypes << [(ID): KTG + 'Print'] // TODO: may be PartOfPrint ?
//                        }
//                        changed = true
//                    } else {
//                        if (mappings.matches(carrierTypes, "Sheet")) {
//                            carrierTypes << [(ID): KBRDA + 'Sheet']
//                            if (dropRedundantString(instance, "marc:mediaTerm", ~/(?i)affisch/)) {
//                                carrierTypes << [(ID): SAOGF + 'Poster']
//                            }
//                            changed = true
//                        }
//                    }
//                }
//            }
//
//        }

        if (addCategory) {
            List<Map> categories = []

            categories += mediaTypes
            categories += carrierTypes

            // Remove the ambiguous NARC term Other
            categories.removeAll { it['@id'] == "https://id.kb.se/marc/Other" }

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
            // TODO: Decide in which property we want the instance "genreForms" (including old types)
            if (instanceGenreForms.size() > 0) {
              instance.put("category", instanceGenreForms)
            }
          }

        return changed
    }

    // ----- Typenormalizer helper methods -----
    static boolean moveInstanceGenreFormsToWork (Map instance, Map work) {
        var changed = false
        List instanceGenreFormsToMove = asList(instance.get("genreForm")).findAll() {isLink(it)}
        // Move unlinked instance GFs to instance category
        List unlinkedInstanceGenreForms = asList(instance.remove("genreForm")).findAll() {!isLink(it)}
        List allInstanceCategories = instance.get("category") ?: [] + unlinkedInstanceGenreForms

        if (allInstanceCategories.size() > 0) {
            instance.put("category", allInstanceCategories)
            changed = true
        }

        // Move all other instance GFs to work GF
        if (instanceGenreFormsToMove) {
            work.put("genreForm", work.get("genreForm", []) + instanceGenreFormsToMove)
            changed = true
        }

        return changed
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

    static boolean dropRedundantString(Map instance, String propertyKey, Pattern pattern) {
        def value = instance.get(propertyKey)
        if (value instanceof List && value.size() == 1) {
          value = value[0]
        }
        if (value instanceof String && value.matches(pattern)) {
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

typeNormalizer = new TypeNormalizer(new TypeMappings(getWhelk(), scriptDir), missingCategoryLog)

if (typeNormalizer.addCategory) {
    System.out.println("Normalizing using property: category")
}

process { def doc, Closure loadWorkItem ->
    def (record, mainEntity) = doc.graph

    // If mainEntity contains "instanceOf", it's an instance
    if ('instanceOf' in mainEntity) {
        // Instances and locally embedded works
        if (ID !in mainEntity.instanceOf) {
            def work = mainEntity.instanceOf
            if (work instanceof List && work.size() == 1) {
                work = work[0]
            }

            var changed = typeNormalizer.normalize(mainEntity, work)

            if (changed) doc.scheduleSave()

        } else {
            // Instances and linked works
            def loadedWorkId = mainEntity.instanceOf[ID]
            // TODO: refactor very hacky solution...
            loadWorkItem(loadedWorkId) { workIt ->
                def (workRecord, work) = workIt.graph

                var changed = typeNormalizer.normalize(mainEntity, work)

                if (changed) {
                    doc.scheduleSave()
                    if (loadedWorkId !in convertedWorks) workIt.scheduleSave()
                }
                convertedWorks << loadedWorkId
            }
        }

    }
    // Else if it contains the property 'hasInstance', it's a Signe work that reuqires special handling
     else if ('hasInstance' in mainEntity) {
        var changed = typeNormalizer.normalize([:], mainEntity)
        if (changed) {
            if (mainEntity[ID] !in convertedWorks) doc.scheduleSave()
        }
        convertedWorks << mainEntity[ID]
        }
}
