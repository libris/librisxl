
import json
import sys   

file = sys.argv[1]

print(f"Running tests on normalized records in file: {file}")

with open(file) as f:
	data = [json.loads(line)["@graph"] for line in f]

special_cases = [
		"https://libris-qa.kb.se/test/workNotLinkedFromInstance",
		"https://libris-qa.kb.se/test/workWithIssuanceType"]

for r in data:
	id = r[0].get("@id", "")
	record = r[1]

	try:
		title = record["instanceOf"]["hasTitle"][0]["mainTitle"]
	except:
		# Special case
		title = {record["hasTitle"][0]["mainTitle"]}

	print(f"\n{id}\t{title}")


	work_type = record.get("instanceOf", {}).get("@type", "")
	work_category = record.get("instanceOf", {}).get("category", [])

	instance_type = record.get("@type", "")
	instance_category = record.get("category", [])

	"""Genreic test for leftover obsolete properties"""

	new_work_types = ["Monograph","Serial","Collection","Integrating","Work"]
	new_instance_types = ["PhysicalResource","DigitalResource"]

	
	if id in special_cases:
		print("Special Signe record! Skipping generic tests.")
	else:
		assert work_type in new_work_types, f"Failed! Unexpected work type: {work_type} in work {id}"
		assert instance_type in new_instance_types, f"Failed! Unexpected instance type: {instance_type} in instance {id}"

		old_properties = ["genreForm","contentType","carrierType","mediaType","issuanceType"]
		
		for p in old_properties:
			assert p not in record.get("instanceOf", {}), "Failed! Obsolete property still present in work: {p}"
			assert p not in record, "Failed! Obsolete property still present in instance: {p}"
		

	"""Specific tests for each tailored example"""

	# Verkstyp Text blir RDA Text
	if id == "https://libris-qa.kb.se/test/text":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/Text"}], record

	# Verkstyp Work sparas inte som verkskategori
	elif id == "https://libris-qa.kb.se/test/work":
		assert not work_category

	# Verkstyp MovingImage blir ktg MovingImage
	elif id == "https://libris-qa.kb.se/test/movingimage":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/MovingImage"}], record

	# Verkstyp Audio blir ktg Audio
	elif id == "https://libris-qa.kb.se/test/audio":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Audio"}], record

	# Verkstyp NotatedMusic blir RDA NotatedMusic
	elif id == "https://libris-qa.kb.se/test/notatedmusic":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/NotatedMusic"}], record

	# Verkstyp StillImage blir RDA StillImage
	elif id == "https://libris-qa.kb.se/test/stillimage":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/StillImage"}], record

	# Verkstyp Music blir SAOGF Music
	elif id == "https://libris-qa.kb.se/test/music":
		assert work_category == [{"@id":"https://id.kb.se/term/saogf/Musik"}], record

	# Verkstyp Multimedia blir ktg/Software ?
	# FIXME Lista ut varför mappning säger att denna ska bli ktg/Multimedia när intenrsectionpatterns säger ktg/Software
	elif id == "https://libris-qa.kb.se/test/multimedia":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Software"}], record

	# Verkstyp Cartography blir RDA CartographicImage om ej annan RDA Cartorgaphy-term redan finns
	elif id == "https://libris-qa.kb.se/test/cartography":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/CartographicImage"}], record

	# Verkstyp Kit blir ktg Kit
	elif id == "https://libris-qa.kb.se/test/kit":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Kit"}], record

	# Verkstyp Object blir RDA ThreeDimensionalForm
	elif id == "https://libris-qa.kb.se/test/object":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/ThreeDimensionalForm"}], record

	# Verkstyp ManuscriptText blir SAOGF Handsrkrifter
	elif id == "https://libris-qa.kb.se/test/manuscripttext":
		assert work_category == [{"@id":"https://id.kb.se/term/saogf/Handskrifter"}], record

	# Verkstyp ManuscriptNotatedMusic blir RDA NotatedMusic och SAOGF Handskrifter
	elif id == "https://libris-qa.kb.se/test/manuscriptnotatedmusic":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/NotatedMusic"},{"@id":"https://id.kb.se/term/saogf/Handskrifter",
		}], record

	# Verkstyp MixedMaterial blir ktg MixedMaterial
	elif id == "https://libris-qa.kb.se/test/mixedmaterial":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/MixedMaterial"}], record

	# Verkstyp Dataset blir ktg Dataset
	elif id == "https://libris-qa.kb.se/test/dataset":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Dataset"}], record

	# Verkstyp NonMusicAudio blir RDA Sounds
	# FIXME - Ska det bli Audtio (ktg/Audio) istället för Sounds (rda/Sounds)?
	#elif id == "https://libris-qa.kb.se/test/nonmusicaudio":
	#	assert work_category == [{"@id":"https://id.kb.se/term/rda/Sounds"}], record

	# Verkstyp saknas i mappning - skriv ut i logg!
	elif id == "https://libris-qa.kb.se/test/unhandledworktype":
		pass

	# Instanstyp Instance och verkstyp Text -> instanstyp PhysicalResource och verkskategori Text
	elif id == "https://libris-qa.kb.se/test/instance":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/Text"}], record
		assert instance_type == "PhysicalResource", record

	# Instanstyp Electronic, inget som antyder Online -> Instanstyp PhysicalResource, instanskategori ElectronicStorageMedium
	# FIXME Det verkar inte som att det finns några termer som implicerar 'https://id.kb.se/term/saobf/AbstractElectronic'
	#elif id == "https://libris-qa.kb.se/test/electronic":
	#	assert instance_type == "PhysicalResource", record
	#	assert instance_category == [{"@id":"https://id.kb.se/term/saobf/ElectronicStorageMedium"}], record

	# Instanstyp Print blir saobf Print
	elif id == "https://libris-qa.kb.se/test/print":
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/Print"}]

	# Instanstyp VideoRecording, inget som antyder Online -> Instanstyp PhysicalResource, instanskategori saobf/VideoStorageMedium, verkskategori ktg/MovingImage
	elif id == "https://libris-qa.kb.se/test/videorecording":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/MovingImage"}], record
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/VideoStorageMedium"}], record

	# Instanstyp SoundRecording, inget som antyder Online -> Instanstyp PhysicalResource, instanskategori saobf/SoundStorageMedium, verkskategori ktg/Audio
	elif id == "https://libris-qa.kb.se/test/soundrecording":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Audio"}], record
		assert instance_type == "PhysicalResource"
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/SoundStorageMedium"}], record

	# Instanstyp StillImageInstance -> instanstyp PhysicalResource, instanskategori rda/Sheet och verkskateogri rda/StillImage
	elif id == "https://libris-qa.kb.se/test/stillimageinstance":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/StillImage"}], record
		assert instance_type == "PhysicalResource", record
		assert instance_category == [{"@id":"https://id.kb.se/term/rda/Sheet"}], record

	# Instanstyp Tactile -> instanstyp PhysicalResource, saobf Braille
	elif id == "https://libris-qa.kb.se/test/tactile":
		assert instance_type == "PhysicalResource", record
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/Braille"}], record

	# Instanstyp Map -> instanstyp PhysicalResource, och verkskategori rda/CartographicImage
	elif id == "https://libris-qa.kb.se/test/map":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/CartographicImage"}], record
		assert instance_type == "PhysicalResource", record

	# Instanstyp Manuscript -> instanstyp PhysicalResource, instanskategori saobf/ManuscriptForm
	elif id == "https://libris-qa.kb.se/test/manuscript":
		assert instance_type == "PhysicalResource", record
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/HandmadeMaterial"}], record

	# Instanstyp Microform -> instanstyp PhysicalResource, instanskategori rda/Microform
	elif id == "https://libris-qa.kb.se/test/microform":
		assert instance_type == "PhysicalResource", record
		assert instance_category == [{"@id":"https://id.kb.se/term/rda/Microform"}], record

	# Instanstyp Globe -> instanstyp PhysicalResource, instanskategori rda/Object, verkskategori krda/CartographicThreeDimensionalForm och saogf/Kartglober
	elif id == "https://libris-qa.kb.se/test/globe":
		assert instance_type == "PhysicalResource", record
		assert instance_category == [{"@id":"https://id.kb.se/term/rda/Object"}], record
		assert work_category == [{"@id":"https://id.kb.se/term/rda/CartographicThreeDimensionalForm"}], record

	# Instanstyp KitInstance med verkstyp Text
	# TODO - what is expected here?
	elif id == "https://libris-qa.kb.se/test/kitinstance":
		assert instance_type == "PhysicalResource", record

	# Instanstyp TextInstance -> instanskategori rda/Volume och verkskategori rda/Text
	elif id == "https://libris-qa.kb.se/test/textinstance":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/Text"}], record
		assert instance_type == "PhysicalResource", record
		assert instance_category == [{"@id":"https://id.kb.se/term/rda/Volume"}], record

	# Instanstyp Archival med verkstyp Text
	# TODO - what is expected here?
	elif id == "https://libris-qa.kb.se/test/archival":
		assert instance_type == "PhysicalResource", record

	# Instanstyp Print med issuanceType ComponentPart blir instanskategory saobf ComponentPart och Print, verkstyp Monograph
	elif id == "https://libris-qa.kb.se/test/componentPart":
		assert work_type == "Monograph", record
		instance_categories = {c["@id"] for c in record["category"]}
		assert instance_categories == {"https://id.kb.se/term/saobf/ComponentPart", "https://id.kb.se/term/saobf/Print"}, record

	# Instanstyp Instance, issuanceType ComponentPart blir instanskategori saobf ComponentPart och Print PRINT, verkstyp Monograph
	elif id == "https://libris-qa.kb.se/test/componentPartInstance":
		assert work_type == "Monograph", record
		instance_categories = {c["@id"] for c in record["category"]}
		assert instance_categories == {"https://id.kb.se/term/saobf/ComponentPart"}, record

	# issuanceType serialComponentPart blir instanskategori saobf ComponentPart och Print, verkstyp Monograph
	elif id == "https://libris-qa.kb.se/test/serialComponentPart":
		assert work_type == "Monograph", record
		instance_categories = {c["@id"] for c in record["category"]}
		assert instance_categories == {"https://id.kb.se/term/saobf/ComponentPart", "https://id.kb.se/term/saobf/Print"}, record

	# Städa bort den tvetydiga MARC-typen Other från Kategori på instans
	elif id == "https://libris-qa.kb.se/test/marcother":
		assert instance_category == [{'@id': 'https://id.kb.se/term/saobf/Print'}], record

	# Flytta länkade SAOGF-termer i genreForm på instansen till kategori på verket efter att instansen normaliserats. Reducera och mappa verkskategorier som vanligt.
	elif id == "https://libris-qa.kb.se/test/moveInstanceGenreForm":
		work_categories = {c["@id"] for c in record["instanceOf"]["category"]}
		assert work_categories == {"https://id.kb.se/term/saogf/Romaner", "https://id.kb.se/term/rda/Text"}, record

		instance_category_nodes = [c for c in record["category"]]
		assert sorted(instance_category_nodes, key=str) == sorted(
			[{'@id': 'https://id.kb.se/term/saobf/Print'}, {"@id":"https://id.kb.se/marc/Thesis"}, {"@type": "GenreForm", "prefLabel":"Ancient scroll"}], key=str), record

	# På verk: gammal hasPart Text blir Work
	elif id == "https://libris-qa.kb.se/test/workHasPartText":
		assert record["instanceOf"]["hasPart"][0]["@type"] == "Work", record

	# På verk: gammal hasPart Print blir PhysicalResource
	elif id == "https://libris-qa.kb.se/test/workHasPartPrint":
		assert record["instanceOf"]["hasPart"][0]["@type"] == "PhysicalResource", record

	# På instans: Om gammal hasPart är Electronic och huvudpostens nya typ DigitalResource, blir hasPart DigitalResources
	elif id == "https://libris-qa.kb.se/test/instanceHasPartElectronic":
		assert instance_type == "DigitalResource", record
		assert record["hasPart"][0]["@type"] == "DigitalResource", record

	# På instans: gammal hasPart Print blir PhysicalResource; gammal hasPart Text blir Work
	elif id == "https://libris-qa.kb.se/test/instanceHasPartsTextPrint":
		part_types = {p["@type"] for p in record["hasPart"]}
		assert part_types == {"PhysicalResource", "Work"}, record

	# Specialfall Signe: verk som inte länkas från instanser blir också normaliserade
	elif id == "https://libris-qa.kb.se/test/workNotLinkedFromInstance":
		assert record["@type"] == "Work", record

	# Specialfall Signe: om det inte finns en issuanceType på instansen, hämta den från verket
	elif id == "https://libris-qa.kb.se/test/workWithIssuanceType":
		assert record["@type"] == "Serial", record

	# Ta bort bredare kategori 'Kataloger' som impliceras av smalare 'Bibliotekskataloger'
	elif id == "https://libris-qa.kb.se/test/removeImpliedBroader":
		work_categories = {c["@id"] for c in record["instanceOf"]["category"]}
		assert work_categories == {"https://id.kb.se/term/saogf/Bibliotekskataloger", "https://id.kb.se/term/rda/Text"}, record


	# Implicera aldrig ContentType - alla bevaras explicit
	elif id == "https://libris-qa.kb.se/test/neverImplyContentType":
		work_categories = {c["@id"] for c in record["instanceOf"]["category"]}
		assert work_categories == {"https://id.kb.se/term/rda/TactileImage", "https://id.kb.se/term/rda/StillImage"}, record

	# Om instanstyp är Map och verkstyp Cartography, behandla verket som CartographicImage
	elif id == "https://libris-qa.kb.se/test/complexInstanceTypeMappingMap":
		assert work_category == [{"@id": "https://id.kb.se/term/rda/CartographicImage" }], record

	# Omappad instanstyp MovingImageInstance -> fallback PhysicalResource
	elif id == "https://libris-qa.kb.se/test/unmappedInstanceTypeMovingImageInstance":
		assert instance_type == "PhysicalResource", record

	# När instanstyp är Electronic och minst en carrierTypes innehåller Online - sätt instanstyp DigitalResource
	elif id == "https://libris-qa.kb.se/test/digitalResourceFromElectronic":
		assert instance_type == "DigitalResource", record

	# När instanstyp är Instance och carrierTypes innehåller termer som innehåller/är mappade till 'Online' och 'Electronic' - sätt instanstyp DigitalResource
	# FIXME Varför får denna inte "isElectronic=true" på rad 382-285? Det verkar inte som att det finns några termer som implicerar 'https://id.kb.se/term/saobf/AbstractElectronic'
	elif id == "https://libris-qa.kb.se/test/digitalResourceFromInstance":
		assert instance_type == "DigitalResource", record
	
	# När instanstyp är Electronic och carrierTypes innehåller Online, ta bort länkade carrierTypes med Online i URIn, utom rda/OnlineResource
	# FIXME Det verkar inte som att det finns några termer som implicerar 'https://id.kb.se/term/saobf/AbstractElectronic'
	elif id == "https://libris-qa.kb.se/test/multipleCarrierTypesOneOnline":
		instance_categories = {c["@id"] for c in record["category"]}
		assert instance_categories == {"https://id.kb.se/term/rda/OnlineResource", "https://id.kb.se/term/rda/Volume", "https://id.kb.se/marc/RegularPrint", "https://id.kb.se/term/saogf/Webbplatser"}, record

	# Sätt instanskategori rda/volume om instanstyp är instans och extent innehåller uppgift om sidor
	elif id == "https://libris-qa.kb.se/test/computeVolumeFromExtent":
		assert instance_type == "PhysicalResource", record
		assert instance_category == [{"@id": "https://id.kb.se/term/rda/Volume"}], record

	# Byr ut lokal entitet 'E-böcker' mot saobf/E-books.
	# FIXME - Det verkar inte som att det finns några termer som implicerar 'https://id.kb.se/term/saobf/AbstractElectronic'
	elif id == "https://libris-qa.kb.se/test/replaceLocalEböckerWithEbooks":
		instance_categories = {c["@id"] for c in record["category"]}
		assert instance_categories == {"https://id.kb.se/term/saobf/ElectronicStorageMedium", "https://id.kb.se/term/saobf/EBook"}, record

	# Bevara nya typer Monograph och DigitalResource på redan normaliserade poster
	elif id == "https://libris-qa.kb.se/test/alreadyNormalized":
		assert work_type == "Monograph", record
		assert instance_type == "DigitalResource", record

	# Videoinspelningar ska inte saobf/Print, även om de har egenskapen publication
	elif id == "https://libris-qa.kb.se/test/doNotAddPrintOnVideoWhenPublication":
		instance_categories = {c["@id"] for c in record["category"]}
		assert "https://id.kb.se/term/saobf/Print" not in instance_categories, record

	# Ljudinspelningar ska inte saobf/Print, även om de har egenskapen publication
	elif id == "https://libris-qa.kb.se/test/doNotAddPrintOnSoundWhenPublication":
		instance_categories = {c["@id"] for c in record["category"]}
		assert "https://id.kb.se/term/saobf/Print" not in instance_categories, record
		
	# TactileText ska inte få verkskategori rda/Text
	elif id == "https://libris-qa.kb.se/test/tactileText":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/TactileText"}], record

	# TactileText och StillImage ska inte få verkskategori rda/Text
	elif id == "https://libris-qa.kb.se/test/tactileTextAndStillImage":
		work_categories = {c["@id"] for c in record["instanceOf"]["category"]}
		assert work_categories == {"https://id.kb.se/term/rda/TactileText", "https://id.kb.se/term/rda/StillImage"}, record

		
print("\nAll tests passed!")
