
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
	entity = r[1]

	try:
		title = entity["instanceOf"]["hasTitle"][0]["mainTitle"]
	except:
		# Special case
		title = {entity["hasTitle"][0]["mainTitle"]}

	print(f"\n{id}\t{title}")


	work_type = entity.get("instanceOf", {}).get("@type", "")
	work_category = entity.get("instanceOf", {}).get("category", [])

	instance_type = entity.get("@type", "")
	instance_category = entity.get("category", [])

	"""Genreic test for leftover obsolete properties"""

	new_work_types = ["Monograph","Serial","Collection","Integrating","Work"]
	new_instance_types = ["PhysicalResource","DigitalResource"]

	
	if id in special_cases:
		print("Special Signe entity! Skipping generic tests.")
	else:
		assert work_type in new_work_types, f"Failed! Unexpected work type: {work_type} in work {id}"
		assert instance_type in new_instance_types, f"Failed! Unexpected instance type: {instance_type} in instance {id}"

		old_properties = ["genreForm","contentType","carrierType","mediaType","issuanceType"]
		
		for p in old_properties:
			assert p not in entity.get("instanceOf", {}), "Failed! Obsolete property still present in work: {p}"
			assert p not in entity, "Failed! Obsolete property still present in instance: {p}"
		

	"""Specific tests for each tailored example"""

	# Verkstyp Text blir RDA Text
	if id == "https://libris-qa.kb.se/test/text":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/Text"}], entity

	# Verkstyp Work sparas inte som verkskategori
	elif id == "https://libris-qa.kb.se/test/work":
		assert not work_category

	# Verkstyp MovingImage blir ktg MovingImage
	elif id == "https://libris-qa.kb.se/test/movingimage":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/MovingImage"}], entity

	# Verkstyp Audio blir ktg Audio
	elif id == "https://libris-qa.kb.se/test/audio":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Audio"}], entity

	# Verkstyp NotatedMusic blir RDA NotatedMusic
	elif id == "https://libris-qa.kb.se/test/notatedmusic":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/NotatedMusic"}], entity

	# Verkstyp StillImage blir RDA StillImage
	elif id == "https://libris-qa.kb.se/test/stillimage":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/StillImage"}], entity

	# Verkstyp Music blir SAOGF Music
	elif id == "https://libris-qa.kb.se/test/music":
		assert work_category == [{"@id":"https://id.kb.se/term/saogf/Musik"}], entity

	# Verkstyp Multimedia blir ktg/Software ?
	# FIXME Lista ut varför mappning säger att denna ska bli ktg/Multimedia när intenrsectionpatterns säger ktg/Software
	elif id == "https://libris-qa.kb.se/test/multimedia":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Software"}], entity

	# Verkstyp Cartography blir RDA CartographicImage om ej annan RDA Cartorgaphy-term redan finns
	elif id == "https://libris-qa.kb.se/test/cartography":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/CartographicImage"}], entity

	# Verkstyp Kit blir ktg Kit
	elif id == "https://libris-qa.kb.se/test/kit":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Kit"}], entity

	# Verkstyp Object blir RDA ThreeDimensionalForm
	elif id == "https://libris-qa.kb.se/test/object":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/ThreeDimensionalForm"}], entity

	# Verkstyp ManuscriptText blir SAOGF Handsrkrifter
	elif id == "https://libris-qa.kb.se/test/manuscripttext":
		assert work_category == [{"@id":"https://id.kb.se/term/saogf/Handskrifter"}], entity

	# Verkstyp ManuscriptNotatedMusic blir RDA NotatedMusic och SAOGF Handskrifter
	elif id == "https://libris-qa.kb.se/test/manuscriptnotatedmusic":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/NotatedMusic"},{"@id":"https://id.kb.se/term/saogf/Handskrifter",
		}], entity

	# Verkstyp MixedMaterial blir ktg MixedMaterial
	elif id == "https://libris-qa.kb.se/test/mixedmaterial":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/MixedMaterial"}], entity

	# Verkstyp Dataset blir ktg Dataset
	elif id == "https://libris-qa.kb.se/test/dataset":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Dataset"}], entity

	# Verkstyp NonMusicAudio blir RDA Sounds
	# FIXME - Ska det bli Audtio (ktg/Audio) istället för Sounds (rda/Sounds)?
	#elif id == "https://libris-qa.kb.se/test/nonmusicaudio":
	#	assert work_category == [{"@id":"https://id.kb.se/term/rda/Sounds"}], entity

	# Verkstyp saknas i mappning - skriv ut i logg!
	elif id == "https://libris-qa.kb.se/test/unhandledworktype":
		pass

	# Instanstyp Instance och verkstyp Text -> instanstyp PhysicalResource och verkskategori Text
	elif id == "https://libris-qa.kb.se/test/instance":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/Text"}], entity
		assert instance_type == "PhysicalResource", entity

	# Instanstyp Electronic, inget som antyder Online -> Instanstyp PhysicalResource, instanskategori ElectronicStorageMedium
	# FIXME Det verkar inte som att det finns några termer som implicerar 'https://id.kb.se/term/saobf/AbstractElectronic'
	#elif id == "https://libris-qa.kb.se/test/electronic":
	#	assert instance_type == "PhysicalResource", entity
	#	assert instance_category == [{"@id":"https://id.kb.se/term/saobf/ElectronicStorageMedium"}], entity

	# Instanstyp Print blir saobf Print
	elif id == "https://libris-qa.kb.se/test/print":
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/Print"}]

	# Instanstyp VideoRecording, inget som antyder Online -> Instanstyp PhysicalResource, instanskategori saobf/VideoStorageMedium, verkskategori ktg/MovingImage
	elif id == "https://libris-qa.kb.se/test/videorecording":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/MovingImage"}], entity
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/VideoStorageMedium"}], entity

	# Instanstyp SoundRecording, inget som antyder Online -> Instanstyp PhysicalResource, instanskategori saobf/SoundStorageMedium, verkskategori ktg/Audio
	elif id == "https://libris-qa.kb.se/test/soundrecording":
		assert work_category == [{"@id":"https://id.kb.se/term/ktg/Audio"}], entity
		assert instance_type == "PhysicalResource"
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/SoundStorageMedium"}], entity

	# Instanstyp StillImageInstance -> instanstyp PhysicalResource, instanskategori rda/Sheet och verkskateogri rda/StillImage
	elif id == "https://libris-qa.kb.se/test/stillimageinstance":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/StillImage"}], entity
		assert instance_type == "PhysicalResource", entity
		assert instance_category == [{"@id":"https://id.kb.se/term/rda/Sheet"}], entity

	# Instanstyp Tactile -> instanstyp PhysicalResource, saobf Braille
	elif id == "https://libris-qa.kb.se/test/tactile":
		assert instance_type == "PhysicalResource", entity
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/Braille"}], entity

	# Instanstyp Map -> instanstyp PhysicalResource, och verkskategori rda/CartographicImage
	elif id == "https://libris-qa.kb.se/test/map":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/CartographicImage"}], entity
		assert instance_type == "PhysicalResource", entity

	# Instanstyp Manuscript -> instanstyp PhysicalResource, instanskategori saobf/ManuscriptForm
	elif id == "https://libris-qa.kb.se/test/manuscript":
		assert instance_type == "PhysicalResource", entity
		assert instance_category == [{"@id":"https://id.kb.se/term/saobf/HandmadeMaterial"}], entity

	# Instanstyp Microform -> instanstyp PhysicalResource, instanskategori rda/Microform
	elif id == "https://libris-qa.kb.se/test/microform":
		assert instance_type == "PhysicalResource", entity
		assert instance_category == [{"@id":"https://id.kb.se/term/rda/Microform"}], entity

	# Instanstyp Globe -> instanstyp PhysicalResource, instanskategori rda/Object, verkskategori krda/CartographicThreeDimensionalForm och saogf/Kartglober
	elif id == "https://libris-qa.kb.se/test/globe":
		assert instance_type == "PhysicalResource", entity
		assert instance_category == [{"@id":"https://id.kb.se/term/rda/Object"}], entity
		assert work_category == [{"@id":"https://id.kb.se/term/rda/CartographicThreeDimensionalForm"}], entity

	# Instanstyp KitInstance med verkstyp Text
	# TODO - what is expected here?
	elif id == "https://libris-qa.kb.se/test/kitinstance":
		assert instance_type == "PhysicalResource", entity

	# Instanstyp TextInstance -> instanskategori rda/Volume och verkskategori rda/Text
	elif id == "https://libris-qa.kb.se/test/textinstance":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/Text"}], entity
		assert instance_type == "PhysicalResource", entity
		assert instance_category == [{"@id":"https://id.kb.se/term/rda/Volume"}], entity

	# Instanstyp Archival med verkstyp Text
	# TODO - what is expected here?
	elif id == "https://libris-qa.kb.se/test/archival":
		assert instance_type == "PhysicalResource", entity

	# Instanstyp Print med issuanceType ComponentPart blir instanskategory saobf ComponentPart och Print, verkstyp Monograph
	elif id == "https://libris-qa.kb.se/test/componentPart":
		assert work_type == "Monograph", entity
		instance_categories = {c["@id"] for c in entity["category"]}
		assert instance_categories == {"https://id.kb.se/term/saobf/ComponentPart", "https://id.kb.se/term/saobf/Print"}, entity

	# Instanstyp Instance, issuanceType ComponentPart blir instanskategori saobf ComponentPart och Print PRINT, verkstyp Monograph
	elif id == "https://libris-qa.kb.se/test/componentPartInstance":
		assert work_type == "Monograph", entity
		instance_categories = {c["@id"] for c in entity["category"]}
		assert instance_categories == {"https://id.kb.se/term/saobf/ComponentPart"}, entity

	# issuanceType serialComponentPart blir instanskategori saobf ComponentPart och Print, verkstyp Monograph
	elif id == "https://libris-qa.kb.se/test/serialComponentPart":
		assert work_type == "Monograph", entity
		instance_categories = {c["@id"] for c in entity["category"]}
		assert instance_categories == {"https://id.kb.se/term/saobf/ComponentPart", "https://id.kb.se/term/saobf/Print"}, entity

	# Städa bort den tvetydiga MARC-typen Other från Kategori på instans
	elif id == "https://libris-qa.kb.se/test/marcother":
		assert instance_category == [{'@id': 'https://id.kb.se/term/saobf/Print'}], entity

	# Flytta länkade SAOGF-termer i genreForm på instansen till kategori på verket efter att instansen normaliserats. Reducera och mappa verkskategorier som vanligt.
	elif id == "https://libris-qa.kb.se/test/moveInstanceGenreForm":
		work_categories = {c["@id"] for c in entity["instanceOf"]["category"]}
		assert work_categories == {"https://id.kb.se/term/saogf/Romaner", "https://id.kb.se/term/rda/Text"}, entity

		instance_category_nodes = [c for c in entity["category"]]
		assert sorted(instance_category_nodes, key=str) == sorted(
			[{'@id': 'https://id.kb.se/term/saobf/Print'}, {"@id":"https://id.kb.se/marc/Thesis"}, {"@type": "GenreForm", "prefLabel":"Ancient scroll"}], key=str), entity

	# På verk: gammal hasPart Text blir Work
	elif id == "https://libris-qa.kb.se/test/workHasPartText":
		assert entity["instanceOf"]["hasPart"][0]["@type"] == "Work", entity

	# På verk: gammal hasPart Print blir Instance med saobf/Print
	elif id == "https://libris-qa.kb.se/test/workHasPartPrint":
		part = entity["instanceOf"]["hasPart"][0]
		assert part["@type"] == "Instance", entity
		assert part["category"] == [{"@id":"https://id.kb.se/term/saobf/Print"}], entity

	# På instans: Om gammal hasPart är Electronic -> ny typ Instance, saobf/AbstractElectronic
	elif id == "https://libris-qa.kb.se/test/instanceHasPartElectronic":
		part = entity["hasPart"][0]
		assert part["@type"] == "Instance", entity
		assert part["category"] == [{"@id":"https://id.kb.se/term/saobf/AbstractElectronic"}], entity

	# På instans: gammal hasPart Print blir Instance med saobf/Print; gammal hasPart Text blir Work med rda/Text
	elif id == "https://libris-qa.kb.se/test/instanceHasPartsTextPrint":
		part_types = {p["@type"] for p in entity["hasPart"]}
		assert part_types == {"Instance", "Work"}, entity
		for part in entity["hasPart"]:
			if part["@type"] == "Instance":
				assert part["category"] == [{"@id":"https://id.kb.se/term/saobf/Print"}], entity
			if part["@type"] == "Work":
				assert part["category"] == [{"@id":"https://id.kb.se/term/rda/Text"}], entity

	# Specialfall Signe: verk som inte länkas från instanser blir också normaliserade
	elif id == "https://libris-qa.kb.se/test/workNotLinkedFromInstance":
		assert entity["@type"] == "Work", entity

	# Specialfall Signe: om det inte finns en issuanceType på instansen, hämta den från verket
	elif id == "https://libris-qa.kb.se/test/workWithIssuanceType":
		assert entity["@type"] == "Serial", entity

	# Ta bort bredare kategori 'Kataloger' som impliceras av smalare 'Bibliotekskataloger'
	elif id == "https://libris-qa.kb.se/test/removeImpliedBroader":
		work_categories = {c["@id"] for c in entity["instanceOf"]["category"]}
		assert work_categories == {"https://id.kb.se/term/saogf/Bibliotekskataloger", "https://id.kb.se/term/rda/Text"}, entity


	# Implicera aldrig ContentType - alla bevaras explicit
	elif id == "https://libris-qa.kb.se/test/neverImplyContentType":
		work_categories = {c["@id"] for c in entity["instanceOf"]["category"]}
		assert work_categories == {"https://id.kb.se/term/rda/TactileImage", "https://id.kb.se/term/rda/StillImage"}, entity

	# Om instanstyp är Map och verkstyp Cartography, behandla verket som CartographicImage
	elif id == "https://libris-qa.kb.se/test/complexInstanceTypeMappingMap":
		assert work_category == [{"@id": "https://id.kb.se/term/rda/CartographicImage" }], entity

	# Omappad instanstyp MovingImageInstance -> fallback PhysicalResource
	elif id == "https://libris-qa.kb.se/test/unmappedInstanceTypeMovingImageInstance":
		assert instance_type == "PhysicalResource", entity

	# När instanstyp är Electronic och minst en carrierTypes innehåller Online - sätt instanstyp DigitalResource
	elif id == "https://libris-qa.kb.se/test/digitalResourceFromElectronic":
		assert instance_type == "DigitalResource", entity

	# När instanstyp är Instance och carrierTypes innehåller termer som innehåller/är mappade till 'Online' och 'Electronic' - sätt instanstyp DigitalResource
	# FIXME Varför får denna inte "isElectronic=true" på rad 382-285? Det verkar inte som att det finns några termer som implicerar 'https://id.kb.se/term/saobf/AbstractElectronic'
	elif id == "https://libris-qa.kb.se/test/digitalResourceFromInstance":
		assert instance_type == "DigitalResource", entity
	
	# När instanstyp är Electronic och carrierTypes innehåller Online, ta bort länkade carrierTypes med Online i URIn, utom rda/OnlineResource
	# FIXME Det verkar inte som att det finns några termer som implicerar 'https://id.kb.se/term/saobf/AbstractElectronic'
	elif id == "https://libris-qa.kb.se/test/multipleCarrierTypesOneOnline":
		instance_categories = {c["@id"] for c in entity["category"]}
		assert instance_categories == {"https://id.kb.se/term/rda/OnlineResource", "https://id.kb.se/term/rda/Volume", "https://id.kb.se/marc/RegularPrint"}, entity

	# Sätt instanskategori rda/volume om instanstyp är instans och extent innehåller uppgift om sidor
	elif id == "https://libris-qa.kb.se/test/computeVolumeFromExtent":
		assert instance_type == "PhysicalResource", entity
		assert instance_category == [{"@id": "https://id.kb.se/term/rda/Volume"}], entity

	# Byr ut lokal entitet 'E-böcker' mot saobf/E-books.
	# FIXME - Det verkar inte som att det finns några termer som implicerar 'https://id.kb.se/term/saobf/AbstractElectronic'
	elif id == "https://libris-qa.kb.se/test/replaceLocalEböckerWithEbooks":
		instance_categories = {c["@id"] for c in entity["category"]}
		assert instance_categories == {"https://id.kb.se/term/saobf/ElectronicStorageMedium", "https://id.kb.se/term/saobf/EBook"}, entity

	# Bevara nya typer Monograph och DigitalResource på redan normaliserade poster
	elif id == "https://libris-qa.kb.se/test/alreadyNormalized":
		assert work_type == "Monograph", entity
		assert instance_type == "DigitalResource", entity

	# Videoinspelningar ska inte saobf/Print, även om de har egenskapen publication
	elif id == "https://libris-qa.kb.se/test/doNotAddPrintOnVideoWhenPublication":
		instance_categories = {c["@id"] for c in entity["category"]}
		assert "https://id.kb.se/term/saobf/Print" not in instance_categories, entity

	# Ljudinspelningar ska inte saobf/Print, även om de har egenskapen publication
	elif id == "https://libris-qa.kb.se/test/doNotAddPrintOnSoundWhenPublication":
		instance_categories = {c["@id"] for c in entity["category"]}
		assert "https://id.kb.se/term/saobf/Print" not in instance_categories, entity
		
	# TactileText ska inte få verkskategori rda/Text
	elif id == "https://libris-qa.kb.se/test/tactileText":
		assert work_category == [{"@id":"https://id.kb.se/term/rda/TactileText"}], entity

	# TactileText och StillImage ska inte få verkskategori rda/Text
	elif id == "https://libris-qa.kb.se/test/tactileTextAndStillImage":
		work_categories = {c["@id"] for c in entity["instanceOf"]["category"]}
		assert work_categories == {"https://id.kb.se/term/rda/TactileText", "https://id.kb.se/term/rda/StillImage"}, entity

	# Ta bort bredare ny SAOGF-term 'Facklitteratur' som impliceras av smalare 'Personlig utveckling'
	elif id == "https://libris-qa.kb.se/test/removeImpliedBroaderNewSAOGF":
		work_categories = {c["@id"] for c in entity["instanceOf"]["category"]}
		assert work_categories == {"https://id.kb.se/term/rda/Text", "https://id.kb.se/term/saogf/Personlig%20utveckling"}, entity

print("\nAll tests passed!")
