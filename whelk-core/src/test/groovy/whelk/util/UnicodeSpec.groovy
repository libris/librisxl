package whelk.util

import spock.lang.Specification

class UnicodeSpec extends Specification {

    def "normalize to NFC"() {
        given:
        String s = "Adam f*å*r en melding fra det gamle friidrettslaget, som ønsker at"
        String nfc = "Adam f*å*r en melding fra det gamle friidrettslaget, som ønsker at"
        expect:
        Unicode.isNormalized(s) == false
        Unicode.normalize(s) == nfc
    }

    def "normalize typographic ligatures"() {
        given:
        String s = "societal bene*ﬁ*t is maximized. This means that the tracks should be used by as much tra*ﬃ*c"
        String norm = "societal bene*fi*t is maximized. This means that the tracks should be used by as much tra*ffi*c"
        expect:
        Unicode.isNormalized(s) == false
        Unicode.normalize(s) == norm
    }
    
    def "strip BOM"() {
        given:
        String s = "9th Koli Calling International Conference on Computing Education Research\ufeff, October 29–November 1, 2009"
        String norm = "9th Koli Calling International Conference on Computing Education Research, October 29–November 1, 2009"
        expect:
        Unicode.isNormalized(s) == false
        Unicode.normalize(s) == norm
    }

    def "trim noise"() {
        expect:
        Unicode.trimNoise(dirty) == clean
        where:
        dirty                                    | clean
        ' _.:;|{[Überzetsung]}|;:. '             | 'Überzetsung'
        ' _.:;|(Überzetsung)|;:. '               | '(Überzetsung)'
        ' _.:;| Ü b e r - z e t - s u n g |;:. ' | 'Ü b e r - z e t - s u n g'
    }

    def "trim"() {
        expect:
        Unicode.trim(dirty) == clean
        where:
        dirty                                                       | clean
        ' SPACE '                                                   | 'SPACE'
        '\u00A0\u00A0\u00A0NO-BREAK SPACE\u00A0\u00A0\u00A0'        | 'NO-BREAK SPACE'
        '\u202F\u202F\u202FNARROW NO-BREAK SPACE\u202F\u202F\u202F' | 'NARROW NO-BREAK SPACE'
        '\u2007\u2007\u2007FIGURE SPACE\u2007\u2007\u2007'          | 'FIGURE SPACE'
        '\u2060\u2060\u2060WORD JOINER\u2060\u2060\u2060'           | 'WORD JOINER'
        'keep\u00A0\u202F\u2007\u2060us'                            | 'keep\u00A0\u202F\u2007\u2060us'
    }
    
    def "double quotation marks"() {
        expect:
        Unicode.isNormalizedDoubleQuotes(dirty) == (dirty == clean)
        Unicode.normalizeDoubleQuotes(dirty) == clean
        where:
        dirty                                                       | clean
        '"my query"'                                                | '"my query"'
        '”my query”'                                                | '"my query"'
        '“my query”'                                                | '"my query"'
        'this is ”my query” string'                                 | 'this is "my query" string'
        'this is “my query” string'                                 | 'this is "my query" string'
    }

    def "Romanize mongolian languages"() {
        expect:
        Unicode.romanize(source, 'mn') == target
        where:
        source                                                      | target
        // s3bjxplkqcmb6k8w
        'Арын ордны нууц. Татвар эмийн хүүрнэл'                     | 'Arîn ordnî nuuts. Tatvar emiin khüürnel'
    }

    def "Romanize Kazakh with cyrillic script"() {
        expect:
        Unicode.romanize(source, 'kk') == target
        where:
        // All examples in the Libris catalogue seem to be wrong...
        source                                                      | target
        // 4k2jkgmm21h1tst0
        // 'Ұзыншұлық Пиппи достарымен бірге'                          | 'Uzynšulyq Pippi dostarymen bіrge'
        // v9s85jwgs41clsb6
        //'Ұзыншұлық Пиппи Коратуттутт аралында'                       | 'Uzynšulyq Pippi Koratuttutt aralynda'
        // dsrl3v9xb3md7pmg
        //'Шығармалары'                                               | 'Shygharmalary'
        //'Әндері, өлең-жырлары. Естеліктер, мақалалар, аңыз'         | 'Ȯnderi, ȯleng-zhyrlary. Estelikster, maqalalar, angyz'
        // 6kfdzm0m4p12r1d9
        //'Науан Хазірет'                                             | 'Nuan Chaziret'
        // x8p5z8r6vd6qf5m0
        'Тiлдескiш, Разговорник'                                    | 'Tildeskiš, Razgovornik'
        // 5h34bgnl3fzmrrs3
        // 'Толковый словарь казахского языка: A-K'                    | 'Tolkovyj slovar kazachskogo jazyka: A-K'
    }

    def "Romanize modern greek"() {
        expect:
        Unicode.romanize(source, 'el') == target
        where:
        source                                                     | target
        // zd982r5xwmcb027n
        'Στις φλόγες'                                              | 'Stis flojes'
        'το καυτό θέμα της κλιματικής αλλαγής'                     | 'to kafto thema tis klimatikis allajis'
        // dtg9sg62b8vs2m93
        'Να εισαι ο εαυτος σου!'                                   | 'Na ise o eaftos sou!'
        // p1mpzmnvmfkx6spp
        'Τι ρόλο παίζει αυτή η αγελάδα;'                           | 'Ti rolo pezi afti i ajeladha?'
        // kwzsh0v4hbcrffll
        'Το θαυμαστό ταξίδι του Νιλς Χόλγκερσον'                   | 'To thavmasto taxidhi tou Nils Cholngerson'
        // v9v5kfgks5kgb8jz
        'Το νησι των θησαυρων'                                     | 'To nisi ton thisavron'
        'η απο τισ πειρατικεσ ιστοριεσ, η πιο πειρατικη'           | 'i apo tis piratikes istories, i pio piratiki'
        // 9q9s44v77vswpddj
        // example of where the manual transcription was wrong "maghissa" <> "majissa"  
        'Η Ανια και η μαγισσα του χιονιου'                         | 'I Ania ke i majissa tou chioniou'
        // https://libris.kb.se/dq6jzpv7b1ccb6m8#it
        //'Σ.Ο.Σ. Άμεσος κίνδυνος'                                   | 'S.O.S. amesos kindhinos'
        'Σ.Ο.Σ. Άμεσος κίνδυνος'                                   | 'S.O.S. Amesos kindhinos'
        // https://libris.kb.se/q1gw6nq9nrjrfz5r#it
        'Δυτικά της ελευθερίας'                                    | 'Dhitika tis eleftherias'
        // https://libris.kb.se/bq7b06548vsgv13d#it
        //'Πώς Οι Δανοί Εκπαιδεύουν Τα Πιο Ευτυχισμένα Παιδιά Στο Σχολείο Και Στην Οικογένεια' | 'Pos i dhani ekpedhevoun ta pio eftichismena pedhia sto scholio ke stin ikojenia'
        'Πώς Οι Δανοί Εκπαιδεύουν Τα Πιο Ευτυχισμένα Παιδιά Στο Σχολείο Και Στην Οικογένεια' | 'Pos I Dhani Ekpedhevoun Ta Pio Eftichismena Pedhia Sto Scholio Ke Stin Ikojenia'
        // https://libris.kb.se/7nnt5t5z5xtlzr37#it
        //'Ο Αϊ-Βασιλης στη Φυλακη με τους 83 αρουραίους'            | 'O Ai-Vasilis sti filaki me tous 83 aroureous'
        'Ο Αϊ-Βασιλης στη Φυλακη με τους 83 αρουραίους'            | 'O Ai-Vasilis sti Filaki me tous 83 aroureous'
    }
    
    def "Romanize ancient greek"() {
        expect:
        Unicode.romanize(source, 'grc') == target
        where:
        source | target
        // https://libris.kb.se/jx2hz4nbg495dgsm#it 	
        "Oδύσσεια" | "Odysseia"
        // https://libris.kb.se/s7h2fcnjqzrhngn7#it 	
        "Ορέστης" | "Orestēs"
        // https://libris.kb.se/gw5q6053dlvgwh1q#it 	
        "Ιστορίαι" | "Istoriai"
        // https://libris.kb.se/wbqtq3x4tbtk0151#it 	
        "Νεφέλαι ; Λυσιστράτη" | "Nephelai ; Lysistratē"
    }

    def "Romanize russian with ISO"() {
        expect:
        Unicode.romanize(source, 'ru') == target
        where:
        source || target
        // https://libris.kb.se/dtmgzm6wb09mh1vl#it
        'С рубежей фронтовых на рубежи дипломатические' || 'S rubežej frontovych na rubeži diplomatičeskie'
        // https://libris.kb.se/8phbwttp6xss25cv#it
        'Россия и Африка' || 'Rossija i Afrika'
        // https://libris.kb.se/3gc420831dfw0h04#it
        'Великая эпидемия' || 'Velikaja ėpidemija'
        // https://libris.kb.se/4ngwxdng0r030jm#it 
        'Сила подсознания, или Как изменить жизнь за 4 недели' || 'Sila podsoznanija, ili Kak izmenitʹ žiznʹ za 4 nedeli'
        // https://libris.kb.se/3mfrcqkf2c4xxq9#it
        // 'Тень горы' || 'Ten\' gory'
        'Тень горы' || 'Tenʹ gory'
        // https://libris.kb.se/9q8j07bp75r3cz75#it
        'Н.В. Гоголь и цензура' || 'N.V. Gogolʹ i cenzura'
        // https://libris.kb.se/fpp3cp1vcp8hp51h#it
        'Маленький ослик Марии' || 'Malenʹkij oslik Marii' 
        // https://libris.kb.se/cqm93c7v97jmrrgn#it
        // Test record seems to be made with a transliterator that doesn't handle case correctly
        //'ВХУТЕМАС - ВХУТЕИН. Полиграфический факультет. 1920-1930' | 'VChUTEMAS - VChUTEIN. Poligrafičeskij fakulʹtet. 1920-1930'
        'ВХУТЕМАС - ВХУТЕИН. Полиграфический факультет. 1920-1930' | 'VCHUTEMAS - VCHUTEIN. Poligrafičeskij fakulʹtet. 1920-1930'
    }

    def "Romanize belarusian with ISO"() {
        expect:
        Unicode.romanize(source, 'be') == target
        where:
        source || target
        // https://libris.kb.se/p408wtcjm06kz192#it
        // ŭ vs ǔ
        // 'Пiпi Доўгая Панчоха' || 'Pipi Doǔhaja Pančocha'
        'Пiпi Доўгая Панчоха' || 'Pipi Doŭhaja Pančocha'
        // https://libris.kb.se/wb7x42x1t8wj48jg#it
        'Срэбная дарога' | 'Srėbnaja daroha'
    }

    def "Romanize bulgarian with ISO"() {
        expect:
        Unicode.romanize(source, 'bg') == target
        where:
        source || target
        // https://libris.kb.se/fzr6pkkr2vnc152#it
        'Баба праща поздрави и се извинява' || 'Baba prašta pozdravi i se izvinjava'
        // https://libris.kb.se/jxqb93w0gxhtbf7d#it
        "Белия зъб" || "Belija zăb"
    }

    def "Romanize macedonian with ISO"() {
        expect:
        Unicode.romanize(source, 'mk') == target
        where:
        source || target
        // https://libris.kb.se/wbxc6926tctq1kz6#it
        "Бележникот на Грег Хефли" || "Beležnikot na Greg Hefli"
        // https://libris.kb.se/zcxzphvrwtgv8cdc#it 	
        //"Биби, Боби и чудовиштето од Охридското езеро" || "Bibi, Bobi i čudovišteto od Ohridskogoto ezero" 
        "Биби, Боби и чудовиштето од Охридското езеро" || "Bibi, Bobi i čudovišteto od Ohridskoto ezero" 
    }

    def "Romanize serbian with ISO"() {
        expect:
        Unicode.romanize(source, 'sr') == target
        where:
        source || target
        // https://libris.kb.se/2dbbcc810dxjnk9l#it
        "Узбуна у кући белих медведа" || "Uzbuna u kući belih medveda"
        // https://libris.kb.se/dqnnpp1sbxm436wp#it
        'Тиги, хаjдемо у шетњу' || 'Tigi, hajdemo u šetnju'
    }

    def "Romanize ukrainian with ISO"() {
        expect:
        Unicode.romanize(source, 'uk') == target
        where:
        source || target
        // https://libris.kb.se/n2wmnf2hll6cn6vg#it 	
        "Дивовижні пригоди в лісовій школі" || "Dyvovyžni pryhody v lisovij školi"
        // https://libris.kb.se/dtwdcxsjbpdf24bj#it
        "Сміттєва революція" || "Smittjeva revoljucija"
        // https://libris.kb.se/gws6kjbndwk4ct10#it 	
        "Знаменитий детектив Блюмквіст" || "Znamenytyj detektyv Bljumkvist"
        // https://libris.kb.se/csld89sd9f4gm75n#it
        "Я люблю ходити в дитячий садок" || "Ja ljublju chodyty v dytjačyj sadok"
    }

    /*
    PREFIX : <https://id.kb.se/vocab/>

    SELECT ?i ?t ?vt {
        ?i :instanceOf [ a :Text;
                :language <https://id.kb.se/language/ukr> ];
                ^:mainEntity [ a :Record ;
                :technicalNote [ a :TechnicalNote ;
                :label "Translitterering enligt ISO 9:1995" ]
        ]
                OPTIONAL {
                    ?i :hasTitle [ a :Title ; :mainTitle ?t ] .
                }
                OPTIONAL {
                    ?i :hasTitle [ a :VariantTitle ; :mainTitle ?vt ] .
                }
    }
     */

}