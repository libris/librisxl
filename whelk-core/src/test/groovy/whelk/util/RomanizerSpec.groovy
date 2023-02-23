package whelk.util

import spock.lang.Specification

class RomanizerSpec extends Specification {

    def "Mongolian languages with cyrillic script"() {
        expect:
        Romanizer.romanize(source, 'mn-Cyrl')['mn-Latn-t-mn-Cyrl-x0-lessing'] == target
        where:
        source                                                      | target
        // s3bjxplkqcmb6k8w
        'Арын ордны нууц. Татвар эмийн хүүрнэл'                     | 'Arîn ordnî nuuts. Tatvar emiin khüürnel'
    }

    def "Kazakh with cyrillic script"() {
        expect:
        Romanizer.romanize(source, 'kk')['kk-Latn-t-kk-Cyrl-m0-iso-1995'] == target
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

    def "Modern greek"() {
        expect:
        Romanizer.romanize(source, 'el')['el-Latn-t-el-Grek-x0-btj'] == target
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

    def "Ancient greek"() {
        expect:
        Romanizer.romanize(source, 'grc')['grc-Latn-t-grc-Grek-x0-skr-1980'] == target
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

    def "Russian with ISO"() {
        expect:
        Romanizer.romanize(source, 'ru')['ru-Latn-t-ru-Cyrl-m0-iso-1995'] == target
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

    def "Belarusian with ISO"() {
        expect:
        Romanizer.romanize(source, 'be')['be-Latn-t-be-Cyrl-m0-iso-1995'] == target
        where:
        source || target
        // https://libris.kb.se/p408wtcjm06kz192#it
        // ŭ vs ǔ
        // 'Пiпi Доўгая Панчоха' || 'Pipi Doǔhaja Pančocha'
        'Пiпi Доўгая Панчоха' || 'Pipi Doŭhaja Pančocha'
        // https://libris.kb.se/wb7x42x1t8wj48jg#it
        'Срэбная дарога' | 'Srėbnaja daroha'
    }

    def "Bulgarian with ISO"() {
        expect:
        Romanizer.romanize(source, 'bg')['bg-Latn-t-bg-Cyrl-m0-iso-1995'] == target
        where:
        source || target
        // https://libris.kb.se/fzr6pkkr2vnc152#it
        'Баба праща поздрави и се извинява' || 'Baba prašta pozdravi i se izvinjava'
        // https://libris.kb.se/jxqb93w0gxhtbf7d#it
        "Белия зъб" || "Belija zăb"
    }

    def "Macedonian with ISO"() {
        expect:
        Romanizer.romanize(source, 'mk')['mk-Latn-t-mk-Cyrl-m0-iso-1995'] == target
        where:
        source || target
        // https://libris.kb.se/wbxc6926tctq1kz6#it
        "Бележникот на Грег Хефли" || "Beležnikot na Greg Hefli"
        // https://libris.kb.se/zcxzphvrwtgv8cdc#it 	
        //"Биби, Боби и чудовиштето од Охридското езеро" || "Bibi, Bobi i čudovišteto od Ohridskogoto ezero" 
        "Биби, Боби и чудовиштето од Охридското езеро" || "Bibi, Bobi i čudovišteto od Ohridskoto ezero"
    }

    def "Serbian with ISO"() {
        expect:
        Romanizer.romanize(source, 'sr')['sr-Latn-t-sr-Cyrl-m0-iso-1995'] == target
        where:
        source || target
        // https://libris.kb.se/2dbbcc810dxjnk9l#it
        "Узбуна у кући белих медведа" || "Uzbuna u kući belih medveda"
        // https://libris.kb.se/dqnnpp1sbxm436wp#it
        'Тиги, хаjдемо у шетњу' || 'Tigi, hajdemo u šetnju'
    }

    def "Ukrainian with ISO"() {
        expect:
        Romanizer.romanize(source, 'uk')['uk-Latn-t-uk-Cyrl-m0-iso-1995'] == target
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

    def "Azerbaijani in Cyrillic with ALA-LOC"() {
        expect:
        Romanizer.romanize(source, 'az')['az-Latn-t-az-Cyrl-m0-alaloc'] == target
        where:
        source || target
        "АБВГҒДЕӘЖЗИЫЈКҜЛМНОӨПРСТУҮФХҺЧҸШ" || "ABVGGhDEĂZHZIYI̐KĠLMNOȮPRSTUU̇FKHḢCHJSH"
        "абвгғдеәжзиыјкҝлмноөпрстуүфхһчҹш" || "abvgghdeăzhziyi̐kġlmnoȯprstuu̇fkhḣchjsh"
    }

    def "Armenian with ALA-LOC"() {
        expect:
        Romanizer.romanize(source, 'hy')['hy-Latn-t-hy-Armn-m0-alaloc'] == target
        where:
        source                            || target

        // https://www.loc.gov/catdir/cpso/romanization/armenian.pdf
        // Note 3
        "պատսպարան"                       || "patʹsparan"
        "կրտսեր"                          || "krtʹser"
        "Դզնունի"                         || "Dʹznuni"
        "մոտս"                            || "motʹs"
        "տստակ"                           || "tʹstak"
        "կհալ"                            || "kʹhal"
        "սիկհ"                            || "sikʹh"
        "Գհուկ"                           || "Gʹhuk"
        "ՍՍՀ"                             || "SSʹH"
        "սհաթ"                            || "sʹhatʻ"

        // Note 6.
        "դպրեվանք"                        || "dpreʹvankʻ"
        "ագեվազ"                          || "ageʹvaz"
        "Քարեվանք"                        || "Kʻareʹvankʻ"
        "հոգեվարք"                        || "hogeʹvarkʻ"
        "հոգեվիճակ"                       || "hogeʹvichak"
        "ոսկեվազ"                         || "oskeʹvaz"
        "ոսկեվարս"                        || "oskeʹvars" // "osokeʹvars" in spec looks wrong to me...
        "ուղեվճար"                        || "ugheʹvchar"
        "գերեվարել"                       || "gereʹvarel"
        "գինեվաճառ"                       || "gineʹvachaṛ"
        "գինեվարպետ"                      || "gineʹvarpet"
        "դափնեվարդ"                       || "dapʻneʹvard"
        "կարեվեր"                         || "kareʹver"

        // Note 6
        "Երևան"                           || "Erevan"
        "ԵՐԵՎԱՆ"                          || "EREVAN" // "Erevan" in spec ?
        "ևեթ"                             || "evetʻ"
        "ևս"                              || "evs"

        // Armenian Punctuation 1
        "«Սարդարապատի պատմաշինությունը» և այլ երկեր" || '"Sardarapati patmashinutʻyuně" ev ayl erker'
        
        // Armenian Punctuation 2
        "Դու կարդացե՞լ ես այս գիրքը"      || "Du kardatsʻel es ays girkʻě?"
        "Դու կարդացե՞լ ես այս գիրքը: Դու" || "Du kardatsʻel es ays girkʻě? Du"

        // Armenian Punctuation 3
        "Այս ու՜մ եմ տեսնում"             || "Ays um em tesnum!"
        "Այս ու՜մ եմ տեսնում: Այս"        || "Ays um em tesnum! Ays"

        // Armenian Punctuation 4
        "Կարդա՛, գրի՛ր տարին բոլոր"                  || "Karda, grir tarin bolor"

        // Armenian Punctuation 5 
        "Բարձրանում է աշխարհ մի անեզր, որ քոնն է անշուշտ և իմը:" || "Bardzranum ē ashkharh mi anezr, or kʻonn ē anshusht ev imě."
        
        // More examples from spec
        "Բարձրանում է աշխարհ մի անեզր, որ քոնն է անշուշտ և իմը." || "Bardzranum ē ashkharh mi anezr, or kʻonn ē anshusht ev imě."
        "Չիփչու Նիչուն և կամակորները"                            || "Chʻipʻchʻu Nichʻun ev kamakornerě"

        // All caps
        "ԲԱՐՁՐԱՆՈՒՄ Է ԱՇԽԱՐՀ ՄԻ ԱՆԵԶՐ, ՈՐ ՔՈՆՆ Է ԱՆՇՈՒՇՏ ԵՒ ԻՄԸ." || "BARDZRANUM Ē ASHKHARH MI ANEZR, OR KʻONN Ē ANSHUSHT EV IMĚ."
        "ՉԻՓՉՈՒ ՆԻՉՈՒՆ ԵՒ ԿԱՄԱԿՈՐՆԵՐԸ"                            || "CHʻIPʻCHʻU NICHʻUN EV KAMAKORNERĚ"
    }

    def "Amharic with ALA-LOC"() {
        expect:
        Romanizer.romanize(source, 'am')['am-Latn-t-am-Ethi-m0-alaloc'] == target
        where:
        source            || target
        "ባለረጅም"           || "bālaraǧem"
        "የእግር"            || "yaʼeger"
        // t8g2d3p4r89plb7s
        "ዘነበና"            || "zanabanā"
        // "ዘንጋዳው"  || "zangādāw" // Unhandled 6th order
        // 2ldvpdwd3gfs5td
        "የፍቅር"            || "yafeqer"
        // "እርግማን"  || "'ergmān" // Unhandled 6th order
        // sb4h6wd44q82dsx
        "ኩርፊያ የሸፈነው ፈገግታ" || "kurfiyā yašafanaw fagagetā"
        "ግለ ታሪክ"          || "gela tārik"
        //"ሥዩም" | "s̍eyum" // diacritic in record looks wrong
        "ሥዩም"              | "śeyum"
        // "ወልደ" | "waledé" // waleda looks correct? ደ da
        // "ራምሴ" | "rāmsé" // Unhandled 6th order
        //"ወ/ሮ መሠረት ባልቻ" | "w/ro maśarat, bālčā" // Unhandled 6th order
        "መሠረት"            || "maśarat"
        // q3wn8w0sns6z7q2v
        "ደፋርዋ ዶሮ"         || "dafārwā doro"
    }
}
