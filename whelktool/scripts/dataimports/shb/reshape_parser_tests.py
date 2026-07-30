import pytest

from reshape_shb import extract_properties_and_values


TEST_CASES = {
    "simple-author-with-initials": (
        "Surname, G.-N., Anything",
        (
            "Surname, G.-N.",
            "Anything",
            None,
            None,
            None,
            None,
            False,
        ),
    ),
    "simple-title-before-author": (
        "Anything. Surname, G.-N., Stuff.",
        (
            None,
            "Anything",
            None,
            None,
            None,
            "Surname, G.-N., Stuff.",
            False,
        ),
    ),
    "publication-year-before-monograph-extent": (
        "Schuck, A., H. Schücks enka & Co. AB 150 år. [Stockholm.] Sthlm 1947, 28 s.",
        (
            "Schuck, A.",
            "H. Schücks enka & Co. AB 150 år",
            None,
            "28 s.",
            None,
            "[Stockholm.] Sthlm 1947",
            False,
        ),
    ),
    "component-part-extent-in-parentheses": (
        "Meyerson, Å., Ett besök vid Stora Kopparberget och Sala gruva år 1662. (BBV 23 (1938), s. 325-343.)",
        (
            "Meyerson, Å.",
            "Ett besök vid Stora Kopparberget och Sala gruva år 1662",
            None,
            "s. 325-343",
            None,
            "(BBV 23 (1938).)",
            True,
        ),
    ),
    "component-part-with-subtitle-and-series": (
        'Davidsson, Åke, "En hoop Discantzböcker i godt förhwar..." : någotom Strängnäsgymnasiets musiksamling under 1600-talet. - I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976, s. 48-62',
        (
            "Davidsson, Åke",
            '"En hoop Discantzböcker i godt förhwar..."',
            "någotom Strängnäsgymnasiets musiksamling under 1600-talet",
            "s. 48-62",
            None,
            "I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976",
            True,
        ),
    ),
    "component-part-without-author-with-issn": (
        "Barton, H. Arnold, A bibliography of writings in English by or onrecent Swedish emigration historians. - I: The Swedish pioneer, ISSN0039-7326, 27, 1976:3, s. 215-221",
        (
            "Barton, H. Arnold",
            "A bibliography of writings in English by or onrecent Swedish emigration historians",
            None,
            "s. 215-221",
            "0039-7326",
            "I: The Swedish pioneer, ISSN0039-7326, 27, 1976:3",
            True,
        ),
    ),
    "monograph-with-contributors-written-two-ways": (
        "Jonsson, Inge, Swedenborg : sökaren i naturens och andens värld :hans verk och efterföljd / Inge Jonsson, Olle Hjern. -Stockholm, 1976. - 187 s.Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin",
        (
            "Jonsson, Inge",
            "Swedenborg",
            "sökaren i naturens och andens värld :hans verk och efterföljd",
            "187 s.",
            None,
            "Inge Jonsson, Olle Hjern. -Stockholm, 1976. -Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin",
            False,
        ),
    ),
    "monograph-with-series-statement-with-issn": (
        "Fries, Elias, Hembygdsperiodika : förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl. - Borås, 1976. - 40 bl. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)",
        (
            "Fries, Elias",
            "Hembygdsperiodika",
            "förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl",
            "40 bl.",
            "0347-1128",
            "Borås, 1976. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)",
            False,
        ),
    ),
    "monograph-with-series-and-dissertation-note-with-issn": (
        "Edvardsson, Lars, Kyrka och judendom : svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975. -Lund, 1976. - 194 s. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed",
        (
            "Edvardsson, Lars",
            "Kyrka och judendom",
            "svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975",
            "194 s.",
            "0346-5438",
            "Lund, 1976. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed",
            False,
        ),
    ),
    "monograph-without-secondary-title-with-issn": (
        "Frithz, Carl-Gösta, Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning. - Lund, 1976. - 428 s. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec",
        (
            "Frithz, Carl-Gösta",
            "Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning",
            None,
            "428 s.",
            "0519-9859",
            "Lund, 1976. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec",
            False,
        ),
    ),
    "multiple-authors-and-complex-multi-part-extent": (
        "Erichsen, B., & Krarup, A., Dansk historisk Bibliografi. Bd 1-3. Khvn1918-21, 1925-27, 1917. xiii, (1), 794 s. + viii, 655 s. + (2), iv, 806, (1) s.",
        (
            "Erichsen, B., & Krarup, A.",
            "Dansk historisk Bibliografi. Bd 1-3",
            None,
            "xiii, (1), 794 s. + viii, 655 s. + (2), iv, 806, (1) s.",
            None,
            "Khvn1918-21, 1925-27, 1917.",
            False,
        ),
    ),
}


@pytest.mark.parametrize("case_name", TEST_CASES)
def test_extract_properties_and_values(case_name):
    record, expected = TEST_CASES[case_name]

    actual = extract_properties_and_values(record, False)

    assert actual == expected, f"\nInput record:\n{record}"