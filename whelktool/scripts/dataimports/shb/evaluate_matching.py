from evaluate_matching_test_cases import TEST_CASES
from identify_matches import prepare_record, get_match_score

THRESHOLD = 0.80


def test_pair(records, shb_id, match_id, expected):
    shb = records[shb_id]
    match = records[match_id]

    shb_prepped = prepare_record(shb)
    match_prepped = prepare_record(match)

    score = get_match_score(shb_prepped, match_prepped)

    result = score >= 0.80  # adjust this threshold

    status = "✓" if result == expected else "✗"

    print(
        f"{status} "
        f"{shb_id} → {match_id} "
        f"score={score:.3f} "
        f"expected={'MATCH' if expected else 'NO MATCH'} "
        f"actual={'MATCH' if result else 'NO MATCH'}"
    )

    return score


if __name__ == "__main__":

    false_positives = 0
    false_negatives = 0

    print(f"\nEvaluating with threshold {THRESHOLD}")

    print("\nKNOWN MATCHES")
    print("-" * 60)

    for case in TEST_CASES:
        shb = case["shb_rec"]["@graph"][1]
        libris = case["libris_rec"]["@graph"][1]

        shb_id = shb["@id"]
        libris_id = libris["@id"]

        shb_prepped = prepare_record(shb)
        libris_prepped = prepare_record(libris)

        score, individual_scores = get_match_score(shb_prepped, libris_prepped)

        actual_match = score >= THRESHOLD
        expected = case["expected_to_match"]

        if actual_match and not expected:
            result = "FALSE MATCH"
            false_positives += 1
        elif not actual_match and expected:
            result = "FALSE NON-MATCH"
            false_negatives += 1
        elif actual_match:
            result = "TRUE MATCH"
        else:
            result = "TRUE NON-MATCH"

        print(
            f"{result:16} "
            f"Total score={score:.3f}  "
            f"SHB={shb_id}  "
            f"MATCH={libris_id}  "
            f"{case.get('description', '')}"
        )

        if "FALSE" in result:
            print(f"Scores by property: {individual_scores}  ")
            print(f"SHB: {shb_prepped}")
            print(f"Libris: {libris_prepped}")

    print()
    print(f"False positives: {false_positives}")
    print(f"False negatives: {false_negatives}\n")