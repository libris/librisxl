package whelk.util;

import com.ibm.icu.text.Transliterator;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Romanizer {
    private static final Logger log = LoggerFactory.getLogger(Romanizer.class);

    /** Languages that use ALA-LOC "Asian Cyrillic - Multi-purpose transliteration for non-Slavic Cyrillic scripts"
     https://github.com/lcnetdev/transliterator/blob/main/scriptshifter/tables/data/index.yml

     Languages in the original list that have not been mapped to lang codes yet (because we don't have them in Libris):
     Abaza, Aisor, Chukchi, Dungan, Even, Evenki, Gagauz, Inuit, Khakass, Khanty, Komi-Permyak,
     Koryak, Lak, Lapp, Mansi, Molodstov, Mordvin, Nanai, Nenets, Nivkh, Shor, Permyak, Tabasaran,
     Tat (tat?), Tuva, Udekhe */
    private static final List<String> ALA_LOC_NON_SLAVIC_CYRILLIC = List.of(
            "abk", "ady", "alt", "ava", "bak", "bua", "che", "chm", "chv", "dar",
            "inh", "kaa", "kbd", "kom", "krc", "krl", "kum", "lez", "lit", "nog",
            "oss", "rom", "rum", "rum", "sah", "sel", "tut", "udm", "xal"
    );

    private static final List<Transform> AUTO = buildAuto();

    private static List<Transform> buildAuto() {
        List<Transform> transforms = new ArrayList<>(List.of(
                auto("be-Cyrl", "be-Latn-t-be-Cyrl-m0-iso-1968", List.of("be-iso.txt", "slavic-iso.txt")),
                auto("bg-Cyrl", "bg-Latn-t-bg-Cyrl-m0-iso-1968", List.of("bg-iso.txt", "slavic-iso.txt")),
                auto("bg-Cyrl", "bg-Latn-t-bg-Cyrl-x0-kr76", List.of("cyrl-kr76.txt", "bg-kr76.txt")),
                auto("bs-Cyrl", "bs-Latn-t-bs-Cyrl-m0-iso-1968", List.of("bs-sr-iso.txt", "slavic-iso.txt")),
                auto("bs-Cyrl", "bs-Latn-t-bs-Cyrl-x0-kr76", List.of("cyrl-kr76.txt", "bs-sr-kr76.txt")),
                auto("el", "el-Latn-t-el-Grek-x0-btj", List.of("el-btj.txt")),
                auto("grc", "grc-Latn-t-grc-Grek-x0-skr-1980", List.of("grc-skr.txt")),
                auto("yi-Hebr", "yi-Latn-t-yi-Hebr-x0-yivo", List.of("yi-yivo.txt")),
                auto("yi-Hebr", "yi-Latn-t-yi-Hebr-m0-alaloc", List.of("yi-alaloc.txt")),
                auto("kk-Cyrl", "kk-Latn-t-kk-Cyrl-m0-iso-1995", List.of("kk-iso.txt")),
                auto("mk-Cyrl", "mk-Latn-t-mk-Cyrl-m0-iso-1968", List.of("mk-iso.txt", "slavic-iso.txt")),
                auto("mn-Cyrl", "mn-Latn-t-mn-Cyrl-x0-lessing", List.of("mn-lessing.txt")),
                auto("ru-Cyrl", "ru-Latn-t-ru-Cyrl-m0-iso-1968", List.of("ru-iso.txt", "slavic-iso.txt")),
                auto("ru-Cyrl", "ru-Latn-t-ru-Cyrl-x0-kr76", List.of("cyrl-kr76.txt", "ru-kr76.txt")),
                auto("sr-Cyrl", "sr-Latn-t-sr-Cyrl-m0-iso-1968", List.of("bs-sr-iso.txt", "slavic-iso.txt")),
                auto("sr-Cyrl", "sr-Latn-t-sr-Cyrl-x0-kr76", List.of("cyrl-kr76.txt", "bs-sr-kr76.txt")),
                auto("uk-Cyrl", "uk-Latn-t-uk-Cyrl-m0-iso-1968", List.of("uk-iso.txt", "slavic-iso.txt")),

                // Converted from LOC mappings
                // TODO: investigate how well these handle case/capitalization
                auto("am-Ethi", "am-Latn-t-am-Ethi-m0-alaloc", List.of("loc/am-Latn-t-am-Ethi-m0-alaloc.txt")),
                auto("az-Cyrl", "az-Latn-t-az-Cyrl-m0-alaloc", List.of("loc/az-Latn-t-az-Cyrl-m0-alaloc.txt")),
                auto("cu", "cu-Latn-t-cu-Cyrs-m0-alaloc", List.of("loc/cu-Latn-t-cu-Cyrs-m0-alaloc.txt")),
                auto("ka", "ka-Latn-t-ka-m0-alaloc", List.of("loc/ka-Latn-t-ka-m0-alaloc.txt")),
                auto("hi-Deva", "hi-Latn-t-hi-Deva-m0-alaloc", List.of("loc/hi-Latn-t-hi-Deva-m0-alaloc.txt")),
                auto("hy-Armn", "hy-Latn-t-hy-Armn-m0-alaloc", List.of("loc/hy-Latn-t-hy-Armn-m0-alaloc.txt")),
                auto("ky-Cyrl", "ky-Latn-t-ky-Cyrl-m0-alaloc", List.of("loc/ky-Latn-t-ky-Cyrl-m0-alaloc.txt")),
                auto("mn-Mong", "mn-Latn-t-mn-Mong-m0-alaloc", List.of("loc/mn-Latn-t-mn-Mong-m0-alaloc.txt")),
                auto("ti-Ethi", "ti-Latn-t-ti-Ethi-m0-alaloc", List.of("loc/am-Latn-t-am-Ethi-m0-alaloc.txt")),
                auto("tt-Cyrl", "tt-Latn-t-tt-Cyrl-m0-alaloc", List.of("loc/tt-Latn-t-tt-Cyrl-m0-alaloc.txt")),
                auto("tg-Cyrl", "tg-Latn-t-tg-Cyrl-m0-alaloc", List.of("loc/tg-Latn-t-tg-Cyrl-m0-alaloc.txt")),
                auto("tk-Cyrl", "tk-Latn-t-tk-Cyrl-m0-alaloc", List.of("loc/tk-Latn-t-tk-Cyrl-m0-alaloc.txt")),
                auto("uz-Cyrl", "uz-Latn-t-uz-Cyrl-m0-alaloc", List.of("loc/uz-Latn-t-uz-Cyrl-m0-alaloc.txt")),
                auto("zh-Hani", "zh-Latn-t-zh-Hani-m0-alaloc", List.of("loc/zh-Latn-t-zh-Hani-m0-alaloc.txt"))
        ));
        transforms.addAll(alaLocNonSlavicCyrillic());
        return transforms;
    }

    private final Map<String, List<Transform>> transliterators = new HashMap<>();

    public Romanizer() {
        this(AUTO.stream().map(Transform::targetTag).toList());
    }

    public Romanizer(List<String> enabledTargetTags) {
        for (String tag : enabledTargetTags) {
            List<Transform> matching = AUTO.stream().filter(t -> t.targetTag().equals(tag)).toList();
            if (matching.isEmpty()) {
                add(new Manual(tag));
            } else {
                matching.forEach(this::add);
            }
        }

        List<Transform> t = transliterators.values().stream().flatMap(List::stream).toList();
        log.info("Initialized with {} transliterators: {}", t.size(),
                t.stream().map(Object::toString).sorted().collect(Collectors.toList()));
    }

    public Map<String, String> romanize(String s, String langTag) {
        String normalized = Unicode.normalize(s);
        List<Transform> transforms = transliterators.get(langTag);
        if (transforms == null) {
            transforms = Unicode.guessIso15924ScriptCode(normalized)
                    .map(code -> transliterators.get(langTag + "-" + code))
                    .orElse(List.of());
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (Transform transform : transforms) {
            result.put(transform.targetTag(), transform.transform(normalized));
        }
        return result;
    }

    public boolean isMaybeRomanizable(String langTag) {
        return !isTransformed(langTag) && transliterators.keySet().stream().anyMatch(sourceTag -> sourceTag.startsWith(langTag));
    }

    private void add(Transform transform) {
        transliterators.computeIfAbsent(transform.sourceTag(), s -> new ArrayList<>()).add(transform);
    }

    private static boolean isTransformed(String langTag) {
        return langTag.contains("-t-");
    }

    private static Transform auto(String sourceTag, String targetTag, List<String> filenames) {
        Transliterator t = Transliterator.createFromRules(targetTag,
                filenames.stream().map(Romanizer::readFromResources).collect(Collectors.joining("\n")),
                Transliterator.FORWARD);
        return new Auto(sourceTag, t);
    }

    private static List<Transform> alaLocNonSlavicCyrillic() {
        return ALA_LOC_NON_SLAVIC_CYRILLIC.stream()
                .map(tag -> auto(tag + "-Cyrl", tag + "-Latn-t-" + tag + "-Cyrl-m0-alaloc", List.of("loc/und-Latn-t-und-Cyrl-m0-alaloc.txt")))
                .collect(Collectors.toList());
    }

    private static String readFromResources(String filename) {
        try (InputStream is = Romanizer.class.getClassLoader().getResourceAsStream("romanizer/" + filename)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public interface Transform {
        String sourceTag();
        String targetTag();
        String transform(String s);
    }

    public static class Auto implements Transform {
        private final String sourceTag;
        private final Transliterator transliterator;

        public Auto(String sourceTag, Transliterator transliterator) {
            this.sourceTag = sourceTag;
            this.transliterator = transliterator;
        }

        @Override
        public String sourceTag() {
            return sourceTag;
        }

        @Override
        public String targetTag() {
            return transliterator.getID();
        }

        @Override
        public String transform(String source) {
            return transliterator.transform(source);
        }

        @Override
        public String toString() {
            return "A(" + sourceTag() + " -> " + targetTag() + ")";
        }
    }

    public static class Manual implements Transform {
        private static final Pattern SOURCE = Pattern.compile(".*-t-(.*?)(-\\p{Alpha}\\p{Digit}-.*)?$");
        private final String sourceTag;
        private final String targetTag;

        public Manual(String targetTag) {
            var m = SOURCE.matcher(targetTag);
            if (m.matches()) {
                this.sourceTag = m.group(1);
            } else {
                this.sourceTag = "---------";
                // TODO: throw or log
            }
            this.targetTag = targetTag;
        }

        @Override
        public String sourceTag() {
            return sourceTag;
        }

        @Override
        public String targetTag() {
            return targetTag;
        }

        @Override
        public String transform(String s) {
            return "";
        }

        @Override
        public String toString() {
            return "M(" + sourceTag() + " -> " + targetTag() + ")";
        }
    }
}
