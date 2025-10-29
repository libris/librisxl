package whelk.rest.api;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.net.MediaType;
import org.apache.commons.io.FilenameUtils;
import org.apache.hc.core5.http.HeaderElement;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeaderValueParser;
import org.apache.hc.core5.http.message.ParserCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.util.http.BadRequestException;
import whelk.util.http.MimeTypes;
import whelk.util.http.NotFoundException;
import whelk.util.http.UnsupportedContentTypeException;

import javax.servlet.http.HttpServletRequest;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.stream.Collectors;

class CrudUtils {
    private static final Logger log = LogManager.getLogger(CrudUtils.class);
    static final MediaType JSON = MediaType.parse(MimeTypes.JSON);
    static final MediaType JSONLD = MediaType.parse(MimeTypes.JSONLD);
    static final MediaType TURTLE = MediaType.parse(MimeTypes.TURTLE);
    static final MediaType TRIG = MediaType.parse(MimeTypes.TRIG);
    static final MediaType RDFXML = MediaType.parse(MimeTypes.RDF);
    static final MediaType N3 = MediaType.parse(MimeTypes.N3);

    static final Map<String, List<MediaType>> ALLOWED_MEDIA_TYPES_BY_EXT = new HashMap<>();
    static {
        ALLOWED_MEDIA_TYPES_BY_EXT.put("", Arrays.asList(JSONLD, JSON));
        ALLOWED_MEDIA_TYPES_BY_EXT.put("jsonld", List.of(JSONLD));
        ALLOWED_MEDIA_TYPES_BY_EXT.put("json", List.of(JSON));
        ALLOWED_MEDIA_TYPES_BY_EXT.put("trig", List.of(TRIG));
        ALLOWED_MEDIA_TYPES_BY_EXT.put("ttl", List.of(TURTLE));
        ALLOWED_MEDIA_TYPES_BY_EXT.put("xml", List.of(RDFXML));
        ALLOWED_MEDIA_TYPES_BY_EXT.put("rdf", List.of(RDFXML));
        ALLOWED_MEDIA_TYPES_BY_EXT.put("n3", List.of(N3));
    }

    protected static Map<String, String> EXTENSION_BY_MEDIA_TYPE = new HashMap<>();
    static {
        for (Map.Entry<String, List<MediaType>> entry : ALLOWED_MEDIA_TYPES_BY_EXT.entrySet()) {
            String ext = entry.getKey();
            List<MediaType> mediatypes = entry.getValue();
            if (!ext.isEmpty()) {
                for (MediaType mediaType : mediatypes) {
                    EXTENSION_BY_MEDIA_TYPE.put(mediaType.toString(), ext);
                }
            }
        }
    }

    static final List<MediaType> ALLOWED_MEDIA_TYPES = List.of(JSON, JSONLD, TRIG, TURTLE, RDFXML, N3);

    static String getBestContentType(String acceptHeader, String resourcePath) {
        List<MediaType> desired = parseAcceptHeader(acceptHeader);
        List<MediaType> allowed = allowedMediaTypes(resourcePath, desired);

        MediaType best = getBestMatchingMimeType(allowed, desired);

        log.debug("Desired Content-Type: {}", desired);
        log.debug("Allowed Content-Type: {}", allowed);
        log.debug("Best Content-Type: {}", best);

        if (best == null) {
            throw new UnsupportedContentTypeException(acceptHeader);
        }

        return best.toString();
    }

    private static List<MediaType> allowedMediaTypes(String resourcePath, List<MediaType> desired) {
        String extension = FilenameUtils.getExtension(resourcePath);
        if (extension == null) {
            extension = "";
        }

        List<MediaType> mediaTypeIntersect = ALLOWED_MEDIA_TYPES.stream()
                .filter(desired::contains)
                .collect(Collectors.toList());

        // If no extension specified but Accept given, try Accept values first.
        // Otherwise, if extension (including no extension) specified, try that.
        if (!mediaTypeIntersect.isEmpty() && extension.isEmpty()) {
            return mediaTypeIntersect;
        } else if (ALLOWED_MEDIA_TYPES_BY_EXT.containsKey(extension)) {
            return ALLOWED_MEDIA_TYPES_BY_EXT.get(extension);
        } else {
            if (!extension.isEmpty()) {
                throw new NotFoundException("." + extension);
            } else {
                throw new NotFoundException(mediaTypeIntersect.toString());
            }
        }
    }


    /**
     * Get Accept header from request.
     *
     * If no Accept header field is present, then it is assumed that the client
     * accepts all media types.
     *
     * (See: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html)
     */
    static String getAcceptHeader(HttpServletRequest request) {
        String acceptHeader = request.getHeader("Accept");
        if (acceptHeader == null) {
            acceptHeader = "*/*";
        }
        return acceptHeader;
    }

    static MediaType getBestMatchingMimeType(List<MediaType> allowedMimeTypes,
                                             List<MediaType> desiredMimeTypes) {
        for (MediaType desired : desiredMimeTypes) {
            for (MediaType allowed : allowedMimeTypes) {
                if (allowed.is(desired)) {
                    return allowed;
                }

            }
        }

        /**
         https://tools.ietf.org/html/rfc7231#section-5.3.2
         "A request without any Accept header field implies that the user agent
         will accept any media type in response. If the header field is
         present in a request and none of the available representations for
         the response have a media type that is listed as acceptable, the
         origin server can either honor the header field by sending a 406 (Not
         Acceptable) response or disregard the header field by treating the
         response as if it is not subject to content negotiation."

         => disregard header
         */
        return allowedMimeTypes.isEmpty() ? JSONLD : allowedMimeTypes.getFirst();
    }
    
    /**
     * Returns a sorted list of media types accepted by the client
     */
    static List<MediaType> parseAcceptHeader(String header) {
        BasicHeaderValueParser parser = new BasicHeaderValueParser();
        ParserCursor cursor = new ParserCursor(0, header.length());

        List<AcceptMediaType> mediaTypes = new ArrayList<>();
        for (HeaderElement h : parser.parseElements(header, cursor)) {
            mediaTypes.add(AcceptMediaType.fromHeaderElement(h));
        }

        Collections.sort(mediaTypes);
        return mediaTypes.stream()
                .map(amt -> amt.mediaType)
                .collect(Collectors.toList());
    }

    static Optional<ETag> getIfNoneMatch(HttpServletRequest request) {
        return Optional
                .ofNullable(request.getHeader("If-None-Match"))
                .map(ETag::parse);
    }

    private static class AcceptMediaType implements Comparable<AcceptMediaType> {
        private final MediaType mediaType;
        private final float q;

        AcceptMediaType(MediaType mediaType, float q) {
            this.mediaType = mediaType;
            this.q = q;
        }

        @Override
        public String toString() {
            return mediaType + " (q:" + q + ")";
        }

        @Override
        public int compareTo(AcceptMediaType other) {
            if (q != other.q) {
                return Float.compare(other.q, q);
            }
            if (mediaType.type().equals("*") && !other.mediaType.type().equals("*")) {
                return 1;
            }
            if (!mediaType.type().equals("*") && other.mediaType.type().equals("*")) {
                return -1;
            }
            if (!mediaType.type().equals(other.mediaType.type())) {
                return 0;
            }
            if (other.mediaType.type().equals("*") && !other.mediaType.subtype().equals("*")) {
                return 1;
            }
            if (!mediaType.subtype().equals("*") && other.mediaType.subtype().equals("*")) {
                return -1;
            }
            if (!mediaType.subtype().equals(other.mediaType.subtype())) {
                return 0;
            }
            return Integer.compare(other.mediaType.parameters().size(), mediaType.parameters().size());
        }

        static AcceptMediaType fromHeaderElement(HeaderElement element) {
            ListMultimap<String, String> parameters = ArrayListMultimap.create();
            float q = 1.0f;
            for (NameValuePair p : element.getParameters()) {
                if ("q".equals(p.getName())) {
                    q = parseQ(p.getValue());
                    break; // ignore 'Accept extension parameters'
                }
                parameters.put(p.getName(), p.getValue());
            }

            try {
                MediaType m = MediaType.parse(element.getName()).withParameters(parameters);
                return new AcceptMediaType(m, q);
            }
            catch (IllegalArgumentException ignored) {
                throw new BadRequestException("Invalid media type in Accept header': " + element);
            }
        }

        private static float parseQ(String value) {
            try {
                return Float.parseFloat(value);
            }
            catch (NumberFormatException e) {
                throw new BadRequestException("Invalid q value in Accept header:" + value);
            }
        }
    }

    static class ETag {
        private static final String SEPARATOR = ":";

        private String plain = null;
        private String embellished = null;

        private ETag(String checksum) {
            plain = checksum;
        }

        private ETag(String checksum, String checksumEmbellished) {
            plain = checksum;
            embellished = checksumEmbellished;
        }

        String documentCheckSum() {
            return plain;
        }
        
        static ETag parse(String eTag) {
            if (eTag == null || eTag.isEmpty()) {
                return plain(null);
            }

            eTag = cleanEtag(eTag);

            if (eTag.contains(SEPARATOR)) {
                String[] e = eTag.split(SEPARATOR);
                return embellished(e[0], e[1]);
            }
            else {
                return plain(eTag);
            }
        }


        static ETag plain(String checksum) {
            return new ETag(checksum);
        }

        static ETag embellished(String checksum, String checksumEmbellish) {
            return new ETag(checksum, checksumEmbellish);
        }

        public String toString() {
            return "\"" + (embellished != null ? plain + SEPARATOR + embellished : plain) + "\"";
        }

        boolean isNotModified(ETag ifNoneMatch) {
            if (!Objects.equals(plain, ifNoneMatch.plain)) {
                return false;
            }
            return embellished == null || Objects.equals(embellished, ifNoneMatch.embellished);
        }

        private static String cleanEtag(String str) {
            String result = stripQuotes(str);
            if (result != null) {
                result = result.replaceAll("W/", "");
                result = result.replaceAll("-gzip", "");
            }
            return result;
        }

        private static String stripQuotes(String str) {
            return str != null ? str.replaceAll("\"", "") : null;
        }
    }
}
