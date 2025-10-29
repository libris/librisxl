package whelk.rest.api

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.google.common.net.MediaType
import groovy.util.logging.Log4j2 as Log
import org.apache.commons.io.FilenameUtils
import org.apache.hc.core5.http.HeaderElement
import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicHeaderValueParser
import org.apache.hc.core5.http.message.ParserCursor
import whelk.util.http.BadRequestException
import whelk.util.http.MimeTypes
import whelk.util.http.NotFoundException
import whelk.util.http.UnsupportedContentTypeException

import javax.servlet.http.HttpServletRequest
import java.lang.management.ManagementFactory

@Log
class CrudUtils {
    final static MediaType JSON = MediaType.parse(MimeTypes.JSON)
    final static MediaType JSONLD = MediaType.parse(MimeTypes.JSONLD)
    final static MediaType TURTLE = MediaType.parse(MimeTypes.TURTLE)
    final static MediaType TRIG = MediaType.parse(MimeTypes.TRIG)
    final static MediaType RDFXML = MediaType.parse(MimeTypes.RDF)
    final static MediaType N3 = MediaType.parse(MimeTypes.N3)

    static final Map ALLOWED_MEDIA_TYPES_BY_EXT = [
            '': [JSONLD, JSON],
            'jsonld': [JSONLD],
            'json': [JSON],
            'trig': [TRIG],
            'ttl': [TURTLE],
            'xml': [RDFXML],
            'rdf': [RDFXML],
            'n3': [N3]
    ]

    protected static Map<String, String> EXTENSION_BY_MEDIA_TYPE = [:]
    static {
        ALLOWED_MEDIA_TYPES_BY_EXT.each { ext, mediatypes ->
            if (ext.size() > 0) {
                mediatypes.each {
                    EXTENSION_BY_MEDIA_TYPE[it.toString()] = ext
                }
            }
        }
    }

    static final List ALLOWED_MEDIA_TYPES = [JSON, JSONLD, TRIG, TURTLE, RDFXML, N3]

    static String getBestContentType(String acceptHeader, String resourcePath) {
        def desired = parseAcceptHeader(acceptHeader)
        def allowed = allowedMediaTypes(resourcePath, desired)

        MediaType best = getBestMatchingMimeType(allowed, desired)

        log.debug("Desired Content-Type: ${desired}")
        log.debug("Allowed Content-Type: ${allowed}")
        log.debug("Best Content-Type: ${best}")

        if (!best) {
            throw new UnsupportedContentTypeException(acceptHeader)
        }

        return best.toString()
    }

    private static List<MediaType> allowedMediaTypes(String resourcePath, List desired) {
        String extension = FilenameUtils.getExtension(resourcePath) ?: ''

        List mediaTypeIntersect = ALLOWED_MEDIA_TYPES.intersect(desired)

        // If no extension specified but Accept given, try Accept values first.
        // Otherwise, if extension (including no extension) specified, try that.
        if (mediaTypeIntersect.size() > 0 && extension == '') {
            return mediaTypeIntersect
        } else if (ALLOWED_MEDIA_TYPES_BY_EXT.containsKey(extension)) {
            return ALLOWED_MEDIA_TYPES_BY_EXT.get(extension)
        } else {
            if (extension) {
                throw new NotFoundException('.' + extension)
            } else {
                throw new NotFoundException("${mediaTypeIntersect}")
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
        String acceptHeader = request.getHeader("Accept")
        if (acceptHeader == null) {
            acceptHeader = '*/*'
        }
        return acceptHeader
    }

    static MediaType getBestMatchingMimeType(List<MediaType> allowedMimeTypes,
                                             List<MediaType> desiredMimeTypes) {
        for (MediaType desired : desiredMimeTypes) {
            for (MediaType allowed : allowedMimeTypes) {
                if (allowed.is(desired)) {
                    return allowed
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
        return allowedMimeTypes.isEmpty() ? JSONLD : allowedMimeTypes[0]
    }
    
    /**
     * Returns a sorted list of media types accepted by the client
     */
    static List<MediaType> parseAcceptHeader(String header) {
        BasicHeaderValueParser parser = new BasicHeaderValueParser()
        ParserCursor cursor = new ParserCursor(0, header.length())

        List<AcceptMediaType> mediaTypes = []
        for (HeaderElement h : parser.parseElements(header, cursor)) {
            mediaTypes << AcceptMediaType.fromHeaderElement(h)
        }

        return mediaTypes.sort().collect { it.mediaType }
    }

    static Optional<ETag> getIfNoneMatch(HttpServletRequest request) {
        return Optional
                .ofNullable(request.getHeader("If-None-Match"))
                .map(ETag.&parse)
    }

    private static class AcceptMediaType implements Comparable<AcceptMediaType> {
        private MediaType mediaType
        private float q

        AcceptMediaType(MediaType mediaType, float q) {
            this.mediaType = mediaType
            this.q = q
        }

        @Override
        String toString() {
            return "${mediaType} (q:${q})"
        }

        @Override
        int compareTo(AcceptMediaType other) {
            if (q != other.q) {
                return other.q <=> q
            }
            if (mediaType.type() == '*' && other.mediaType.type() != '*') {
                return 1
            }
            if (mediaType.type() != '*' && other.mediaType.type() == '*') {
                return -1
            }
            if (mediaType.type() != mediaType.type()) {
                return 0
            }
            if (mediaType.subtype() == '*' && other.mediaType.subtype() != '*') {
                return 1
            }
            if (mediaType.subtype() != '*' && other.mediaType.subtype() == '*') {
                return -1
            }
            if (mediaType.subtype() != mediaType.subtype()) {
                return 0
            }
            return other.mediaType.parameters().size() <=> mediaType.parameters().size()
        }

        static AcceptMediaType fromHeaderElement(HeaderElement element) {
            ListMultimap<String, String> parameters = new ArrayListMultimap<>()
            float q = 1.0
            for (NameValuePair p : element.getParameters()) {
                if ('q' == p.getName()) {
                    q = parseQ(p.getValue())
                    break // ignore 'Accept extension parameters'
                }
                parameters.put(p.getName(), p.getValue())
            }

            try {
                MediaType m = MediaType.parse(element.name).withParameters(parameters)
                return new AcceptMediaType(m, q)
            }
            catch (IllegalArgumentException ignored) {
                throw new BadRequestException("Invalid media type in Accept header': $element")
            }
        }

        private static float parseQ(String value) {
            try {
                return Float.parseFloat(value)
            }
            catch (NumberFormatException e) {
                throw new BadRequestException("Invalid q value in Accept header:" + value)
            }
        }
    }

    static class ETag {
        static final ETag SYSTEM_START = plain("${ManagementFactory.getRuntimeMXBean().getStartTime()}")
        
        private static final String SEPARATOR = ':'
        
        private String plain = null
        private String embellished = null

        private ETag(String checksum) {
            plain = checksum
        }

        private ETag(String checksum, String checksumEmbellished) {
            plain = checksum
            embellished = checksumEmbellished
        }

        String documentCheckSum() {
            return plain
        }
        
        static ETag parse(String eTag) {
            if(!eTag) {
                return plain(null)
            }
            
            eTag = cleanEtag(eTag)
            
            if (eTag.contains(SEPARATOR)) {
                def e = eTag.split(SEPARATOR)
                embellished(e[0], e[1])
            }
            else {
                plain(eTag)
            }
        }


        static ETag plain(String checksum) {
            new ETag(checksum)
        }

        static ETag embellished(String checksum, String checksumEmbellish) {
            new ETag(checksum, checksumEmbellish)
        }

        String toString() {
            "\"${ embellished ? "$plain$SEPARATOR$embellished" : plain }\""
        }

        boolean isNotModified(ETag ifNoneMatch) {
            if (plain != ifNoneMatch.plain) {
                return false
            }
            if (embellished && embellished != ifNoneMatch.embellished) {
                return false
            }
            return true
        }

        private static String cleanEtag(String str) {
            return stripQuotes(str)?.replaceAll('W/', '')?.replaceAll('-gzip', '')
        }

        private static String stripQuotes(String str) {
            return str?.replaceAll('"', '')
        }
    }
}
