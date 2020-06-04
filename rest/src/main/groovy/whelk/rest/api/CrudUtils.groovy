package whelk.rest.api

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.google.common.net.MediaType
import groovy.util.logging.Log4j2 as Log
import org.apache.commons.io.FilenameUtils
import org.apache.http.HeaderElement
import org.apache.http.NameValuePair
import org.apache.http.message.BasicHeaderValueParser

import javax.servlet.http.HttpServletRequest

@Log
class CrudUtils {
    final static MediaType JSON = MediaType.parse(MimeTypes.JSON)
    final static MediaType JSONLD = MediaType.parse(MimeTypes.JSONLD)

    static final Map ALLOWED_MEDIA_TYPES_BY_EXT = [
            '': [JSONLD, JSON],
            'jsonld': [JSONLD],
            'json': [JSON],
    ]

    static String getBestContentType(HttpServletRequest request) {
        def header = getAcceptHeader(request)
        def desired = parseAcceptHeader(header)
        def allowed = allowedMediaTypes(request)

        MediaType best = getBestMatchingMimeType(allowed, desired)

        if (!best) {
            throw new UnsupportedContentTypeException(header)
        }

        return best.toString()
    }

    private static List<MediaType> allowedMediaTypes(HttpServletRequest request) {
        String extension = FilenameUtils.getExtension(request.getRequestURI())

        if (ALLOWED_MEDIA_TYPES_BY_EXT.containsKey(extension)) {
            return ALLOWED_MEDIA_TYPES_BY_EXT.get(extension)
        }
        else {
            throw new Crud.NotFoundException('.' + extension)
        }
    }

    private static String getAcceptHeader(HttpServletRequest request) {
        String acceptHeader = request.getHeader("Accept")

        /**
         * from w3.org:
         * If no Accept header field is present, then it is assumed that the client accepts all media types.
         */
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

    static String cleanEtag(String str) {
        return stripQuotes(str)?.replaceAll('-gzip', '')
    }

    private static String stripQuotes(String str) {
        return str?.replaceAll('"', '')
    }

    /**
     * Returns a sorted list of media types accepted by the client
     */
    static List<MediaType> parseAcceptHeader(String header) {
        BasicHeaderValueParser parser = new BasicHeaderValueParser()

        List<AcceptMediaType> mediaTypes = []
        for (HeaderElement h : BasicHeaderValueParser.parseElements(header, parser)) {
            mediaTypes << AcceptMediaType.fromHeaderElement(h)
        }

        return mediaTypes.sort().collect { it.mediaType }
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
            catch (IllegalArgumentException e) {
                throw new BadRequestException('Invalid media type in Accept header': element.toString())
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
}
