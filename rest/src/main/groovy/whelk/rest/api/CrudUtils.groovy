package whelk.rest.api

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.google.common.net.MediaType
import groovy.util.logging.Log4j2 as Log
import org.apache.commons.io.FilenameUtils
import org.apache.http.HeaderElement
import org.apache.http.NameValuePair
import org.apache.http.message.BasicHeaderValueParser
import whelk.rest.api.MimeTypes

import javax.servlet.http.HttpServletRequest

@Log
class CrudUtils {
    static final List ALLOWED_MIME_TYPES = [ MimeTypes.JSONLD,
                                             MimeTypes.TURTLE,
                                             MimeTypes.RDF,
                                             MimeTypes.JSON
    ]

    static String getBestContentType(HttpServletRequest request) {
        return getBestContentType(getAcceptHeader(request), getPathSuffix(request))
    }

    private static String getPathSuffix(HttpServletRequest request) {
        return FilenameUtils.getExtension(request.getRequestURI())
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

    static String getBestContentType(String acceptHeader, String suffix) {
        List mimeTypes = parseAcceptHeader(acceptHeader).collect{ it.toString() }
        String suffixMimeType = getMimeTypeForSuffix(suffix)
        if (findMatchingMimeType(suffixMimeType, mimeTypes)) {
            mimeTypes.add(0, suffixMimeType)
        }
        return getBestMatchingMimeType(ALLOWED_MIME_TYPES, mimeTypes)
    }

    static String getMimeTypeForSuffix(String suffix) {
        String result
        switch (suffix) {
            case "jsonld":
                result = MimeTypes.JSONLD
                break
            case "json":
                result = MimeTypes.JSON
                break
            case "rdf":
                result = MimeTypes.RDF
                break
            case "ttl":
                result = MimeTypes.TURTLE
                break
            case '':
                break
            case null:
                break
            default:
                throw new Crud.NotFoundException(suffix)
        }
        return result
    }

    static String getBestMatchingMimeType(List allowedMimeTypes,
                                          List desiredMimeTypes) {
        for (String desiredMimeType : desiredMimeTypes) {
            // If we can, we want to serve JSON LD
            if (desiredMimeType == "*/*") {
                return MimeTypes.JSONLD
            } else {
                for (String allowedMimeType : allowedMimeTypes) {
                    if (isMatch(desiredMimeType, allowedMimeType)) {
                        return allowedMimeType
                    }
                }
            }
        }
        log.debug("No acceptable MIME type found.")
        return null
    }

    static String findMatchingMimeType(String soughtMimeType,
                                       List availableMimeTypes) {
        for (String availableMimeType : availableMimeTypes) {
            if (isMatch(soughtMimeType, availableMimeType)) {
                return availableMimeType
            }
        }
        return null
    }

    static boolean isMatch(String myMimeType, String yourMimeType) {
      // If either is null, we consider it a mismatch
      if (!myMimeType || !yourMimeType) {
          return false
      }

      List myTokens = myMimeType.tokenize("/")
      List yourTokens = yourMimeType.tokenize("/")

      // If we can't parse, we consider it a mismatch
      if (myTokens.size() != 2 || yourTokens.size() != 2) {
          return false
      }

      if (myTokens[0] == yourTokens[0] ||
          myTokens[0] == "*" ||
          yourTokens[0] == "*") {
          if (myTokens[1] == yourTokens[1] ||
              myTokens[1] == "*" ||
              yourTokens[1] == "*") {
              return true
          } else {
              return false
          }
      } else {
          return false
      }
    }

    static String cleanEtag(String str) {
        return stripQuotes(str).replaceAll('-gzip', '')
    }

    private static String stripQuotes(String str) {
        return str.replaceAll('"', '')
    }

    /**
     * Returns a sorted list of media types accepted by the client
     */
    static List<MediaType> parseAcceptHeader(String header) {
        BasicHeaderValueParser parser = new BasicHeaderValueParser()

        List<AcceptMediaType> mediaTypes = []

        try {
            for (HeaderElement h : BasicHeaderValueParser.parseElements(header, parser)) {
                mediaTypes << AcceptMediaType.fromHeaderElement(h)
            }
        }
        catch (Exception e) {
            throw new BadRequestException("Bad Accept header: " + header)
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

        static AcceptMediaType fromHeaderElement(HeaderElement e) {
            ListMultimap<String, String> parameters = new ArrayListMultimap<>()
            float q = 1.0
            for (NameValuePair p : e.getParameters()) {
                if ('q' == p.getName()) {
                    q = Float.parseFloat(p.getValue())
                    break // ignore 'Accept extension parameters'
                }
                parameters.put(p.getName(), p.getValue())
            }

            MediaType m = MediaType.parse(e.name).withParameters(parameters)
            return new AcceptMediaType(m, (float) q)
        }
    }
}
