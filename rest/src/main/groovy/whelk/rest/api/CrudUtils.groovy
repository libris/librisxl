package whelk.rest.api

import groovy.util.logging.Slf4j as Log
import javax.servlet.http.HttpServletRequest

import whelk.rest.api.MimeTypes


@Log
class CrudUtils {
    static final List ALLOWED_MIME_TYPES = [ MimeTypes.JSONLD,
                                             MimeTypes.TURTLE,
                                             MimeTypes.RDF,
                                             MimeTypes.JSON
    ]

    static String getBestContentType(HttpServletRequest request) {
        String suffix
        String acceptHeader = request.getHeader("Accept")
        List tokens = request.getRequestURI().tokenize(".")
        if (tokens.size >= 2) {
            suffix = tokens[-1]
        }
        return getBestContentType(acceptHeader, suffix)
    }

    static String getBestContentType(String acceptHeader, String suffix) {
        // FIXME use suffix
        List acceptedMimeTypes = getMimeTypes(acceptHeader)
        List mimeTypes = sortMimeTypesByQuality(acceptedMimeTypes)
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
            default:
                break
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

    static List sortMimeTypesByQuality(List mimeTypes) {
        Map result = [:]
        for (String mimeType : mimeTypes) {
            List tokens = mimeType.tokenize(";")
            if (tokens.size() == 1) {
                result[tokens[0]] = 1.0
            } else if (tokens.size() == 2) {
                Float quality = tokens[1].split("=")[1] as Float
                result[tokens[0]] = quality
            }
        }
        String[] sorted = result.keySet() as String[]
        return sorted.sort { a, b ->
            result[b] <=> result[a] ?: a <=> b
        }
    }

    static List getMimeTypes(String acceptHeader) {
        List result = []
        for (String token : acceptHeader.tokenize(",")) {
            result.add(token.trim())
        }
        return result
    }
}
