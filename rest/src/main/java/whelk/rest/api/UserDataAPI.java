package whelk.rest.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;
import whelk.util.http.HttpTools;
import whelk.util.http.WhelkHttpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class UserDataAPI extends WhelkHttpServlet {
    private static final Logger log = LogManager.getLogger(UserDataAPI.class);
    private static final int POST_MAX_SIZE = 1000000;
    static final String ID_HASH_FUNCTION = "SHA-256";
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public UserDataAPI() {
    }
    
    public UserDataAPI(Whelk whelk) {
        this.whelk = whelk;
    }
    
    @Override
    public void init(Whelk whelk) {
        log.info("Starting User Data API");
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        log.debug("Handling GET request for {}", request.getPathInfo());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> userInfo = (Map<String, Object>) request.getAttribute("user");
        if (!isValidUserWithPermission(request, response, userInfo)) {
            return;
        }
        
        String id = digest(String.valueOf(userInfo.get("id")), ID_HASH_FUNCTION);
        String data = whelk.getUserData(id);
        if (data == null) {
            data = upgradeOldEmailBasedEntry(userInfo);
            if (data == null) {
                data = "{}";
            }
        }
        
        String eTag = CrudUtils.ETag.plain(digest(data, "MD5")).toString();
        response.setHeader("ETag", eTag);
        
        Optional<CrudUtils.ETag> ifNoneMatch = CrudUtils.getIfNoneMatch(request);
        if (ifNoneMatch.isPresent() && CrudUtils.ETag.plain(digest(data, "MD5")).isNotModified(ifNoneMatch.get())) {
            HttpTools.sendResponse(response, "", "application/json", HttpServletResponse.SC_NOT_MODIFIED);
        } else {
            HttpTools.sendResponse(response, data, "application/json");
        }
    }
    
    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws IOException {
        log.info("Handling PUT request for {}", request.getPathInfo());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> userInfo = (Map<String, Object>) request.getAttribute("user");
        if (!isValidUserWithPermission(request, response, userInfo)) {
            return;
        }
        
        String id = digest(String.valueOf(userInfo.get("id")), ID_HASH_FUNCTION);
        String data = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
        String idAndEmail = id + " (" + userInfo.get("id") + " " + userInfo.get("email") + ")";
        
        // Arbitrary upper limit to prevent Clearly Too Large things from being saved.
        // Can't rely on request.getContentLength() because that's sent by the client.
        if (data.length() > POST_MAX_SIZE) {
            log.warn("{} sent too much data (length {}, max {})", idAndEmail, data.length(), POST_MAX_SIZE);
            response.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE, 
                    "Too much data (length " + data.length() + ", max " + POST_MAX_SIZE + ")");
            return;
        }
        
        // Make sure what we're saving is actually valid JSON
        try {
            if (data.trim().isEmpty()) {
                throw new IllegalArgumentException("Empty JSON");
            }
            objectMapper.readTree(data);
        } catch (Exception e) {
            log.warn("{} sent invalid JSON", idAndEmail);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid JSON");
            return;
        }
        
        if (whelk.storeUserData(id, data)) {
            log.info("{} saved", idAndEmail);
            response.setStatus(HttpServletResponse.SC_OK);
        } else {
            log.warn("{} could not be saved to database", idAndEmail);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 
                    "Saving " + idAndEmail + " to database failed");
        }
    }
    
    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws IOException {
        log.info("Handling DELETE request for {}", request.getPathInfo());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> userInfo = (Map<String, Object>) request.getAttribute("user");
        if (!isValidUserWithPermission(request, response, userInfo)) {
            return;
        }
        
        whelk.removeUserData(digest(String.valueOf(userInfo.get("id")), ID_HASH_FUNCTION));
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }
    
    private String upgradeOldEmailBasedEntry(Map<String, Object> userInfo) {
        Object emailObj = userInfo.get("email");
        if (emailObj == null) {
            return null;
        }
        
        String email = digest(String.valueOf(emailObj), ID_HASH_FUNCTION);
        String data = whelk.getUserData(email);
        if (data != null) {
            String id = digest(String.valueOf(userInfo.get("id")), ID_HASH_FUNCTION);
            String idAndEmail = id + " (" + userInfo.get("id") + " " + userInfo.get("email") + ")";
            if (whelk.storeUserData(id, data)) {
                whelk.removeUserData(email);
                log.info("{} moved from email to id", idAndEmail);
            } else {
                log.warn("{} could not be saved to database", idAndEmail);
            }
        }
        return data;
    }
    
    private static boolean isValidUserWithPermission(HttpServletRequest request, HttpServletResponse response, 
            Map<String, Object> userInfo) throws IOException {
        if (userInfo == null) {
            log.info("User authentication failed");
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "User authentication failed");
            return false;
        }
        
        if (!userInfo.containsKey("id")) {
            log.info("User check failed: 'id' missing in user");
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Key 'id' missing in user info");
            return false;
        }
        
        String id = digest(String.valueOf(userInfo.get("id")), ID_HASH_FUNCTION);
        if (!getRequestId(request).equals(id)) {
            log.info("ID in request doesn't match ID from token");
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "ID in request doesn't match ID from token");
            return false;
        }
        
        return true;
    }
    
    private static String getRequestId(HttpServletRequest request) {
        String pathInfo = request.getPathInfo();
        if (pathInfo == null) {
            return "";
        }
        int slashIndex = pathInfo.indexOf('/');
        return slashIndex == -1 ? pathInfo : pathInfo.substring(slashIndex + 1);
    }
    
    private static String digest(String input, String algorithm) {
        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Hash algorithm not available: " + algorithm, e);
        }
    }
}