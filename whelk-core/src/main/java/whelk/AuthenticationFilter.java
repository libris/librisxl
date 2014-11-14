package whelk;

import org.apache.commons.codec.binary.Base64;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.json.simple.JSONObject;

import javax.crypto.*;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Key;
import java.util.*;

/**
 * Created by Markus Holm on 2014-10-28.
 */
public class AuthenticationFilter implements Filter {

    private ObjectMapper mapper = null;
    private List<String> supportedMethods;
    private String encryptionKey = null;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

        String initParams = filterConfig.getInitParameter("supportedMethods");
        supportedMethods = splitSupportedMethod(initParams);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        if (isApiCall(httpRequest) && supportedMethods != null && supportedMethods.contains(httpRequest.getMethod())) {
            try {
                String toBeEncrtypted = httpRequest.getHeader("xlkey");
                String json = decrypt(toBeEncrtypted);

                if (mapper == null) {
                    mapper = new ObjectMapper();
                }

                HashMap result = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                if (!isExpired(Long.parseLong(result.get("exp").toString()))) {
                    request.setAttribute("user", result.get("user"));
                    chain.doFilter(request, response);
                }else {
                    httpResponse.sendError(httpResponse.SC_UNAUTHORIZED);
                }
            } catch (Exception e) {
                httpResponse.sendError(httpResponse.SC_INTERNAL_SERVER_ERROR);
                e.printStackTrace();
            }
        }else {
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {

    }

    private boolean isApiCall(HttpServletRequest httpRequest) {
        return httpRequest.getServerPort() == 80 ? true : false;
    }

    private JSONObject getUserInfo(JSONObject obj) {

        if (obj != null) {
            return (JSONObject)obj.get("user");
        }
        return null;
    }

    private boolean isExpired(long unixtime) {
        Date now = new Date();
        Date expires = new Date(unixtime);
        return now.compareTo(expires) > 0;
    }

    /**
     *
     * @param supportedMethods
     * @return a list with all supported methods
     */
    private List<String> splitSupportedMethod(String supportedMethods) {

        if (supportedMethods != null) {
            return Arrays.asList(supportedMethods.replace(" ", "").split(","));
        }
        return null;
    }

    private String getEncryptionKey() {
        if (encryptionKey == null) {
            Properties properties = new Properties();
            try {
                properties.load(this.getClass().getClassLoader().getResourceAsStream("api.properties"));
            } catch (IOException ioe) {
                throw new RuntimeException("Failed to load api properties.", ioe);
            }
            encryptionKey = properties.getProperty("encryptionkey");
        }
        //System.out.println("Encryptionkey: " + encryptionKey);
        return encryptionKey;
    }

    private String decrypt(String encrypted) throws Exception{
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS7Padding");
        Key aesKey = new SecretKeySpec(getEncryptionKey().getBytes("UTF-8"), "AES");
        cipher.init(Cipher.DECRYPT_MODE, aesKey);
        byte[] decodeValue = Base64.decodeBase64(encrypted);
        byte[] decryptValue = cipher.doFinal(decodeValue);
        return new String(decryptValue, "UTF-8");
    }
}
