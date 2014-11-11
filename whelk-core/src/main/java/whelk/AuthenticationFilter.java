package whelk;

import org.apache.commons.codec.binary.Base64;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Key;
import java.util.*;

/**
 * Created by Markus Holm on 2014-10-28.
 */
public class AuthenticationFilter implements Filter {

    private List<String> supportedMethods;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

        String initParams = filterConfig.getInitParameter("supportedMethods");
        supportedMethods = splitSupportedMethod(initParams);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;

        String path = httpRequest.getRequestURI();
        if (exclude(path)) {
            chain.doFilter(request, response);
        }

        if (supportedMethods != null && supportedMethods.contains(httpRequest.getMethod())) {
            try {
                String user = "";
                String toBeEncrtypted = httpRequest.getHeader("xlkey");
                JSONObject result = decrypt(getEncryptionKey(), toBeEncrtypted);
                JSONObject userInfo = getUserInfo(result);

                if (!isExpired(Long.parseLong(result.get("exp").toString()))) {
                System.out.println("############ NOT EXPIRED");
                    if (user.equalsIgnoreCase(userInfo.get("email").toString())) {
                        System.out.println("################# SAME USER PROCEED!!!");
                            // Populate request with stuff backend needs.
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }else {
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {

    }

    private String getEncryptionKey() {
        return "XXXXXXXXXXXXXXXX";
    }

    private boolean exclude(String path) {
        if (path.endsWith("_operations")) {
            return true;
        }

        return false;
    }

    private JSONObject getUserInfo(JSONObject obj) {

        if (obj != null) {
            return (JSONObject)obj.get("user");
        }
        return null;
    }

    private boolean isExpired(long unixtime) {
        Date now = new Date();
        Date expires = new Date(unixtime * 1000);
        return now.compareTo(expires) != 0;
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

    private JSONObject decrypt(String key, String encrypted) throws Exception{
        Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");
        Key aesKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
        cipher.init(Cipher.DECRYPT_MODE, aesKey); //, new IvParameterSpec(new byte[16]));
        byte[] decodeValue = Base64.decodeBase64(encrypted);
        byte[] decryptValue = cipher.doFinal(decodeValue);
        String jsonString = new String(decryptValue, "UTF-8").replaceAll("%+$", "");
        System.out.println("Decrypted value: " + jsonString);
        JSONParser parser = new JSONParser();
        return (JSONObject)parser.parse(jsonString);
    }

    private String encrypt(String key, String text) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CFB/NoPadding");
        Key aesKey = new SecretKeySpec(key.getBytes(), "AES");

        cipher.init(Cipher.ENCRYPT_MODE, aesKey, new IvParameterSpec(new byte[16]));
        byte[] encryptValue = cipher.doFinal(text.getBytes());
        byte[] encodedValue = Base64.encodeBase64(encryptValue);

        return new String(encodedValue);
    }
}
