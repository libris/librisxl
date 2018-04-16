package whelk;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.util.PropertyLoader;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;

public class AuthenticationFilter implements Filter {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String XL_ACTIVE_SIGEL_HEADER = "XL-Active-Sigel";
    private List<String> supportedMethods;
    private boolean mockAuthMode = false;
    private String url = null;

    final static Logger log = LogManager.getLogger(AuthenticationFilter.class);

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

        String initParams = filterConfig.getInitParameter("supportedMethods");
        mockAuthMode = filterConfig.getInitParameter("mockAuthentication").equals("true");
        if (mockAuthMode) {
            log.warn("Starting authentication filter in development mode. System is wide open.");
        }
        log.debug("Mock auth mode: " + mockAuthMode);
        supportedMethods = splitInitParameters(initParams);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        if (!mockAuthMode && supportedMethods != null && supportedMethods.contains(httpRequest.getMethod())) {
            int response_code = 0;
            String json = null;
            try {
                String token = httpRequest.getHeader("Authorization");
                if (token == null) {
                    httpResponse.sendError(httpResponse.SC_UNAUTHORIZED, "Invalid accesstoken, Token is: "+token);
                    response_code = httpResponse.SC_UNAUTHORIZED;
                    return;
                }
                log.debug("Verifying token " + token);
                json = verifyToken(token.replace("Bearer ", ""));
                if (json == null || json.isEmpty()) {
                    httpResponse.sendError(httpResponse.SC_UNAUTHORIZED, "Access token has expired");
                    response_code = httpResponse.SC_UNAUTHORIZED;
                    return;
                }

                HashMap result = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});

                Object message = result.get("message");
                if (message != null && message.toString().equals("Bearer token is expired.")) {
                    httpResponse.sendError(httpResponse.SC_UNAUTHORIZED, "Access token has expired");
                    response_code = httpResponse.SC_UNAUTHORIZED;
                    return;
                }
                if (message != null && message.toString().equals("Bearer token not found.")) {
                    httpResponse.sendError(httpResponse.SC_UNAUTHORIZED, "Missing access token.");
                    response_code = httpResponse.SC_UNAUTHORIZED;
                    return;
                }

                if (!isExpired(result.get("expires_at").toString())) {
                    HashMap user = (HashMap) result.get("user");
                    String activeSigel = httpRequest.getHeader(XL_ACTIVE_SIGEL_HEADER);
                    if (activeSigel != null) {
                        user.put("active_sigel", activeSigel);
                    }
                    request.setAttribute("user", user);
                    chain.doFilter(request, response);
                }else {
                    httpResponse.sendError(httpResponse.SC_UNAUTHORIZED);
                    response_code = httpResponse.SC_UNAUTHORIZED;
                }
            } catch (org.codehaus.jackson.JsonParseException jpe) {
                log.error("JsonParseException. Failed to parse:" + json, jpe);
                httpResponse.sendError(httpResponse.SC_INTERNAL_SERVER_ERROR);
                response_code = httpResponse.SC_INTERNAL_SERVER_ERROR;
            } catch (Exception e) {
                log.error("Exception: " + e);
                httpResponse.sendError(httpResponse.SC_INTERNAL_SERVER_ERROR);
                response_code = httpResponse.SC_INTERNAL_SERVER_ERROR;
                e.printStackTrace();
            } finally {
                if (response_code != 0) {
                    String method = httpRequest.getMethod();
                    String endpoint = httpRequest.getRequestURI();
                    log.info("Request " + method + " " + endpoint + " blocked: " + response_code);
                }
            }
        } else {
            if (mockAuthMode) {
                log.warn("Mock authentication mode enabled, creating dummy user.");
                request.setAttribute("user", createDevelopmentUser());
            }
            chain.doFilter(request, response);
        }
    }

    private Map createDevelopmentUser() {
        Map emptyUser = new HashMap<String,Object>();
        emptyUser.put("user", "SYSTEM");
        return emptyUser;
    }

    private String verifyToken(String token) {

        try {

            HttpClient client = HttpClientBuilder.create().build();
            HttpGet get = new HttpGet(getVerifyUrl());

            get.setHeader("Authorization", "Bearer " + token);
            HttpResponse response = client.execute(get);

            BufferedReader rd = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent()));

            StringBuffer result = new StringBuffer();
            String line = "";
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }

            rd.close();
            return result.toString();

        }catch (Exception e) {
            e.printStackTrace();
        }

        return null;

    }

    @Override
    public void destroy() {

    }

    /*
    private JSONObject getUserInfo(JSONObject obj) {

        if (obj != null) {
            return (JSONObject)obj.get("user");
        }
        return null;
    }
    */

    private boolean isExpired(String expires_at) {
        try {
            Instant exp = Instant.parse(expires_at);
            Instant now = Instant.now();
            log.debug("expires_at: " + exp + ", now: " + now);
            return now.compareTo(exp) > 0;
        } catch(DateTimeParseException e) {
            log.warn("Failed to parse token expiration: " + e);
            return true;
        }
    }

    /**
     *
     * @param initParams
     * @return a list with all supported methods
     */
    private List<String> splitInitParameters(String initParams) {

        if (initParams != null) {
            return Arrays.asList(initParams.replace(" ", "").split(","));
        }
        return null;
    }

    private String getVerifyUrl() {
        if (url == null) {
            Properties secrets = null;
            try {
                secrets = PropertyLoader.loadProperties("secret");
            } catch (Exception e) {
                throw new RuntimeException("Failed to load api properties.", e);
            }
            url = (String)secrets.getProperty("oauth2verifyurl");
        }
        return url;
    }
}
