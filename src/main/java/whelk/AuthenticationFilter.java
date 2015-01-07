package whelk;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.json.simple.JSONObject;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class AuthenticationFilter implements Filter {

    private ObjectMapper mapper = null;
    private List<String> supportedMethods;
    private String url = null;

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
                String token = httpRequest.getHeader("Authorization");
                String json = verifyToken(token.replace("Bearer ", ""));
                if (json == null || json.isEmpty()) {
                    httpResponse.sendError(httpResponse.SC_INTERNAL_SERVER_ERROR, "Content is null");
                }

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
            return result.toString();

        }catch (Exception e) {
            e.printStackTrace();
        }

        return null;

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

    private String getVerifyUrl() {
        if (url == null) {
            Properties properties = new Properties();
            try {
                properties.load(this.getClass().getClassLoader().getResourceAsStream("whelk.properties"));
            } catch (IOException ioe) {
                throw new RuntimeException("Failed to load api properties.", ioe);
            }
            url = properties.getProperty("verifyurl");
        }
        return url;
    }
}
