<%@page contentType="text/plain"%>
<%@page pageEncoding="UTF-8"%>

<%
        java.util.Properties prop = new java.util.Properties();
        prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
        java.io.File file = new java.io.File(prop.getProperty("HomeDir") + "/files/" + request.getParameter("name") + "/logs/" + request.getParameter("filename"));
        java.io.FileInputStream is = new java.io.FileInputStream(file);
        java.io.OutputStream os = response.getOutputStream();
        
        int n=0;
        while ((n = is.read()) != -1) {
            os.write(n);
        }
        is.close();
%>