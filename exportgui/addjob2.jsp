<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
    <%
        java.util.Properties properties = new java.util.Properties();
        java.util.Properties prop = new java.util.Properties();
        prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
        java.io.File file = new java.io.File(
                prop.getProperty("HomeDir") + java.io.File.separatorChar + 
                "jobs" + java.io.File.separatorChar + 
                "incoming" + java.io.File.separatorChar + 
                ((int)(java.lang.Math.random() * 1000000)));

        for (String key: (java.util.Set<String>)request.getParameterMap().keySet()) {
            properties.setProperty(key, request.getParameter(key));
        }
        
        java.io.OutputStream os = new java.io.FileOutputStream(file);
        properties.store(os, null);
        os.close();
        
        response.sendRedirect("/exportgui/showprofile.jsp?operation=listjobs&name=" + request.getParameter("name"));
%>    