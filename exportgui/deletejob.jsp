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
                request.getParameter("jobid"));

        if (file.exists()) {
            file.delete();
        }
        
        response.sendRedirect("/exportgui/showprofile.jsp?operation=listjobs&name=" + request.getParameter("name"));
%>    