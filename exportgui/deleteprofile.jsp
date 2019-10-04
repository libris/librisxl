<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<%
    java.util.Properties prop = new java.util.Properties();
    prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
    java.io.File file = new java.io.File(prop.getProperty("ProfileDir") + java.io.File.separatorChar + request.getParameter("name") + ".properties");
    file.delete();
    response.sendRedirect("/exportgui");
%>
<html>
    <head><title>JSP Page</title></head>
    <body>
    </body>
</html>
