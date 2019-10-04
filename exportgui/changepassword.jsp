<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<%@include file="logincheck.jsp" %>

<%
        java.util.Properties properties = new java.util.Properties();
        java.util.Properties prop = new java.util.Properties();
        prop.load(new java.io.FileInputStream(new java.io.File(application.getRealPath("/exportgui.properties"))));
        java.io.File file = new java.io.File(prop.getProperty("ProfileDir") + java.io.File.separatorChar + request.getParameter("name") + ".properties");
        properties.load(new java.io.FileInputStream(file));
        properties.setProperty("password", request.getParameter("password"));
        java.io.OutputStream os = new java.io.FileOutputStream(file);
        properties.store(os, null);
        os.close();
                
        response.sendRedirect("showprofile.jsp?operation=listfiles&name=" + request.getParameter("name"));
%>