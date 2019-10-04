<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<% 
    
    java.util.Enumeration<String> en = request.getSession().getAttributeNames();
    while (en.hasMoreElements()) {
        request.getSession().removeAttribute(en.nextElement());
    }
    
    response.sendRedirect("index.jsp");
%>
