<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<% 
        /* if (request.getSession().getAttribute("group") == null || (!request.getSession().getAttribute("group").equals(request.getParameter("name")) && (request.getRemoteAddr().equals("127.0.0.1") || request.getRemoteAddr().startsWith("193.10.75") || request.getRemoteAddr().startsWith("192.168.3") || request.getRemoteAddr().equals("193.10.249.131") || request.getRemoteAddr().startsWith("10.50.64") || request.getRemoteAddr().startsWith("10.50.66") || request.getRemoteAddr().startsWith("10.50.67") || request.getRemoteAddr().startsWith("10.50.71") || request.getRemoteAddr().startsWith("10.50.79")))) { */
        if (request.getSession().getAttribute("group") == null || (!request.getSession().getAttribute("group").equals(request.getParameter("name")) || !request.getSession().getAttribute("admin").equals("admin") )) {

            //response.sendRedirect("error.jsp?type=whatareyoutryingtopull");
            response.sendRedirect("logout.jsp");
            return;
        }
%>
