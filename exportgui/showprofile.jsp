<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<html>
<%@include file="logincheck.jsp"%>
    <head>
        <style type="text/css">
            .default     { font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 80% }
            .menu     { font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 80% }
            a { color: #0000ff }
            a:active { color: #0000ff }
            a:hover { color: #0000ff }
            a:visited { color: #0000ff }
        </style>
    </head>
    <body>
        <table width="800" cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td><img src="images/batchlogo.gif"></td>
                <td background="images/header2.gif" width="100%" align="right" valign="bottom"></td>
                <td><img src="images/header3.gif"></td>
            </tr>
        </table>
        <table width="800" cellpadding="0" cellspacing="0">
            <tr>
                <td width="1" bgcolor="#7f7f7f"><img src="images/1trans.gif" width="1" height="1"></td>
                <td nowrap="nowrap" class="menu" valign="top">
                    <%@include file="menu.jsp"%>
                </td>
                <td width="1" bgcolor="#7f7f7f"><img src="images/1trans.gif" width="1" height="1"></td>
                <td width="100%" valign="top">
                    <c:choose>
                        <c:when test="${param.operation == 'editprofile'}">
                            <%@include file="editprofile.jsp"%>
                        </c:when>
                        <c:when test="${param.operation == 'listfiles'}">
                            <%@include file="listfiles.jsp"%>
                        </c:when>
                        <c:when test="${param.operation == 'changepassword'}">
                            <%@include file="password.jsp"%>
                        </c:when>
                        <c:when test="${param.operation == 'editsab'}">
                            <%@include file="editsab.jsp"%>
                        </c:when>
                        <c:when test="${param.operation == 'createprofile'}">
                            <%@include file="createprofile.jsp"%>
                        </c:when>
                        <%-- <c:when test="${param.operation == 'testprofile'}">
                            <%@include file="testprofile.jsp"%>
                        </c:when> --%>
                        <c:when test="${param.operation == 'chooseprofile'}">
                            <%@include file="chooseprofile.jsp"%>
                        </c:when>
                        <%-- <c:when test="${param.operation == 'getrecord'}">
                            <%@include file="getrecord.jsp"%>
                        </c:when> --%>
                        <c:when test="${param.operation == 'addjob'}">
                            <%@include file="addjob.jsp"%>
                        </c:when>
                        <c:when test="${param.operation == 'fulldump'}">
                            <%@include file="fulldump.jsp"%>
                        </c:when>
                        <c:when test="${param.operation == 'listjobs'}">
                            <%@include file="listjobs.jsp"%>
                        </c:when>
                        <c:otherwise>
                        </c:otherwise>
                    </c:choose>                                
                </td>
                <td width="1" bgcolor="#7f7f7f"><img src="images/1trans.gif" width="1" height="1"></td>
            </tr>
            <tr>
                <td colspan="5" width="1" height="1" bgcolor="#7f7f7f"></td>
            </tr>
            <tr>
                <td width="1" bgcolor="#7f7f7f"><img src="images/1trans.gif" width="1" height="1"></td>
                <td width="1" colspan="3" bgcolor="#dfdfdf" class="default" nowrap="nowrap">&nbsp;</td>
                <td width="1" bgcolor="#7f7f7f"><img src="images/1trans.gif" width="1" height="1"></td>
            </tr>
            <tr>
                <td colspan="5" width="1" height="1" bgcolor="#7f7f7f"></td>
            </tr>
        </table>
    </body>
</html>
