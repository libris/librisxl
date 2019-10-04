<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<html>
<%
    java.io.File file =  new java.io.File(application.getRealPath("/logs/" + request.getParameter("name") + "/" + request.getParameter("logname")));
%>
    <head>
        <title>visa logg</title>
        <style type="text/css">
            .default { font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 80%; }
            .rubrik { font-family: Verdana, Arial, Helvetica, sans-serif; font-size: 120%; }
        </style>
    </head>
    <body>
        <table cellpadding="0" cellspacing="0" width="700">
            <tr>
                <td><img src="/exportgui/images/logga.jpg"></td>
            </tr>
            <tr height="1" bgcolor="#dfdfdf">
                <td></td>
            </tr>
        </table>
        <p class="rubrik">exportadministration (<% out.print(request.getParameter("name")); %>)</p>    
        <table cellpadding="0" cellspacing="0" width="700">
            <tr bgcolor="#000000">
                <td colspan="6" height="1"></td>
            </tr>
            <tr bgcolor="#dfdfdf" class="default">
                <td nowrap align="center">&nbsp;<b>BIBID</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>ISBN/ISSN</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>författare</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>titel</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>operatör</b>&nbsp;</td>
                <td nowrap align="center">&nbsp;<b>orsak</b>&nbsp;</td>
            </tr>
            <tr bgcolor="#000000">
                <td colspan="6" height="1"></td>
            </tr>
            <tr bgcolor="#dfdfdf">
                <td colspan="6" height="1"></td>
            </tr>
<%
            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(file));
            reader.readLine();
            String line = null;
            
            while ((line = reader.readLine()) != null) {
                if (line.trim().equals("")) continue;
                String s[] = line.split("\t");
%>
            <tr class="default">
                <td nowrap align="center">&nbsp;<% out.print(s[0]); %>&nbsp;</td>
                <td nowrap align="center">&nbsp;<% out.print(s[1]); %>&nbsp;</td>
                <td nowrap align="center">&nbsp;<% out.print(s[2]); %>&nbsp;</td>
                <td nowrap align="center">&nbsp;<% out.print(s[3]); %>&nbsp;</td>
                <td nowrap align="center">&nbsp;<% out.print(s[4]); %>&nbsp;</td>
                <td nowrap align="center">&nbsp;<% out.print(s[5]); %>&nbsp;</td>
            </tr>
            <tr bgcolor="#dfdfdf">
                <td colspan="6" height="1"></td>
            </tr>
<%
            }
            
            reader.close();
%>

            <tr>
                <td>
        </table>
  </body>
</html>
