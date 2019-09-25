<%@page contentType="text/html"%>
<%@page pageEncoding="UTF-8"%>
<table width="100%">
    <tr height="300">
        <td height="100%" align="center" valign="middle">
            <form method="GET" action="changepassword.jsp">
                <input type="hidden" name="name" value="${param.name}">
            <table>
                <tr>
                    <td class="default">nytt lösenord:</td>
                    <td><input name="password"/></td>
                </tr>
                <tr>
                    <td></td>
                    <td><input type="submit" value="ändra lösenord"/></td>
                </tr>
            </table>
            </form>
        </td>
    </tr>
</table>

