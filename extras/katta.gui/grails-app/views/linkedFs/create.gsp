

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <meta name="layout" content="main" />
        <title>Create LinkedFs</title>         
    </head>
    <body>
        <div class="nav">
            <span class="menuButton"><g:link class="list" action="list">LinkedFs List</g:link></span>
        </div>
        <div class="body">
            <h1>Create LinkedFs</h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <g:hasErrors bean="${linkedFsInstance}">
            <div class="errors">
                <g:renderErrors bean="${linkedFsInstance}" as="list" />
            </div>
            </g:hasErrors>
            <g:form action="save" method="post" >
                <div class="dialog">
                    <table>
                        <tbody>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="name">Name:</label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean:linkedFsInstance,field:'name','errors')}">
                                    <input type="text" id="name" name="name" value="${fieldValue(bean:linkedFsInstance,field:'name')}"/>
                                </td>
                            </tr> 
                             <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="fileUri">File Uri:</label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean:linkedFsInstance,field:'fileUri','errors')}">
                                    <input type="text" id="fileUri" name="fileUri" value="${fieldValue(bean:linkedFsInstance,field:'fileUri')}"/>
                                </td>
                            </tr> 
                        
                        </tbody>
                    </table>
                </div>
                <div class="buttons">
                    <span class="button"><input class="save" type="submit" value="Create" /></span>
                </div>
            </g:form>
        </div>
    </body>
</html>
