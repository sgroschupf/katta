

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <meta name="layout" content="main" />
        <title>Show Cluster</title>
    </head>
    <body>
        <div class="nav">
            <span class="menuButton"><g:link class="list" controller="home" action="index">Cluster List</g:link></span>
            <span class="menuButton"><g:link class="create" action="create">New Cluster</g:link></span>
        </div>
        <div class="body">
            <h1>Show Cluster</h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <div class="dialog">
                <table>
                    <tbody>

                    
                        <tr class="prop">
                            <td valign="top" class="name">Id:</td>
                            
                            <td valign="top" class="value">${fieldValue(bean:clusterInstance, field:'id')}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name">Name:</td>
                            
                            <td valign="top" class="value">${fieldValue(bean:clusterInstance, field:'name')}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name">Zk Url:</td>
                            
                            <td valign="top" class="value">${fieldValue(bean:clusterInstance, field:'zkUrl')}</td>
                            
                        </tr>
                        
                         <tr class="prop">
                            <td valign="top" class="name">Root Node:</td>
                            
                            <td valign="top" class="value">${fieldValue(bean:clusterInstance, field:'rootNode')}</td>
                            
                        </tr>
                        
                          <tr class="prop">
                            <td valign="top" class="name">Data life time:</td>
                            
                            <td valign="top" class="value">${fieldValue(bean:clusterInstance, field:'dataLifeTime')}</td>
                            
                        </tr>
                    
                    </tbody>
                </table>
            </div>
            <div class="buttons">
                <g:form>
                    <input type="hidden" name="id" value="${clusterInstance?.id}" />
                    <span class="button"><g:actionSubmit class="edit" value="Edit" /></span>
                    <span class="button"><g:actionSubmit class="delete" onclick="return confirm('Are you sure?');" value="Delete" /></span>
                </g:form>
            </div>
        </div>
    </body>
</html>
