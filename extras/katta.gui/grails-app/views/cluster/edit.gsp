

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <meta name="layout" content="main" />
        <title>Edit Cluster</title>
    </head>
    <body>
        <div class="nav">
           <span class="menuButton"><g:link class="list" controller="home" action="index">Cluster List</g:link></span>
            <span class="menuButton"><g:link class="create" action="create">New Cluster</g:link></span>
        </div>
        <div class="body">
            <h1>Edit Cluster</h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <g:hasErrors bean="${clusterInstance}">
            <div class="errors">
                <g:renderErrors bean="${clusterInstance}" as="list" />
            </div>
            </g:hasErrors>
            <g:form method="post" >
                <input type="hidden" name="id" value="${clusterInstance?.id}" />
                <input type="hidden" name="version" value="${clusterInstance?.version}" />
                <div class="dialog">
                    <table>
                        <tbody>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="name">Name:</label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean:clusterInstance,field:'name','errors')}">
                                    <input type="text" id="name" name="name" value="${fieldValue(bean:clusterInstance,field:'name')}"/>
                                </td>
                            </tr> 
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="zkUrl">Zk Url:</label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean:clusterInstance,field:'zkUrl','errors')}">
                                    <input type="text" id="zkUrl" name="zkUrl" value="${fieldValue(bean:clusterInstance,field:'zkUrl')}"/>
                                </td>
                            </tr> 
                            
                             <tr class="prop">
                                <td valign="top" class="rootNode">
                                    <label for="zkUrl">Root Node:</label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean:clusterInstance,field:'rootNode','errors')}">
                                    <input type="text" id="rootNode" name="rootNode" value="${fieldValue(bean:clusterInstance,field:'rootNode')}"/>
                                </td>
                            </tr> 
                       		 <tr class="prop">
                                <td valign="top" class="rootNode">
                                    <label for="zkUrl">Data life time:</label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean:clusterInstance,field:'dataLifeTime','errors')}">
                                    <input type="text" id="rootNode" name="rootNode" value="${fieldValue(bean:clusterInstance,field:'dataLifeTime')}1"/>
                                </td>
                            </tr> 
                        </tbody>
                    </table>
                </div>
                <div class="buttons">
                    <span class="button"><g:actionSubmit class="save" value="Update" /></span>
                    <span class="button"><g:actionSubmit class="delete" onclick="return confirm('Are you sure?');" value="Delete" /></span>
                </div>
            </g:form>
        </div>
    </body>
</html>
