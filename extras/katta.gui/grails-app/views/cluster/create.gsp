

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <meta name="layout" content="main" />
        <title>Create Cluster</title>         
    </head>
    <body>
        <div class="nav">
                <span class="menuButton"><g:link class="list" controller="home" action="index">Cluster List</g:link></span>
        </div>
        <div class="body">
            <h1>Create Cluster</h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <g:hasErrors bean="${clusterInstance}">
            <div class="errors">
                <g:renderErrors bean="${clusterInstance}" as="list" />
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
                                <td valign="top" class="value ${hasErrors(bean:clusterInstance,field:'name','errors')}">
                                    <input type="text" id="name" name="name" value="${fieldValue(bean:clusterInstance,field:'name')}localhost"/>
                                </td>
                            </tr> 
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                    <label for="zkUrl">Zk Url:</label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean:clusterInstance,field:'zkUrl','errors')}">
                                    <input type="text" id="zkUrl" name="zkUrl" value="${fieldValue(bean:clusterInstance,field:'zkUrl')}localhost:2181"/>
                                </td>
                            </tr>
                             <tr class="prop">
                                <td valign="top" class="rootNode">
                                    <label for="zkUrl">Root Note:</label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean:clusterInstance,field:'rootNode','errors')}">
                                    <input type="text" id="rootNode" name="rootNode" value="${fieldValue(bean:clusterInstance,field:'rootNode')}/katta"/>
                                </td>
                            </tr> 
                             <tr class="prop">
                                <td valign="top" class="rootNode">
                                    <label for="zkUrl">Data life time:</label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean:clusterInstance,field:'dataLifeTime','errors')}">
                                    <input type="text" id="dataLifeTime" name="dataLifeTime" value="${fieldValue(bean:clusterInstance,field:'dataLifeTime')}1"/>
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
