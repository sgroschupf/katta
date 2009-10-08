

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <meta name="layout" content="main" />
        <title>Cluster List</title>
    </head>
    <body>
        <div class="nav">
            <span class="menuButton"><g:link class="create" action="create">New Cluster</g:link></span>
        </div>
        <div class="body">
            <h1>Cluster List</h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <div class="list">
                <table>
                    <thead>
                        <tr>
                        
                   	        <g:sortableColumn property="id" title="Id" />
                        
                   	        <g:sortableColumn property="name" title="Name" />
                        
                   	        <g:sortableColumn property="zkUrl" title="Zk Url" />
                   	        
                   	        <g:sortableColumn property="rootNode" title="Root Node" />
                   	          
                   	        <g:sortableColumn property="rootNode" title="Data life time" />
                        
                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${clusterInstanceList}" status="i" var="clusterInstance">
                        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                        
                            <td><g:link action="show" id="${clusterInstance.id}">${fieldValue(bean:clusterInstance, field:'id')}</g:link></td>
                        
                            <td>${fieldValue(bean:clusterInstance, field:'name')}</td>
                        
                            <td>${fieldValue(bean:clusterInstance, field:'zkUrl')}</td>
                            
                            <td>${fieldValue(bean:clusterInstance, field:'rootNode')}</td>
                            
                              <td>${fieldValue(bean:clusterInstance, field:'dataLifeTime')}</td>
                        
                        </tr>
                    </g:each>
                    </tbody>
                </table>
            </div>
            <div class="paginateButtons">
                <g:paginate total="${clusterInstanceTotal}" />
            </div>
        </div>
    </body>
</html>
