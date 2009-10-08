

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <meta name="layout" content="main" />
        <title>LinkedFs List</title>
    </head>
    <body>
        <div class="nav">
            <span class="menuButton"><g:link class="create" action="create">New LinkedFs</g:link></span>
        </div>
        <div class="body">
            <h1>LinkedFs List</h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <div class="list">
                <table>
                    <thead>
                        <tr>
                        
                   	        <g:sortableColumn property="id" title="Id" />
                        
                   	        <g:sortableColumn property="fileUri" title="File Uri" />
                        
                   	        <g:sortableColumn property="name" title="Name" />
                        
                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${linkedFsInstanceList}" status="i" var="linkedFsInstance">
                        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                        
                            <td><g:link action="show" id="${linkedFsInstance.id}">${fieldValue(bean:linkedFsInstance, field:'id')}</g:link></td>
                        
                            <td>${fieldValue(bean:linkedFsInstance, field:'fileUri')}</td>
                        
                            <td>${fieldValue(bean:linkedFsInstance, field:'name')}</td>
                        
                        </tr>
                    </g:each>
                    </tbody>
                </table>
            </div>
            <div class="paginateButtons">
                <g:paginate total="${linkedFsInstanceTotal}" />
            </div>
        </div>
    </body>
</html>
