<html>
    <head>
        <title>Katta Overview</title>
		<meta name="layout" content="main" />
    </head>
    <body>
        <div class="nav">
            <span class="menuButton"></span>
        </div>
        <h1 style="margin-left:20px;">Welcome to Katta</h1>
        <p style="margin-left:20px;width:80%">
        	Below an overview of all registered Clusters.<br/>
        	You can <g:link controller="cluster" action="create">add a cluster</g:link> or simply click on one of the clusters to get to the Cluster dashboard.
        </p>
         
		<br/>
        <h2>Clusters</h2>
         <div class="list">
                <table>
                    <thead>
                        <tr>
                        <!-- TODO Frank, I dont want to use the tag g:sortableColumn, though I'm not sure what else to take so I copy pasted what the tag renders, however there might be a better way.. -->
                   	        <th class="sortable" >Name</th>
                   	        <th class="sortable" >Master uri</th>
                   	        <th class="sortable" ># Nodes</th>
                   	        <th class="sortable" ># Indexes</th>
                   	        <th class="sortable" >Action</th>
                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${clusters}" status="i" var="cluster">
                        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                           <td><g:link controller="dashboard" action="index" id="${cluster['id']}">${cluster['name']}</g:link></td>
                           <td>${cluster['uri']}</td>
                           <td>${cluster['nodeCount']}</td>
                           <td>${cluster['indexCount']}</td>
                           <td>
                             <g:form controller="cluster" >
			                    <input type="hidden" name="id" value="${cluster['id']}" />
			                    <span class="button"><g:actionSubmit class="edit" value="Edit" /></span>
			                    <span class="button"><g:actionSubmit class="delete" onclick="return confirm('Are you sure?');" value="Delete" /></span>
			                </g:form>
                           </td>
                        </tr>
                    </g:each>
                    </tbody>
                </table>
            </div>
            
            <br/><br/><br/>
        <!--
         <h2>File System</h2>
         <div class="list">
                <table>
                    <thead>
                        <tr>
                   	        <g:sortableColumn property="name" title="Name" />
                   	        <g:sortableColumn property="uri" title="Naster uri" />
                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${fileSystems}" status="i" var="linkedFsInstance">
                        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                          <td>${fieldValue(bean:linkedFsInstance, field:'name')}</td>
                          <td>${fieldValue(bean:linkedFsInstance, field:'fileUri')}</td>
                        </tr>
                    </g:each>
                    </tbody>
                </table>
            </div>
        -->
    </body>
</html>