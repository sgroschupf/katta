

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <meta name="layout" content="main" />
        <title>Dashboard</title>
         <g:javascript library="jquery-1.3.2.min" />
         <g:javascript library="jquery.sparkline.min" />
		<script type="text/javascript">

 $(function() {
 startAll();
 })

	function startAll(){
		<g:each in="${nodes}" status="i" var="node">
			<g:each in="${charts}" status="j" var="chart">
				requestValues('${node['id']}', '${chart}')
			</g:each>
		</g:each>
	}

	function requestValues(node, key) {
	
    var http_request = false;
	var url = '<g:createLink controller="dashboard" action="getValues"/>?serverId='+node+'&key='+key+'&'+new Date().getTime(); // timestamp prevents caching in IE
    if (window.XMLHttpRequest) { // Mozilla, Safari,...
        http_request = new XMLHttpRequest();
        if (http_request.overrideMimeType) {
            http_request.overrideMimeType('text/html');
        }
    } else if (window.ActiveXObject) { // IE
        try {
            http_request = new ActiveXObject("Msxml2.XMLHTTP");
        } catch (e) {
            try {
                http_request = new ActiveXObject("Microsoft.XMLHTTP");
            } catch (e) {}
        }
    }

    if (!http_request) {
        return false;
    }
    
    http_request.onreadystatechange = function(){
	    if (http_request.readyState == 4 && http_request.status == 200) {
        		var myValues = eval(http_request.responseText);
				$('.'+node+key).sparkline(myValues, {type: 'line', width: '100', lineColor:'#193441', fillColor:'#91aa9d'} );
				setTimeout(requestValues,1000, node, key);
        }
    };
    http_request.open('GET', url, true);
    http_request.send(null);

}


    </script>
    </head>
    <body>
        <div class="body">
         <div class="nav">
            	 <span class="menuButton"><g:link class="home" controller="home" action="index" id="${cluster}">Home</g:link></span>
        </div>
         <g:if test="${flash.message}">
         <div class="message">${flash.message}</div>
         </g:if>
         <!-- Master -->
            <h1>Master List</h1>
          
            <div class="list">
                <table>
                    <thead>
                        <tr>
                   	         <th class="sortable" >Name</th>
                   	        <th class="sortable" >Start Time</th>
                        </tr>
                    </thead>
                    <tbody>
                    <!-- this is not yet a list g:each in="${nodes}" status="i" var="node" -->
                        <tr class="odd'}">
                            <td>${master.masterName}</td>
                             <td>${master.startTimeAsString}</td>
                        </tr>
                    <!-- not a list /g:each-->
                    </tbody>
                </table>
            </div>
        
        
        <!-- nodes -->
            <h1>Nodes List</h1>
            <div class="list">
                <table>
                    <thead>
                        <tr>
                   	         <th class="sortable" >Name</th>
                   	         <th class="sortable" >State</th>
                   	         <th class="sortable" >Started</th>
                   	         <th class="sortable" >Assigned Shards</th>
							
							<g:each in="${charts}" status="j" var="chart">
	                   	         <th class="sortable" >${chart}</th>
							</g:each>

                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${nodes}" status="i" var="node">
                        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                            <td><g:link action="node" params="[cluster:'1', node:node['name']]">${node['name']}</g:link></td>
                            <td>${node['state']}</td>
                            <td>${node['started']}</td>
                            <td>${node['assigned']}</td>
                            
                            <g:each in="${charts}" status="j" var="chart">
                            	<td><span class="${node['id']}${chart}">Loading....</span></td>
							</g:each>
                        </tr>
                    </g:each>
                    </tbody>
                </table>
            </div>
            
      <!-- indexes -->
            <h1>Index List</h1>
            <div class="list">
                <table>
                    <thead>
                        <tr>
                   	         <th class="sortable" >Name</th>
                   	         <th class="sortable" >Path</th>
           	                 <th class="sortable" >State</th>
	   	                     <th class="sortable" >Replication Level</th>
                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${indexes}" status="i" var="index">
                        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                            <td>${index['name']}</td>
                            <td>${index['path']}</td>
                            <td>${index['state']}</td>
                            <td>${index['repLevel']}</td>
                        </tr>
                    </g:each>
                    </tbody>
                </table>
            </div>
            
        </div>
    </body>
</html>
