<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
        <meta name="layout" content="main" />
        <title>Node Overview</title>
 		<g:javascript library="jquery-1.3.2.min" />
        <g:javascript library="jquery.flot" />
    </head>
    <body>
   <script id="source" language="javascript" type="text/javascript">
    $(function() {
 		startAll();
 	})
   
   function startAll(){
		<g:each in="${charts}" status="i" var="chart">
			renderchart('${chart.key}')
		</g:each>
	}
   
	function renderchart(chartName) 
	{ 
		var url = '<g:createLink absolute="true" controller="dashboard" action="getNodeData"/>?nodeName=${node}&chartName='+chartName
	    jQuery.get(url, function(strdata, textStatus) 
	    	 { 
	    	//alert(strdata);
		        var data  = eval('('+strdata+')');
		        $.plot($("#"+chartName), data, { xaxis: { mode: "time" }, legend: { show: true, position:'nw'} }); 
	              //$.plot($("#placeholder"), [d], { xaxis: { mode: "time" } });
	            setTimeout(renderchart,1000, chartName); // refresh chart every 1second 
	        } 
	    ); 
	} 		
	 </script>
             
        <div class="body">
	         <div class="nav">
				 <span class="menuButton"><g:link class="list" controller="dashboard" action="index" id="${cluster}">Dashboard</g:link></span>
	        </div>
         <g:if test="${flash.message}">
        	 <div class="message">${flash.message}</div>
         </g:if>
         <!-- Master -->
            <h1>Shard List</h1>
            <div class="list">
                <div class="list">
                <table>
                    <thead>
                        <tr>
                   	        <th class="sortable" >Shard Name</th>
                   	        <th class="sortable" >Index Name</th>
                   	        <th class="sortable" >Shard Path</th>
                        </tr>
                    </thead>
                    <tbody>
                    <g:each in="${shards}" status="i" var="shard">
                        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
                            <td>${shard.shardName}</td>
                         	<td>${shard.indexName}</td>
                         	<td>${shard.shardPath}</td>
                        </tr>
                    </g:each>
                    </tbody>
                </table>
             
            </div>
            </div>
        <g:each in="${charts}" status="i" var="chart">
	          	 <h2>${chart.key}</h2>
	          	 <div id="${chart.key}" style="width:600px;height:300px;"></div> 
	          	 <br/>
             </g:each>
        </div>
        
       
    </body>
</html>
