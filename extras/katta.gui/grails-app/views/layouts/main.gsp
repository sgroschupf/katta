<html>
    <head>
        <title><g:layoutTitle default="Grails" /></title>
        <link rel="stylesheet" href="${resource(dir:'css',file:'style.css')}" type="text/css" media="screen"/>
        <link rel="stylesheet" href="${resource(dir:'css',file:'main.css')}" />
        
        <link rel="shortcut icon" href="${resource(dir:'images',file:'favicon.ico')}" type="image/x-icon" />
        <g:layoutHead />
        <g:javascript library="application" />				
    </head>
    <body>
    
    <div id="header">
		<div class="header_wrapper">
			<div class="logo">
            <a href="${resource(dir:'')}">
            	<img src="${resource(dir:'images',file:'katta-logo.png')}" alt="katta" longdesc="http://katta.sourceforge.net" />
            </a>	
			</div>
			
			<div class="header_right">
			<ul>
		        <li class="page_item page-item-16 current_page_item"><a  href="${resource(dir:'')}" title="Home">Home</a></li>
			</ul>
			</div>
		</div>
	</div>
    <br/>
    <br/>
        <g:layoutBody />		
    </body>	
</html>