class UrlMappings {
    static mappings = {
    	"/"(controller:"home", action:"index")
    	"/$controller/$action?/$id?"{
	      constraints {
			 // apply constraints here
		  }
	  }
     // "/dashboard/$id?"(controller:"dashboard", action:"dashboard")
	  "500"(view:'/error')
	}
}
