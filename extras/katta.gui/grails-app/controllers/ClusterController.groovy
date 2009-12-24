
class ClusterController {
	// zkService
	def zkService
	def index = { redirect(action:list,params:params)
	}
	
	// the delete, save and update actions only accept POST requests
	static allowedMethods = [delete:'POST', save:'POST', update:'POST']
	
	def list = {
		params.max = Math.min( params.max ? params.max.toInteger() : 10,  100)
		[ clusterInstanceList: Cluster.list( params ), clusterInstanceTotal: Cluster.count() ]
	}
	
	def show = {
		def clusterInstance = Cluster.get( params.id )
		
		if(!clusterInstance) {
			flash.message = "Cluster not found with id ${params.id}"
			redirect(action:list)
		}
		else { return [ clusterInstance : clusterInstance ]
		}
	}
	
	def delete = {
		def clusterInstance = Cluster.get( params.id )
		if(clusterInstance) {
			try {
				clusterInstance.delete(flush:true)
				flash.message = "Cluster ${params.id} deleted"
				redirect(controller:'home', action:'index')
			}
			catch(org.springframework.dao.DataIntegrityViolationException e) {
				flash.message = "Cluster ${params.id} could not be deleted"
				redirect(action:show,id:params.id)
			}
		}
		else {
			flash.message = "Cluster not found with id ${params.id}"
			redirect(controller:'home', action:'index')
		}
	}
	
	def edit = {
		def clusterInstance = Cluster.get( params.id )
		
		if(!clusterInstance) {
			flash.message = "Cluster not found with id ${params.id}"
			redirect(action:list)
		}
		else {
			return [ clusterInstance : clusterInstance ]
		}
	}
	
	def update = {
		def clusterInstance = Cluster.get( params.id )
		if(clusterInstance) {
			if(params.version) {
				def version = params.version.toLong()
				if(clusterInstance.version > version) {
					
					clusterInstance.errors.rejectValue("version", "cluster.optimistic.locking.failure", "Another user has updated this Cluster while you were editing.")
					render(view:'edit',model:[clusterInstance:clusterInstance])
					return
				}
			}
			clusterInstance.properties = params
			if(!clusterInstance.hasErrors() && clusterInstance.save()) {
				flash.message = "Cluster ${params.id} updated"
				redirect(action:show,id:clusterInstance.id)
			}
			else {
				render(view:'edit',model:[clusterInstance:clusterInstance])
			}
		}
		else {
			flash.message = "Cluster not found with id ${params.id}"
			redirect(action:list)
		}
	}
	
	def create = {
		def clusterInstance = new Cluster()
		clusterInstance.properties = params
		return ['clusterInstance':clusterInstance]
	}
	
	def save = {
		def clusterInstance = new Cluster(params)
		if(!clusterInstance.hasErrors() && clusterInstance.save()) {
			flash.message = "Cluster ${clusterInstance.id} created"
			redirect(action:show,id:clusterInstance.id)
		}
		else {
			render(view:'create',model:[clusterInstance:clusterInstance])
		}
		zkService.startMetricsListening(clusterInstance)
	}
	
	
	
}
