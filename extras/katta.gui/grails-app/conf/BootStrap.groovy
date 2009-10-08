import org.codehaus.groovy.grails.web.servlet.GrailsApplicationAttributes;
import org.springframework.context.ApplicationContext;

class BootStrap {

     def init = { servletContext ->
     // we want to start listing to zkUpdates

     ApplicationContext ctx = servletContext.getAttribute(GrailsApplicationAttributes.APPLICATION_CONTEXT)
     ZkService service = (ZkService) ctx.getBean("zkService")
     service.subScribeMetricsUpdates()


     
     }
     def destroy = {
     }
} 