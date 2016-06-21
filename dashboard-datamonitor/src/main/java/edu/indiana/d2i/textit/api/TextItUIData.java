package edu.indiana.d2i.textit.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
public abstract class TextItUIData {

	@GET
	@Path("/{country}/flows")
	@Produces(MediaType.APPLICATION_JSON)
	public abstract Response getAllFlows(@PathParam("country") String country);

	@GET
	@Path("/{country}/lastweekflows")
	@Produces(MediaType.APPLICATION_JSON)
	public abstract Response getAllLastWeekFlows(@PathParam("country") String country);

	@GET
	@Path("/{country}/runs")
	@Produces(MediaType.APPLICATION_JSON)
	public abstract Response getAllRuns(@PathParam("country") String country);

	@GET
	@Path("/{country}/lastweekruns")
	@Produces(MediaType.APPLICATION_JSON)
	public abstract Response getAllLastWeekRuns(@PathParam("country") String country);

	@GET
	@Path("/{country}/contacts")
	@Produces(MediaType.APPLICATION_JSON)
	public abstract Response getAllContacts(@PathParam("country") String country);

	@GET
	@Path("/{country}/lastweekcontacts")
	@Produces(MediaType.APPLICATION_JSON)
	public abstract Response getAllLastWeekModifiedContacts(@PathParam("country") String country);

	@GET
	@Path("/{country}/all")
	@Produces(MediaType.APPLICATION_JSON)
	public abstract Response getAllData(@PathParam("country") String country);

	@GET
	@Path("/{country}/runsofflow")
	@Produces(MediaType.APPLICATION_JSON)
	public abstract Response getRunsOfFlowData(@PathParam("country") String country);

	@GET
	@Path("/{country}/quesforanswers")
	@Produces(MediaType.APPLICATION_JSON)
	public abstract Response getQuesForAnswers(@PathParam("country") String country);
	
}
