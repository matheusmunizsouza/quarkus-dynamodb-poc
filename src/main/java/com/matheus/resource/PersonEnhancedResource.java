package com.matheus.resource;

import com.matheus.model.PersonEnhanced;
import com.matheus.service.PersonEnhancedService;
import com.matheus.vo.request.PaginationRequest;
import com.matheus.vo.response.PaginationResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/sync/enhanced/person")
public class PersonEnhancedResource {

  private final PersonEnhancedService personEnhancedService;

  public PersonEnhancedResource(PersonEnhancedService personEnhancedService) {
    this.personEnhancedService = personEnhancedService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public PaginationResponse<PersonEnhanced> findAll(final PaginationRequest paginationRequest) {
    return personEnhancedService.findAll(paginationRequest);
  }

  @GET
  @Path("/firstname/{firstName}")
  @Produces(MediaType.APPLICATION_JSON)
  public PaginationResponse<PersonEnhanced> findByFirstName(
      @PathParam("firstName") final String firstName,
      final PaginationRequest paginationRequest) {
    return personEnhancedService.findByFirstName(firstName, paginationRequest);
  }

  @GET
  @Path("/firstname/{firstName}/lastname/{lastName}")
  @Produces(MediaType.APPLICATION_JSON)
  public PersonEnhanced findByFirstNameAndLastName(
      @PathParam("firstName") final String firstName,
      @PathParam("lastName") final String lastName) {
    return personEnhancedService.findByFirstNameAndLastName(firstName, lastName);
  }

  @GET
  @Path("/cpf/{cpf}")
  @Produces(MediaType.APPLICATION_JSON)
  public PaginationResponse<PersonEnhanced> findByCpf(
      @PathParam("cpf") final String cpf,
      final PaginationRequest paginationRequest) {
    return personEnhancedService.findByCpf(cpf, paginationRequest);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public PersonEnhanced add(final PersonEnhanced person) {
    return personEnhancedService.add(person);
  }

  @DELETE
  @Path("/firstname/{firstName}/lastname/{lastName}")
  @Produces(MediaType.APPLICATION_JSON)
  public PersonEnhanced delete(
      @PathParam("firstName") final String firstName,
      @PathParam("lastName") final String lastName) {
    return personEnhancedService.delete(firstName, lastName);
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public PersonEnhanced update(final PersonEnhanced person) {
    return personEnhancedService.update(person);
  }
}
