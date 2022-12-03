package com.matheus.resource;

import com.matheus.model.PersonEnhanced;
import com.matheus.service.PersonEnhancedAsyncService;
import com.matheus.vo.request.PaginationRequest;
import com.matheus.vo.response.PaginationResponse;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/async/enhanced/person")
public class PersonEnhancedAsyncResource {

  private final PersonEnhancedAsyncService personEnhancedAsyncService;

  public PersonEnhancedAsyncResource(PersonEnhancedAsyncService personEnhancedAsyncService) {
    this.personEnhancedAsyncService = personEnhancedAsyncService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Multi<PersonEnhanced> getAll() {
    return personEnhancedAsyncService.findAll();
  }

  @GET
  @Path("/firstname/{firstName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<PersonEnhanced> findByFirstName(@PathParam("firstName") final String firstName) {
    return personEnhancedAsyncService.findByFirstName(firstName);
  }

  @GET
  @Path("/firstname/{firstName}/lastname/{lastName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<PersonEnhanced> findByFirstNameAndLastName(
      @PathParam("firstName") final String firstName,
      @PathParam("lastName") final String lastName) {
    return personEnhancedAsyncService.findByFirstNameAndLastName(firstName, lastName);
  }

  @GET
  @Path("/cpf/{cpf}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<PaginationResponse<PersonEnhanced>> findByCpf(
      @PathParam("cpf") final String cpf,
      final PaginationRequest paginationRequest) {
    return personEnhancedAsyncService.findByCpf(cpf, paginationRequest);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<PersonEnhanced> add(final PersonEnhanced book) {
    return personEnhancedAsyncService.add(book);
  }
}
