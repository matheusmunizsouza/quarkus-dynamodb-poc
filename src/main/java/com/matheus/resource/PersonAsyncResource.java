package com.matheus.resource;

import com.matheus.model.Person;
import com.matheus.service.PersonAsyncService;
import com.matheus.vo.request.PaginationRequest;
import com.matheus.vo.response.PaginationResponse;
import io.smallrye.mutiny.Uni;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/async/person")
public class PersonAsyncResource {

  private final PersonAsyncService personAsyncService;

  public PersonAsyncResource(PersonAsyncService personAsyncService) {
    this.personAsyncService = personAsyncService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<PaginationResponse<Person>> getAll(final PaginationRequest paginationRequest) {
    return personAsyncService.findAll(paginationRequest);
  }

  @GET
  @Path("/firstname/{firstName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<PaginationResponse<Person>> findByFirstName(
      @PathParam("firstName") final String firstName,
      final PaginationRequest paginationRequest) {
    return personAsyncService.findByFirstName(firstName, paginationRequest);
  }

  @GET
  @Path("/firstname/{firstName}/lastname/{lastName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Person> findByFirstNameAndLastName(
      @PathParam("firstName") final String firstName,
      @PathParam("lastName") final String lastName) {
    return personAsyncService.findByFirstNameAndLastName(firstName, lastName);
  }

  @GET
  @Path("/cpf/{cpf}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<PaginationResponse<Person>> findByCpf(
      @PathParam("cpf") final String cpf,
      final PaginationRequest paginationRequest) {
    return personAsyncService.findByCpf(cpf, paginationRequest);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Person> add(final Person person) {
    return personAsyncService.add(person);
  }

  @DELETE
  @Path("/firstname/{firstName}/lastname/{lastName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Person> delete(
      @PathParam("firstName") final String firstName,
      @PathParam("lastName") final String lastName) {
    return personAsyncService.delete(firstName, lastName);
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Person> update(final Person person) {
    return personAsyncService.update(person);
  }

  @PUT
  @Path("/batch")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Void> putBatch(final List<Person> people) {
    return personAsyncService.putBatch(people);
  }
}
