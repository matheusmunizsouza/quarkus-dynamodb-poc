package com.matheus.resource;

import com.matheus.model.Person;
import com.matheus.service.PersonService;
import com.matheus.vo.request.PaginationRequest;
import com.matheus.vo.response.PaginationResponse;
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

@Path("/sync/person")
public class PersonResource {

  private final PersonService personService;

  public PersonResource(PersonService personService) {
    this.personService = personService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<Person> findAll() {
    return personService.findAll();
  }

  @GET
  @Path("/firstname/{firstName}")
  @Produces(MediaType.APPLICATION_JSON)
  public PaginationResponse<Person> findByFirstName(
      @PathParam("firstName") final String firstName,
      final PaginationRequest paginationRequest) {
    return personService.findByFirstName(firstName, paginationRequest);
  }

  @GET
  @Path("/firstname/{firstName}/lastname/{lastName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Person findByFirstNameAndLastName(
      @PathParam("firstName") final String firstName,
      @PathParam("lastName") final String lastName) {
    return personService.findByFirstNameAndLastName(firstName, lastName);
  }

  @GET
  @Path("/cpf/{cpf}")
  @Produces(MediaType.APPLICATION_JSON)
  public PaginationResponse<Person> findByCpf(
      @PathParam("cpf") final String cpf,
      final PaginationRequest paginationRequest) {
    return personService.findByCpf(cpf, paginationRequest);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Person add(final Person person) {
    return personService.add(person);
  }

  @DELETE
  @Path("/firstname/{firstName}/lastname/{lastName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Person delete(
      @PathParam("firstName") final String firstName,
      @PathParam("lastName") final String lastName) {
    return personService.delete(firstName, lastName);
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Person update(final Person person) {
    return personService.update(person);
  }
}
