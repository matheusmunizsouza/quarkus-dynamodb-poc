package com.matheus.resource;

import com.matheus.model.BookEnhanced;
import com.matheus.service.BookAsyncEnhancedService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/async/enhanced/books")
public class BookAsyncEnhancedResource {

  private final BookAsyncEnhancedService bookAsyncEnhancedService;

  public BookAsyncEnhancedResource(BookAsyncEnhancedService bookAsyncEnhancedService) {
    this.bookAsyncEnhancedService = bookAsyncEnhancedService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Multi<BookEnhanced> getAll() {
    return bookAsyncEnhancedService.findAll();
  }

  @GET
  @Path("/isbn/{isbn}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<BookEnhanced> findByIsbn(@PathParam("isbn") final String isbn) {
    return bookAsyncEnhancedService.findByIsbn(isbn);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<BookEnhanced> add(final BookEnhanced book) {
    return bookAsyncEnhancedService.add(book);
  }
}
