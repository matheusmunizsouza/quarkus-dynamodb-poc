package com.matheus.resource;

import com.matheus.model.Book;
import com.matheus.service.BookAsyncService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/async/books")
public class BookAsyncResource {

  private final BookAsyncService bookAsyncService;

  public BookAsyncResource(BookAsyncService bookAsyncService) {
    this.bookAsyncService = bookAsyncService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Multi<Book> getAll() {
    return bookAsyncService.findAll();
  }

  @GET
  @Path("/isbn/{isbn}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Book> findByIsbn(@PathParam("isbn") final String isbn) {
    return bookAsyncService.findByIsbn(isbn);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Book> add(final Book book) {
    return bookAsyncService.add(book);
  }
}
