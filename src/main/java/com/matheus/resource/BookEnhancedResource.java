package com.matheus.resource;

import com.matheus.model.BookEnhanced;
import com.matheus.service.BookEnhancedService;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/sync/enhanced/books")
public class BookEnhancedResource {

  private final BookEnhancedService bookEnhancedService;

  public BookEnhancedResource(BookEnhancedService bookEnhancedService) {
    this.bookEnhancedService = bookEnhancedService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<BookEnhanced> findAll() {
    return bookEnhancedService.findAll();
  }

  @GET
  @Path("/isbn/{isbn}")
  @Produces(MediaType.APPLICATION_JSON)
  public BookEnhanced findByIsbn(@PathParam("isbn") final String isbn) {
    return bookEnhancedService.findByIsbn(isbn);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public BookEnhanced add(final BookEnhanced book) {
    return bookEnhancedService.add(book);
  }
}
