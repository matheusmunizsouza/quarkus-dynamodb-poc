package com.matheus.resource;

import com.matheus.model.Book;
import com.matheus.service.BookService;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/sync/books")
public class BookResource {

  private final BookService bookService;

  public BookResource(BookService bookService) {
    this.bookService = bookService;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<Book> findAll() {
    return bookService.findAll();
  }

  @GET
  @Path("/isbn/{isbn}")
  @Produces(MediaType.APPLICATION_JSON)
  public Book findByIsbn(@PathParam("isbn") final String isbn) {
    return bookService.findByIsbn(isbn);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Book add(final Book book) {
    return bookService.add(book);
  }
}
