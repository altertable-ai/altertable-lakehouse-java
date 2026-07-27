package ai.altertable.lakehouse;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LakehouseClientQuerySchemaTest {
  private static HttpServer server;
  private static LakehouseClient client;

  @BeforeAll static void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/query", exchange -> {
      String request = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
      byte[] body = responseFor(request).getBytes(UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/x-ndjson");
      exchange.getResponseHeaders().add("X-Request-ID", "test-request-id");
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
    client = new LakehouseClient(new LakehouseClient.Config()
        .baseUrl("http://127.0.0.1:" + server.getAddress().getPort())
        .credentials("testuser", "testpass")
        .retries(0));
  }

  @AfterAll static void stopServer() {
    server.stop(0);
  }

  @Test void preservesTypedSchemaForStreamingAndAccumulatedResults() {
    try (LakehouseClient.QueryResult result = client.query(
        LakehouseClient.QueryRequest.of("SELECT 1 AS answer"))) {
      assertEquals(List.of("answer"), result.columns());
      assertEquals(
          List.of(new LakehouseClient.QueryColumn("answer", "INTEGER")),
          result.schema());
    }

    LakehouseClient.QueryAllResult result = client.queryAll(
        LakehouseClient.QueryRequest.of("SELECT 1 AS answer"));
    assertEquals(
        List.of(new LakehouseClient.QueryColumn("answer", "INTEGER")),
        result.schema());
    assertEquals(1, result.rows().get(0).get(0).asInt());
  }

  @Test void acceptsLegacyStringSchema() {
    try (LakehouseClient.QueryResult result = client.query(
        LakehouseClient.QueryRequest.of("SELECT 1 AS legacy_answer"))) {
      assertEquals(List.of("legacy_answer"), result.columns());
      assertEquals(
          List.of(new LakehouseClient.QueryColumn("legacy_answer", null)),
          result.schema());
    }
  }

  @Test void rejectsMalformedSchemaObjects() {
    LakehouseClient.ParseError error = assertThrows(
        LakehouseClient.ParseError.class,
        () -> client.query(LakehouseClient.QueryRequest.of("SELECT 1 AS malformed_answer")));

    assertEquals(
        "NDJSON schema line 2 column 1 must be a string or an object with string name and type",
        error.getCause().getMessage());
  }

  @Test void keepsTheNameOnlyQueryAllResultConstructor() {
    LakehouseClient.QueryAllResult result = new LakehouseClient.QueryAllResult(
        null, List.of("answer"), List.of());

    assertEquals(
        List.of(new LakehouseClient.QueryColumn("answer", null)),
        result.schema());
  }

  @Test void surfacesQueryErrorsBeforeColumns() {
    LakehouseClient.QueryError error = assertThrows(
        LakehouseClient.QueryError.class,
        () -> client.query(LakehouseClient.QueryRequest.of("SELECT * FROM line_two_error")));

    assertTrue(error.getMessage().contains("NDJSON line 2"));
    assertTrue(error.getMessage().contains("Catalog Error: unknown table"));
    assertEquals("test-request-id", error.requestId());
    assertEquals(200, error.statusCode());
  }

  @Test void surfacesQueryErrorsDuringRows() {
    try (LakehouseClient.QueryResult result = client.query(
        LakehouseClient.QueryRequest.of("SELECT * FROM row_error"))) {
      Iterator<List<JsonNode>> rows = result.iterator();
      LakehouseClient.QueryError error = assertThrows(
          LakehouseClient.QueryError.class,
          rows::next);

      assertTrue(error.getMessage().contains("NDJSON line 3"));
      assertTrue(error.getMessage().contains("Conversion Error: invalid value"));
      assertFalse(rows.hasNext());
      assertEquals("test-request-id", error.requestId());
      assertEquals(200, error.statusCode());
    }
  }

  private static String responseFor(String request) {
    String metadata = "{\"statement\":\"SELECT 1 AS answer\",\"rows_limit\":null,\"rows_offset\":null,\"init_time_ms\":1,\"connections_errors\":{},\"session_id\":\"123e4567-e89b-12d3-a456-426614174000\",\"query_id\":\"123e4567-e89b-12d3-a456-426614174000\",\"worker_slug\":\"test-worker\"}\n";
    if (request.contains("line_two_error")) {
      return metadata + "{\"error\":\"Catalog Error: unknown table\"}\n";
    }
    if (request.contains("row_error")) {
      return metadata
          + "[{\"name\":\"answer\",\"type\":\"INTEGER\"}]\n"
          + "{\"error\":\"Conversion Error: invalid value\"}\n";
    }
    if (request.contains("legacy_answer")) {
      return metadata + "[\"legacy_answer\"]\n[1]\n";
    }
    if (request.contains("malformed_answer")) {
      return metadata + "[{\"name\":\"answer\",\"type\":1}]\n[1]\n";
    }
    return metadata + "[{\"name\":\"answer\",\"type\":\"INTEGER\"}]\n[1]\n";
  }
}
