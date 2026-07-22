package ai.altertable.lakehouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LakehouseClientTest {
  private HttpServer server;
  private LakehouseClient client;

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/", this::respond);
    server.start();
    client = new LakehouseClient(new LakehouseClient.Config()
        .baseUrl("http://localhost:" + server.getAddress().getPort())
        .credentials("testuser", "testpass").retries(0));
  }

  @AfterEach void stopServer() { server.stop(0); }

  @Test void streamsRowsAndAccumulatesThem() {
    LakehouseClient.QueryResult result = client.query(LakehouseClient.QueryRequest.of("select 1"));
    assertEquals("select 1", result.metadata().get("statement").asText());
    assertEquals(List.of("number"), result.columns());
    assertEquals(1, result.iterator().next().get(0).asInt());
    result.close();

    LakehouseClient.QueryAllResult all = client.queryAll(LakehouseClient.QueryRequest.of("select 1"));
    assertEquals(2, all.rows().size());
  }

  @Test void constructsAllRequiredEndpointRequests() {
    UUID id = UUID.randomUUID();
    assertTrue(client.append("cat", "public", "events", LakehouseClient.AppendRequest.single(Map.of("id", 1)), null).ok());
    assertEquals(id, client.getTask(id).taskId());
    assertFalse(client.getQuery(id).query().isBlank());
    assertTrue(client.cancelQuery(id, "session").cancelled());
    client.upload("cat", "public", "events", LakehouseClient.UploadMode.CREATE, "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8), "text/csv");
    client.upsert("cat", "public", "events", "id", "id,name\n1,a\n".getBytes(StandardCharsets.UTF_8), "text/csv");
    assertTrue(client.validate(LakehouseClient.ValidateRequest.of("select 1")).valid());
    assertEquals("SEL", client.autocomplete(LakehouseClient.AutocompleteRequest.of("SEL")).statement());
  }

  @Test void mapsAuthenticationFailuresAndRejectsMissingCredentials() {
    LakehouseClient.Config config = new LakehouseClient.Config().baseUrl("http://localhost:1");
    assertThrows(LakehouseClient.ConfigurationError.class, () -> new LakehouseClient(config));
    LakehouseClient.AuthError error = assertThrows(LakehouseClient.AuthError.class,
        () -> client.validate(LakehouseClient.ValidateRequest.of("unauthorized")));
    assertEquals(401, error.statusCode());
  }

  @Test void preservesAppendOneOfShape() {
    LakehouseClient.AppendRequest request = LakehouseClient.AppendRequest.batch(List.of(Map.of("id", 1), Map.of("id", 2)));
    assertTrue(request.payload().isArray());
  }

  private void respond(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    String body;
    int status = 200;
    if (path.equals("/query")) {
      exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
      body = "{\"statement\":\"select 1\",\"session_id\":\"s\",\"query_id\":\"q\"}\n[\"number\"]\n[1]\n[2]\n";
    } else if (path.equals("/validate") && new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).contains("unauthorized")) {
      status = 401; body = "{\"error\":\"unauthorized\"}";
    } else if (path.equals("/append")) body = "{\"ok\":true,\"error_code\":null,\"error_message\":null,\"task_id\":null}";
    else if (path.startsWith("/tasks/")) body = "{\"task_id\":\"" + path.substring(7) + "\",\"status\":\"pending\"}";
    else if (path.startsWith("/query/") && exchange.getRequestMethod().equals("DELETE")) body = "{\"cancelled\":true,\"message\":\"cancelled\"}";
    else if (path.startsWith("/query/")) body = "{\"uuid\":\"" + path.substring(7) + "\",\"start_time\":\"2026-01-01T00:00:00Z\",\"query\":\"select 1\",\"client_interface\":\"HttpQuery\",\"visible\":true,\"session_id\":\"session\",\"threads\":1}";
    else if (path.equals("/validate")) body = "{\"valid\":true,\"statement\":\"select 1\",\"connections_errors\":{},\"error\":null}";
    else if (path.equals("/autocomplete")) body = "{\"suggestions\":[],\"statement\":\"SEL\",\"connections_errors\":{}}";
    else body = "{}";
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }
}
