package ai.altertable.lakehouse;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LakehouseClientQuerySerializationTest {
  @Test void omitsUnspecifiedBindParametersOnTheWire() throws Exception {
    assertFalse(capture(LakehouseClient.QueryRequest.of("SELECT 1")).has("params"));
  }

  @Test void preservesExplicitNullBindValues() throws Exception {
    Map<String, Object> named = new LinkedHashMap<>();
    named.put("value", null);
    assertTrue(capture(request(named)).path("params").path("value").isNull());
    assertTrue(capture(request(Arrays.asList(25, null))).path("params").get(1).isNull());
  }

  private static LakehouseClient.QueryRequest request(Object params) {
    return new LakehouseClient.QueryRequest("SELECT 1", null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, params);
  }

  private static JsonNode capture(LakehouseClient.QueryRequest request) throws Exception {
    AtomicReference<JsonNode> captured = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/query", exchange -> {
      captured.set(new ObjectMapper().readTree(exchange.getRequestBody()));
      byte[] response = "{}\n[]\n".getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson");
      exchange.sendResponseHeaders(200, response.length);
      try (var output = exchange.getResponseBody()) { output.write(response); }
    });
    server.start();
    try {
      var client = new LakehouseClient(new LakehouseClient.Config()
          .baseUrl("http://127.0.0.1:" + server.getAddress().getPort())
          .credentials("test", "test").retries(0));
      try (var result = client.query(request)) { assertTrue(result.columns().isEmpty()); }
      return captured.get();
    } finally {
      server.stop(0);
    }
  }
}
