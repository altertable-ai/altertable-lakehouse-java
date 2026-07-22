package ai.altertable.lakehouse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

class LakehouseClientTest {
  private static final int LAKEHOUSE_PORT = 15000;
  private static final String USERNAME = "testuser";
  private static final String PASSWORD = "testpass";
  private static final boolean CI = System.getenv("CI") != null;
  private static final GenericContainer<?> MOCK = new GenericContainer<>(
      DockerImageName.parse("ghcr.io/altertable-ai/altertable-mock:latest"))
      .withExposedPorts(LAKEHOUSE_PORT)
      .withEnv("ALTERTABLE_MOCK_USERS", USERNAME + ":" + PASSWORD)
      .waitingFor(Wait.forListeningPort())
      .withStartupTimeout(Duration.ofSeconds(90));
  private static LakehouseClient client;

  @BeforeAll static void startMock() {
    int port;
    if (CI) {
      port = Integer.parseInt(System.getenv().getOrDefault("ALTERTABLE_MOCK_PORT", "15000"));
    } else {
      MOCK.start();
      port = MOCK.getMappedPort(LAKEHOUSE_PORT);
    }
    client = new LakehouseClient(new LakehouseClient.Config()
        .baseUrl("http://localhost:" + port)
        .credentials(USERNAME, PASSWORD)
        .retries(0));
  }

  @AfterAll static void stopMock() {
    if (!CI) MOCK.stop();
  }

  @Test void streamsAndAccumulatesRowsFromTheMock() {
    LakehouseClient.QueryResult streamed = client.query(LakehouseClient.QueryRequest.of("SELECT 1 AS answer"));
    assertEquals("SELECT 1 AS answer", streamed.metadata().get("statement").asText());
    assertEquals(List.of("answer"), streamed.columns());
    JsonNode row = streamed.iterator().next();
    assertEquals(1, row.get("answer").asInt());
    streamed.close();

    LakehouseClient.QueryAllResult all = client.queryAll(LakehouseClient.QueryRequest.of("SELECT 1 AS answer UNION ALL SELECT 2"));
    assertEquals(2, all.rows().size());
    assertEquals(2, all.rows().get(1).get("answer").asInt());
  }

  @Test void supportsIngestionAppendAndUpsertAgainstTheMock() {
    byte[] csv = "id,name\n1,Alice\n2,Bob\n".getBytes(StandardCharsets.UTF_8);
    client.upload("memory", "main", "people", LakehouseClient.UploadMode.CREATE, csv, "text/csv");
    client.append("memory", "main", "people", LakehouseClient.AppendRequest.single(Map.of("id", 3, "name", "Cara")), null);
    client.upsert("memory", "main", "people", "id", "[{\"id\":2,\"name\":\"Bobby\"}]".getBytes(StandardCharsets.UTF_8), "application/json");

    LakehouseClient.QueryAllResult result = client.queryAll(LakehouseClient.QueryRequest.of("SELECT id, name FROM people ORDER BY id"));
    assertEquals(3, result.rows().size());
    assertEquals("Bobby", result.rows().get(1).get("name").asText());
  }

  @Test void getsQueryLogCancelsAndUsesSqlHelpersAgainstTheMock() {
    LakehouseClient.QueryAllResult query = client.queryAll(LakehouseClient.QueryRequest.of("SELECT 42 AS answer"));
    UUID queryId = UUID.fromString(query.metadata().get("query_id").asText());
    String sessionId = query.metadata().get("session_id").asText();

    assertEquals(queryId, client.getQuery(queryId).uuid());
    assertTrue(client.cancelQuery(queryId, sessionId).cancelled());
    assertTrue(client.validate(LakehouseClient.ValidateRequest.of("SELECT 1")).valid());
    assertFalse(client.validate(LakehouseClient.ValidateRequest.of("NOT VALID SQL !!!")).valid());
    assertTrue(client.autocomplete(LakehouseClient.AutocompleteRequest.of("SEL")).suggestions().stream()
        .anyMatch(suggestion -> suggestion.suggestion().trim().equals("SELECT")));
  }

  @Test void mapsAuthenticationFailureFromTheMock() {
    LakehouseClient invalidClient = new LakehouseClient(new LakehouseClient.Config()
        .baseUrl(clientBaseUrl())
        .credentials(USERNAME, "wrong-password")
        .retries(0));

    LakehouseClient.AuthError error = assertThrows(LakehouseClient.AuthError.class,
        () -> invalidClient.validate(LakehouseClient.ValidateRequest.of("SELECT 1")));
    assertEquals(401, error.statusCode());
  }

  @Test void preservesAppendOneOfShape() {
    LakehouseClient.AppendRequest request = LakehouseClient.AppendRequest.batch(List.of(Map.of("id", 1), Map.of("id", 2)));
    assertTrue(request.payload().isArray());
  }

  private static String clientBaseUrl() {
    int port = CI ? Integer.parseInt(System.getenv().getOrDefault("ALTERTABLE_MOCK_PORT", "15000")) : MOCK.getMappedPort(LAKEHOUSE_PORT);
    return "http://localhost:" + port;
  }
}
