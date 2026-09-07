package ai.altertable.lakehouse;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

class LakehouseClientQuerySchemaTest {
  @Test void acceptsLegacyStringSchema() throws IOException {
    try (LakehouseClient.QueryResult result = parse("[\"legacy_answer\"]\n[1]\n")) {
      assertEquals(List.of("legacy_answer"), result.columns());
      assertEquals(
          List.of(new LakehouseClient.QueryColumn("legacy_answer", null)),
          result.schema());
      assertEquals(1, result.iterator().next().get(0).asInt());
    }
  }

  @Test void preservesTypedSchema() throws IOException {
    try (LakehouseClient.QueryResult result = parse(
        "[{\"name\":\"answer\",\"type\":\"INTEGER\"}]\n[1]\n")) {
      assertEquals(
          List.of(new LakehouseClient.QueryColumn("answer", "INTEGER")),
          result.schema());
      assertEquals(1, result.iterator().next().get(0).asInt());
    }
  }

  @Test void rejectsMalformedSchemaObjects() {
    IOException error = assertThrows(IOException.class,
        () -> parse("[{\"name\":\"answer\",\"type\":1}]\n[1]\n"));

    assertEquals(
        "NDJSON schema line 2 column 1 must be a string or an object with string name and type",
        error.getMessage());
  }

  @Test void keepsTheNameOnlyQueryAllResultConstructor() {
    LakehouseClient.QueryAllResult result = new LakehouseClient.QueryAllResult(
        null, List.of("answer"), List.of());

    assertEquals(
        List.of(new LakehouseClient.QueryColumn("answer", null)),
        result.schema());
  }

  @Test void surfacesQueryErrorsDuringRows() throws IOException {
    try (LakehouseClient.QueryResult result = parse(
        "[\"answer\"]\n[1]\n{\"error\":\"Conversion Error: invalid value\"}\n")) {
      Iterator<List<JsonNode>> rows = result.iterator();
      assertEquals(1, rows.next().get(0).asInt());
      LakehouseClient.QueryError error = assertThrows(LakehouseClient.QueryError.class, rows::next);

      assertTrue(error.getMessage().contains("NDJSON line 4"));
      assertTrue(error.getMessage().contains("Conversion Error: invalid value"));
      assertFalse(rows.hasNext());
      assertEquals("test-request-id", error.requestId());
      assertEquals(200, error.statusCode());
    }
  }

  private static LakehouseClient.QueryResult parse(String lines) throws IOException {
    return new LakehouseClient.QueryResult(new ByteArrayInputStream(("{}\n" + lines).getBytes(UTF_8)),
        new ObjectMapper(), 200, "test-request-id");
  }
}
