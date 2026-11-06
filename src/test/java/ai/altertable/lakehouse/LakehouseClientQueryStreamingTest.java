package ai.altertable.lakehouse;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

class LakehouseClientQueryStreamingTest {
  @Test void returnsEachRowWithoutReadingTheFollowingLine() throws Exception {
    GatedBody body = new GatedBody();
    try (LakehouseClient.QueryResult result = new LakehouseClient.QueryResult(
        body, new ObjectMapper(), 200, "stream-test")) {
      var rows = result.iterator();
      assertTrue(rows.hasNext());
      assertTrue(rows.hasNext());
      assertEquals("Zoë", rows.next().get(0).asText());
      body.release();
      // next() also works without a preceding hasNext().
      assertEquals("last", rows.next().get(0).asText());
      assertFalse(rows.hasNext());
    }
  }

  private static final class GatedBody extends InputStream {
    private final ByteArrayInputStream first = new ByteArrayInputStream(
        "{}\n[{\"name\":\"name\",\"type\":\"VARCHAR\"}]\n[\"Zoë\"]\n".getBytes(UTF_8));
    private final ByteArrayInputStream last = new ByteArrayInputStream("[\"last\"]".getBytes(UTF_8));
    private boolean released;

    void release() { released = true; }

    @Override public int read() {
      byte[] value = new byte[1];
      return read(value, 0, 1) == -1 ? -1 : Byte.toUnsignedInt(value[0]);
    }

    @Override public int read(byte[] target, int offset, int length) {
      if (length == 0) return 0;
      if (first.available() > 0) return first.read(target, offset, length);
      if (!released) throw new AssertionError("Read beyond the available row before returning it");
      return last.read(target, offset, length);
    }
  }
}
