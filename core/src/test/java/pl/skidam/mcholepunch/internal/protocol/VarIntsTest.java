package pl.skidam.mcholepunch.internal.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class VarIntsTest {
	@Test
	void roundTripsCanonicalValues() throws IOException {
		for (int value : new int[]{0, 1, 127, 128, 255, 2097151, Integer.MAX_VALUE}) {
			ByteBuffer encoded = ByteBuffer.allocate(5);
			VarInts.write(encoded, value);
			encoded.flip();
			assertEquals(value, VarInts.read(encoded));
			assertEquals(0, encoded.remaining());
		}
	}

	@Test
	void rejectsNonCanonicalEncoding() {
		assertThrows(IOException.class, () -> VarInts.read(ByteBuffer.wrap(new byte[]{(byte) 0x80, 0})));
	}
}
