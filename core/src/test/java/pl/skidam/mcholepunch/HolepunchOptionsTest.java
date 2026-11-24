package pl.skidam.mcholepunch;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import pl.skidam.mcholepunch.internal.protocol.TransportFrame;

class HolepunchOptionsTest {
	@Test
	void markerConstantIsOptionalAndValidated() {
		assertTrue(HolepunchOptions.builder().build().markerConstant().isEmpty(), "the built-in constant is used by default");
		byte[] constant = {(byte) 0xA5, 0x5A, (byte) 0xC3, 0x3C, (byte) 0x96, 0x69, (byte) 0x7E, (byte) 0x87, 0x11, (byte) 0xEE};
		HolepunchOptions options = HolepunchOptions.builder().markerConstant(constant).build();
		assertArrayEquals(constant, options.markerConstant().orElseThrow());
		constant[0] = 0;
		assertArrayEquals(new byte[]{(byte) 0xA5, 0x5A, (byte) 0xC3, 0x3C, (byte) 0x96, 0x69, (byte) 0x7E, (byte) 0x87, 0x11, (byte) 0xEE}, options.markerConstant().orElseThrow(),
				"the option must copy, not alias, the caller's array");
		assertThrows(IllegalArgumentException.class, () -> HolepunchOptions.builder().markerConstant(null));
		assertThrows(IllegalArgumentException.class, () -> HolepunchOptions.builder().markerConstant(new byte[9]));
	}

	@Test
	void frameCapDefaultsToTheSafeSendSizeAndStaysUnderTheProtocolCeiling() {
		assertEquals(HolepunchOptions.DEFAULT_MAXIMUM_FRAME_BYTES, HolepunchOptions.builder().build().maximumFrameBytes());
		assertEquals(TransportFrame.DEFAULT_MAX_FRAME_BYTES, HolepunchOptions.DEFAULT_MAXIMUM_FRAME_BYTES);
		assertEquals(TransportFrame.MAX_FRAME_BYTES, HolepunchOptions.MAX_FRAME_BYTES);
		assertEquals(64 * 1024, HolepunchOptions.builder().maximumFrameBytes(64 * 1024).build().maximumFrameBytes());
		assertThrows(IllegalArgumentException.class, () -> HolepunchOptions.builder().maximumFrameBytes(0));
		assertThrows(IllegalArgumentException.class, () -> HolepunchOptions.builder().maximumFrameBytes(HolepunchOptions.MAX_FRAME_BYTES + 1));
	}

	@Test
	void failureRequiresNonNullKind() {
		assertThrows(NullPointerException.class, () -> new HolepunchFailure(null, "missing kind"));
		assertThrows(NullPointerException.class, () -> new HolepunchFailure(null, "missing kind", new RuntimeException()));
	}
}
