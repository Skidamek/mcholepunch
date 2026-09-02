package pl.skidam.mcholepunch.internal.protocol;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public record HolepunchMarker(int protocolVersion, int flags, int nonce) {
	public static final int CURRENT_PROTOCOL_VERSION = 1;
	public static final int SUPPORTED_FLAGS = 0;
	private static final byte[] MASK_DOMAIN = "mcholepunch:uuid-mask".getBytes(StandardCharsets.UTF_8);
	private static final byte[] FIXED_MARKER = markerHash();
	private static final int[] PAYLOAD_BIT_POSITIONS = payloadBitPositions();

	public HolepunchMarker {
		if (protocolVersion < 0 || protocolVersion > 255) {
			throw new IllegalArgumentException("protocolVersion must fit in one byte");
		}
		if (flags < 0 || flags > 255) {
			throw new IllegalArgumentException("flags must fit in one byte");
		}
		if (nonce < 0 || nonce >= (1 << 26)) {
			throw new IllegalArgumentException("nonce must fit in 26 bits");
		}
	}

	public static UUID create(int flags, int nonce) {
		HolepunchMarker marker = new HolepunchMarker(CURRENT_PROTOCOL_VERSION, flags, nonce);
		byte[] mask = nonceMask(nonce);
		long most = 0;
		long least = 0;
		int payloadIndex = 0;
		for (int bit = 127; bit >= 0; bit--) {
			if (bit == 76 || bit == 77 || bit == 78 || bit == 79) {
				continue;
			}
			if (bit == 62 || bit == 63) {
				continue;
			}
			if (payloadBit(marker, mask, payloadIndex++)) {
				if (bit >= 64) {
					most |= 1L << (bit - 64);
				} else {
					least |= 1L << bit;
				}
			}
		}
		most = (most & ~(0xFL << 12)) | (4L << 12);
		least = (least & ~(0x3L << 62)) | (0x2L << 62);
		return new UUID(most, least);
	}

	public static UUID random(int flags) {
		return create(flags, ThreadLocalRandom.current().nextInt(1 << 26));
	}

	public static Optional<HolepunchMarker> decode(UUID uuid) {
		if (uuid == null || uuid.version() != 4 || uuid.variant() != 2) {
			return Optional.empty();
		}
		boolean[] payload = new boolean[122];
		for (int index = 0; index < PAYLOAD_BIT_POSITIONS.length; index++) {
			int bit = PAYLOAD_BIT_POSITIONS[index];
			payload[index] = bit >= 64 ? ((uuid.getMostSignificantBits() >>> (bit - 64)) & 1L) != 0 : ((uuid.getLeastSignificantBits() >>> bit) & 1L) != 0;
		}
		int nonce = intValue(payload, 96, 26);
		byte[] mask = nonceMask(nonce);
		for (int index = 0; index < 96; index++) {
			payload[index] ^= bit(mask, index);
		}
		for (int index = 0; index < 80; index++) {
			if (payload[index] != bit(FIXED_MARKER, index)) {
				return Optional.empty();
			}
		}
		int protocolVersion = byteValue(payload, 80);
		int flags = byteValue(payload, 88);
		if (protocolVersion != CURRENT_PROTOCOL_VERSION || (flags & ~SUPPORTED_FLAGS) != 0) {
			return Optional.empty();
		}
		return Optional.of(new HolepunchMarker(protocolVersion, flags, nonce));
	}

	public UUID uuid() {
		return create(flags, nonce);
	}

	private static boolean payloadBit(HolepunchMarker marker, byte[] mask, int index) {
		boolean value;
		if (index < 80) {
			value = bit(FIXED_MARKER, index);
		} else if (index < 88) {
			value = ((marker.protocolVersion >>> (87 - index)) & 1) != 0;
		} else if (index < 96) {
			value = ((marker.flags >>> (95 - index)) & 1) != 0;
		} else {
			return ((marker.nonce >>> (121 - index)) & 1) != 0;
		}
		return value ^ bit(mask, index);
	}

	private static boolean bit(byte[] bytes, int index) {
		return ((bytes[index / 8] >>> (7 - index % 8)) & 1) != 0;
	}

	private static int byteValue(boolean[] payload, int offset) {
		int result = 0;
		for (int index = 0; index < 8; index++) {
			result = (result << 1) | (payload[offset + index] ? 1 : 0);
		}
		return result;
	}

	private static int intValue(boolean[] payload, int offset, int length) {
		int result = 0;
		for (int index = 0; index < length; index++) {
			result = (result << 1) | (payload[offset + index] ? 1 : 0);
		}
		return result;
	}

	private static int[] payloadBitPositions() {
		int[] positions = new int[122];
		int index = 0;
		for (int bit = 127; bit >= 0; bit--) {
			if (bit == 76 || bit == 77 || bit == 78 || bit == 79 || bit == 62 || bit == 63) {
				continue;
			}
			positions[index++] = bit;
		}
		return positions;
	}

	private static byte[] nonceMask(int nonce) {
		MessageDigest digest = sha256();
		digest.update(MASK_DOMAIN);
		digest.update((byte) (nonce >>> 24));
		digest.update((byte) (nonce >>> 16));
		digest.update((byte) (nonce >>> 8));
		digest.update((byte) nonce);
		return digest.digest();
	}

	private static byte[] markerHash() {
		return Arrays.copyOf(sha256().digest("mcholepunch:marker-hash".getBytes(StandardCharsets.UTF_8)), 10);
	}

	private static MessageDigest sha256() {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException exception) {
			throw new ExceptionInInitializerError(exception);
		}
	}
}
