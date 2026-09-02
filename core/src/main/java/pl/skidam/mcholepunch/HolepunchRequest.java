package pl.skidam.mcholepunch;

import java.security.SecureRandom;
import java.util.Objects;

public record HolepunchRequest(String host, int port, int protocolVersion, HolepunchHandler handler, HolepunchOptions options) {
	private static final SecureRandom RANDOM = new SecureRandom();

	public HolepunchRequest {
		Objects.requireNonNull(host, "host");
		if (host.isBlank()) {
			throw new IllegalArgumentException("host must not be blank");
		}
		if (port < 1 || port > 65535) {
			throw new IllegalArgumentException("port out of range");
		}
		if (protocolVersion < 0) {
			throw new IllegalArgumentException("protocolVersion must be non-negative");
		}
		Objects.requireNonNull(handler, "handler");
		Objects.requireNonNull(options, "options");
	}

	public static String defaultUsername() {
		String value = Long.toUnsignedString(RANDOM.nextLong(), 36);
		return value.substring(0, Math.min(16, value.length()));
	}

	public static String usernameOrDefault(HolepunchOptions options) {
		return options.username() == null ? defaultUsername() : options.username();
	}

	public static boolean isValidUsername(String username) {
		if (username == null || username.isEmpty() || username.length() > 16) {
			return false;
		}
		for (int index = 0; index < username.length(); index++) {
			char character = username.charAt(index);
			if (!(character >= 'A' && character <= 'Z') && !(character >= 'a' && character <= 'z') && !(character >= '0' && character <= '9') && character != '_') {
				return false;
			}
		}
		return true;
	}
}
