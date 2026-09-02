package pl.skidam.mcholepunch.internal;

import java.security.SecureRandom;

import pl.skidam.mcholepunch.HolepunchOptions;

/** Minecraft username rules for the identity the login negotiation presents. */
public final class Usernames {
	private static final int MAXIMUM_LENGTH = 16;
	private static final SecureRandom RANDOM = new SecureRandom();

	private Usernames() {}

	public static String defaultUsername() {
		String value = Long.toUnsignedString(RANDOM.nextLong(), 36);
		return value.substring(0, Math.min(MAXIMUM_LENGTH, value.length()));
	}

	public static String usernameOrDefault(HolepunchOptions options) {
		return options.username() == null ? defaultUsername() : options.username();
	}

	public static boolean isValid(String username) {
		if (username == null || username.isEmpty() || username.length() > MAXIMUM_LENGTH) {
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
