package pl.skidam.mcholepunch.internal.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;

import org.junit.jupiter.api.Test;

class MinecraftCryptTest {
	@Test
	void rsaAndContinuousAesMatch() throws Exception {
		KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(1024);
		KeyPair pair = generator.generateKeyPair();
		byte[] secret = MinecraftCrypt.newSecret();
		byte[] encrypted = MinecraftCrypt.rsaEncrypt(pair.getPublic(), secret);
		assertArrayEquals(secret, MinecraftCrypt.rsaDecrypt(pair.getPrivate(), encrypted));

		byte[] first = "first".getBytes(StandardCharsets.UTF_8);
		byte[] second = "second".getBytes(StandardCharsets.UTF_8);
		CipherStreamState encryptor = MinecraftCrypt.encryptor(secret);
		CipherStreamState decryptor = MinecraftCrypt.decryptor(secret);
		assertArrayEquals(first, decryptor.update(encryptor.update(first)));
		assertArrayEquals(second, decryptor.update(encryptor.update(second)));
	}

	@Test
	void byteBufferCipherUpdatePreservesContinuousState() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		byte[] first = "first".getBytes(StandardCharsets.UTF_8);
		byte[] second = "second".getBytes(StandardCharsets.UTF_8);
		CipherStreamState encryptor = MinecraftCrypt.encryptor(secret);
		CipherStreamState decryptor = MinecraftCrypt.decryptor(secret);
		ByteBuffer encryptedFirst = ByteBuffer.allocate(first.length);
		ByteBuffer encryptedSecond = ByteBuffer.allocate(second.length);
		encryptor.update(ByteBuffer.wrap(first), encryptedFirst);
		encryptor.update(ByteBuffer.wrap(second), encryptedSecond);
		encryptedFirst.flip();
		encryptedSecond.flip();
		ByteBuffer decryptedFirst = ByteBuffer.allocate(first.length);
		ByteBuffer decryptedSecond = ByteBuffer.allocate(second.length);
		decryptor.update(encryptedFirst, decryptedFirst);
		decryptor.update(encryptedSecond, decryptedSecond);
		decryptedFirst.flip();
		decryptedSecond.flip();
		assertArrayEquals(first, readBytes(decryptedFirst));
		assertArrayEquals(second, readBytes(decryptedSecond));
	}

	@Test
	void decryptsAndValidatesLoginSecret() throws Exception {
		KeyPair pair = keyPair();
		byte[] secret = MinecraftCrypt.newSecret();
		byte[] challenge = {1, 2, 3, 4};

		byte[] decrypted = MinecraftCrypt.decryptLoginSecret(pair.getPrivate(), MinecraftCrypt.rsaEncrypt(pair.getPublic(), secret), MinecraftCrypt.rsaEncrypt(pair.getPublic(), challenge), challenge);

		assertArrayEquals(secret, decrypted);
		decrypted[0] ^= 1;
		assertArrayEquals(secret, MinecraftCrypt.decryptLoginSecret(pair.getPrivate(), MinecraftCrypt.rsaEncrypt(pair.getPublic(), secret), MinecraftCrypt.rsaEncrypt(pair.getPublic(), challenge), challenge));
	}

	@Test
	void rejectsInvalidLoginEncryptionResponses() throws Exception {
		KeyPair pair = keyPair();
		byte[] challenge = {1, 2, 3, 4};

		assertThrows(GeneralSecurityException.class,
				() -> MinecraftCrypt.decryptLoginSecret(pair.getPrivate(), MinecraftCrypt.rsaEncrypt(pair.getPublic(), new byte[15]), MinecraftCrypt.rsaEncrypt(pair.getPublic(), challenge), challenge));
		assertThrows(GeneralSecurityException.class,
				() -> MinecraftCrypt.decryptLoginSecret(pair.getPrivate(), MinecraftCrypt.rsaEncrypt(pair.getPublic(), new byte[16]), MinecraftCrypt.rsaEncrypt(pair.getPublic(), new byte[]{4, 3, 2, 1}), challenge));
		assertThrows(GeneralSecurityException.class, () -> MinecraftCrypt.decryptLoginSecret(pair.getPrivate(), new byte[]{1}, new byte[]{2}, challenge));
	}

	private static KeyPair keyPair() throws GeneralSecurityException {
		KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(1024);
		return generator.generateKeyPair();
	}

	private static byte[] readBytes(ByteBuffer buffer) {
		byte[] result = new byte[buffer.remaining()];
		buffer.get(result);
		return result;
	}
}
