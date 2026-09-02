package pl.skidam.mcholepunch.internal.crypto;

import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public final class MinecraftCrypt {
	private static final SecureRandom RANDOM = new SecureRandom();

	private MinecraftCrypt() {}

	public static byte[] newSecret() throws GeneralSecurityException {
		KeyGenerator generator = KeyGenerator.getInstance("AES");
		generator.init(128, RANDOM);
		return generator.generateKey().getEncoded();
	}

	public static PublicKey decodePublicKey(byte[] encoded) throws GeneralSecurityException {
		return KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(encoded));
	}

	public static byte[] rsaEncrypt(PublicKey key, byte[] value) throws GeneralSecurityException {
		Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
		cipher.init(Cipher.ENCRYPT_MODE, key);
		return cipher.doFinal(value);
	}

	public static byte[] rsaDecrypt(PrivateKey key, byte[] value) throws GeneralSecurityException {
		Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
		cipher.init(Cipher.DECRYPT_MODE, key);
		return cipher.doFinal(value);
	}

	public static byte[] decryptLoginSecret(PrivateKey key, byte[] encryptedSecret, byte[] encryptedChallenge, byte[] expectedChallenge) throws GeneralSecurityException {
		try {
			byte[] secret = rsaDecrypt(key, encryptedSecret);
			byte[] challenge = rsaDecrypt(key, encryptedChallenge);
			if (secret.length != 16 || !constantTimeEquals(challenge, expectedChallenge)) {
				throw new GeneralSecurityException("invalid login encryption response");
			}
			return secret.clone();
		} catch (GeneralSecurityException exception) {
			throw new GeneralSecurityException("invalid login encryption response", exception);
		}
	}

	public static CipherStreamState encryptor(byte[] secret) throws GeneralSecurityException {
		return CipherStreamState.create(Cipher.ENCRYPT_MODE, secret);
	}

	public static CipherStreamState decryptor(byte[] secret) throws GeneralSecurityException {
		return CipherStreamState.create(Cipher.DECRYPT_MODE, secret);
	}

	public static boolean constantTimeEquals(byte[] left, byte[] right) {
		return MessageDigest.isEqual(left, right);
	}

	public static SecretKey secretKey(byte[] secret) {
		if (secret.length != 16) {
			throw new IllegalArgumentException("Minecraft AES key must be 16 bytes");
		}
		return new SecretKeySpec(secret.clone(), "AES");
	}
}
