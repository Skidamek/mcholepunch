package pl.skidam.mcholepunch.internal.crypto;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;

public final class CipherStreamState {
	private final Cipher cipher;

	private CipherStreamState(Cipher cipher) {
		this.cipher = cipher;
	}

	static CipherStreamState create(int mode, byte[] secret) throws GeneralSecurityException {
		Cipher cipher = Cipher.getInstance("AES/CFB8/NoPadding");
		IvParameterSpec iv = new IvParameterSpec(secret);
		cipher.init(mode, MinecraftCrypt.secretKey(secret), iv);
		return new CipherStreamState(cipher);
	}

	public byte[] update(byte[] input) {
		return cipher.update(input);
	}

	public int update(ByteBuffer input, ByteBuffer output) {
		try {
			return cipher.update(input, output);
		} catch (ShortBufferException exception) {
			throw new IllegalStateException("cipher output buffer is too small", exception);
		}
	}
}
