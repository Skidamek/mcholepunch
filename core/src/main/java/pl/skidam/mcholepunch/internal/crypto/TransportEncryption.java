package pl.skidam.mcholepunch.internal.crypto;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TransportEncryption {
	private final CipherStreamState decryptor;
	private final CipherStreamState encryptor;
	private final AtomicBoolean raw = new AtomicBoolean();

	public TransportEncryption(CipherStreamState initialDecryptor, CipherStreamState initialEncryptor) {
		this.decryptor = Objects.requireNonNull(initialDecryptor, "initialDecryptor");
		this.encryptor = Objects.requireNonNull(initialEncryptor, "initialEncryptor");
	}

	public static TransportEncryption server(byte[] secret) throws GeneralSecurityException {
		return new TransportEncryption(MinecraftCrypt.decryptor(secret), MinecraftCrypt.encryptor(secret));
	}

	public byte[] decrypt(byte[] input) {
		if (raw.get()) {
			return input.clone();
		}
		return decryptor.update(input);
	}

	public byte[] encrypt(byte[] input) {
		if (raw.get()) {
			return input.clone();
		}
		return encryptor.update(input);
	}

	public int decrypt(ByteBuffer input, ByteBuffer output) {
		if (raw.get()) {
			int bytes = input.remaining();
			output.put(input);
			return bytes;
		}
		return decryptor.update(input, output);
	}

	public int encrypt(ByteBuffer input, ByteBuffer output) {
		if (raw.get()) {
			int bytes = input.remaining();
			output.put(input);
			return bytes;
		}
		return encryptor.update(input, output);
	}

	public boolean isRaw() {
		return raw.get();
	}

	public void activateRaw() {
		if (raw.get()) {
			return;
		}
		raw.compareAndSet(false, true);
	}
}
