package pl.skidam.mcholepunch.internal.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchRequest;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.concurrent.SerialExecutor;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.crypto.TransportEncryption;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameReader;
import pl.skidam.mcholepunch.internal.transport.TransportHandoff;

final class NioHolepunchConnection implements HolepunchConnection {
	private static final int TRANSFER_BUFFER_SIZE = 128 * 1024;
	private final long debugId;
	private final SocketChannel channel;
	private final HolepunchHandler handler;
	private final HolepunchRequest request;
	private final Executor callbackExecutor;
	private final TransportEncryption encryption;
	private final MinecraftFrameReader frameReader;
	private final TransportFrameReader transportReader;
	private final ExecutorService writerExecutor;
	private final ExecutorService readerExecutor;
	private final Queue<WriteRequest> writes = new ArrayDeque<>();
	private final AtomicLong pendingBytes = new AtomicLong();
	private final AtomicBoolean closed = new AtomicBoolean();
	private final AtomicBoolean openSucceeded = new AtomicBoolean();
	private final AtomicLong receivedBytes = new AtomicLong();
	private final AtomicLong sentBytes = new AtomicLong();
	private final Object readFlow = new Object();
	private volatile boolean takeoverComplete;
	private boolean readsPaused;
	private final ScheduledFuture<?> watchdog;
	private final ByteArrayOutputStream initialData = new ByteArrayOutputStream();
	private byte[] initialDataSnapshot = new byte[0];
	private ByteBuffer encryptedOutput = ByteBuffer.allocate(8192);
	private final TransportHandoff handoff = new TransportHandoff();
	private CompletableFuture<Void> prepareResult;
	private CompletableFuture<Void> commitResult;

	private NioHolepunchConnection(long debugId, SocketChannel channel, HolepunchRequest request, ClientNegotiation.Result result) throws GeneralSecurityException {
		this.debugId = debugId;
		this.channel = channel;
		this.handler = request.handler();
		this.request = request;
		this.callbackExecutor = new SerialExecutor(request.options().callbackExecutor());
		this.encryption = new TransportEncryption(result.decryptor(), MinecraftCrypt.encryptor(result.secret()));
		this.frameReader = result.frameReader();
		this.transportReader = new TransportFrameReader(maximumFrameLength(request.options().maxPendingWriteBytes()));
		this.writerExecutor = Executors.newSingleThreadExecutor(runnable -> daemon(runnable, "mcholepunch-writer"));
		this.readerExecutor = Executors.newSingleThreadExecutor(runnable -> daemon(runnable, "mcholepunch-reader"));
		this.watchdog = DebugLog.watchdog("CLIENT", debugId, this::debugState);
	}

	static NioHolepunchConnection negotiate(long debugId, SocketChannel channel, HolepunchRequest request, HolepunchRoute address) throws Exception {
		ClientNegotiation.Result result = ClientNegotiation.negotiate(debugId, channel, request, address);
		NioHolepunchConnection connection = new NioHolepunchConnection(debugId, channel, request, result);
		DebugLog.log("CLIENT", debugId, "waiting for takeover query");
		connection.awaitTakeover(result.deadline());
		channel.configureBlocking(true);
		DebugLog.log("CLIENT", debugId, "takeover complete; switching to coordinated transport initialBytes=%d", connection.initialDataSnapshot.length);
		connection.dispatchOpen();
		connection.dispatchInitialData();
		connection.writerExecutor.execute(connection::writeLoop);
		connection.readerExecutor.execute(connection::readLoop);
		return connection;
	}

	private void awaitTakeover(long deadline) throws Exception {
		ByteBuffer network = ByteBuffer.allocate(8192);
		while (System.nanoTime() < deadline) {
			network.clear();
			int count = channel.read(network);
			if (count < 0) {
				throw new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "server closed during takeover", null);
			}
			if (count == 0) {
				Thread.sleep(1);
				continue;
			}
			network.flip();
			DebugLog.log("CLIENT", debugId, "takeover TCP read encryptedBytes=%d", count);
			ByteBuffer plaintext = ByteBuffer.wrap(encryption.decrypt(ClientNegotiation.readRemaining(network)));
			for (ByteBuffer frame : frameReader.feed(plaintext, 1)) {
				try {
					LoginCodec.readTakeoverQuery(frame);
					DebugLog.log("CLIENT", debugId, "received takeover query frameBytes=%d", frame.position());
				} catch (IOException exception) {
					throw new HolepunchFailure(HolepunchFailure.Kind.UNEXPECTED_PACKET, "unexpected packet during takeover", exception);
				}
				ByteBuffer trailing = frameReader.drainPending();
				if (trailing.hasRemaining()) {
					processTransportPlaintext(trailing);
				}
				takeoverComplete = true;
				initialDataSnapshot = initialData.toByteArray();
				return;
			}
		}
		throw new HolepunchFailure(HolepunchFailure.Kind.TIMEOUT, "takeover timed out", null);
	}

	private void dispatchOpen() {
		callbackExecutor.execute(() -> {
			try {
				DebugLog.log("CLIENT", debugId, "dispatch onOpen");
				handler.onOpen(this);
				openSucceeded.set(true);
				DebugLog.log("CLIENT", debugId, "onOpen completed");
			} catch (Throwable throwable) {
				fail(new HolepunchFailure(HolepunchFailure.Kind.IO, "onOpen failed", throwable));
			}
		});
	}

	private void dispatchInitialData() {
		if (initialDataSnapshot.length != 0) {
			dispatchRead(ByteBuffer.wrap(initialDataSnapshot));
			initialDataSnapshot = new byte[0];
		}
	}

	@Override
	public boolean isRaw() {
		return handoff.isRaw();
	}

	@Override
	public CompletionStage<Void> write(ByteBuffer data) {
		if (data == null) {
			return CompletableFuture.failedFuture(new NullPointerException("data"));
		}
		ByteBuffer copy = data.duplicate();
		int size = copy.remaining();
		if (size == 0) {
			return CompletableFuture.completedFuture(null);
		}
		while (true) {
			long current = pendingBytes.get();
			long next = current + size;
			if (next > request.options().maxPendingWriteBytes()) {
				return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.IO, "pending write limit exceeded", null));
			}
			if (pendingBytes.compareAndSet(current, next)) {
				break;
			}
		}
		CompletableFuture<Void> result = new CompletableFuture<>();
		synchronized (writes) {
			if (closed.get()) {
				pendingBytes.addAndGet(-size);
				result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
			} else {
				boolean blocked = handoff.blocksData();
				boolean rawEra = handoff.isRaw();
				writes.add(new WriteRequest(copy, size, result, blocked, rawEra, Kind.DATA));
				DebugLog.log("CLIENT", debugId, "write queued queueDepth=%d pending=%d blocked=%s rawEra=%s", writes.size(), pendingBytes.get(), blocked, rawEra);
				writes.notifyAll();
			}
		}
		return result;
	}

	@Override
	public CompletionStage<Void> prepareTransportUpgrade() {
		TransportHandoff.Transition transition;
		CompletableFuture<Void> result;
		synchronized (writes) {
			if (closed.get()) {
				return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
			}
			if (handoff.isRaw()) {
				return CompletableFuture.completedFuture(null);
			}
			if (prepareResult != null) {
				return prepareResult;
			}
			prepareResult = new CompletableFuture<>();
			result = prepareResult;
			transition = handoff.requestPrepare();
			applyTransition(transition);
		}
		completeTransition(transition);
		return result;
	}

	@Override
	public CompletionStage<Void> commitTransportUpgrade() {
		TransportHandoff.Transition transition;
		CompletableFuture<Void> result;
		synchronized (writes) {
			if (closed.get()) {
				return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
			}
			if (handoff.isRaw()) {
				return CompletableFuture.completedFuture(null);
			}
			if (commitResult != null) {
				return commitResult;
			}
			if (!handoff.isPrepared()) {
				return CompletableFuture.failedFuture(new IllegalStateException("transport handoff is not prepared"));
			}
			commitResult = new CompletableFuture<>();
			result = commitResult;
			transition = handoff.requestCommit();
			applyTransition(transition);
		}
		completeTransition(transition);
		return result;
	}

	private void receiveControl(TransportFrame.Type type) throws IOException {
		TransportHandoff.Transition transition;
		synchronized (writes) {
			if (closed.get() || handoff.isRaw()) {
				return;
			}
			try {
				transition = handoff.onPeerControlReceived(type);
			} catch (IllegalStateException exception) {
				throw new IOException("invalid transport handoff control " + type, exception);
			}
			applyTransition(transition);
		}
		completeTransition(transition);
	}

	private void writeLoop() {
		try {
			while (!closed.get()) {
				WriteRequest write = takeWrite();
				if (write == null) {
					return;
				}
				try {
					ByteBuffer frame = write.kind() == Kind.DATA && write.rawEra() ? write.data().duplicate() : switch (write.kind()) {
						case DATA -> TransportFrameCodec.data(write.data());
						case READY -> TransportFrameCodec.ready();
						case ACK -> TransportFrameCodec.ack();
						case COMMIT -> TransportFrameCodec.commit();
					};
					ensureEncryptedCapacity(frame.remaining());
					encryptedOutput.clear();
					int encryptedBytes = encryption.encrypt(frame, encryptedOutput);
					encryptedOutput.flip();
					DebugLog.log("CLIENT", debugId, "writer sending kind=%s plaintext=%d wire=%d rawEra=%s", write.kind(), frame.limit(), encryptedBytes, write.rawEra());
					ClientNegotiation.writeFully(channel, encryptedOutput);
					if (write.kind() == Kind.DATA) {
						long total = sentBytes.addAndGet(write.size());
						long pending = pendingBytes.addAndGet(-write.size());
						DebugLog.log("CLIENT", debugId, "write complete plaintext=%d totalSent=%d pending=%d", write.size(), total, pending);
					}
					TransportHandoff.Transition transition = null;
					if (write.kind() != Kind.DATA) {
						synchronized (writes) {
							try {
								transition = handoff.onLocalControlSent(toFrameType(write.kind()));
								applyTransition(transition);
							} catch (IllegalStateException exception) {
								throw new IOException("invalid local transport handoff control " + write.kind(), exception);
							}
						}
						completeTransition(transition);
					}
					write.result().complete(null);
				} catch (Throwable throwable) {
					if (write.kind() == Kind.DATA) {
						pendingBytes.addAndGet(-write.size());
					}
					write.result().completeExceptionally(throwable);
					fail(new HolepunchFailure(HolepunchFailure.Kind.IO, "write failed", throwable));
					return;
				}
			}
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
		}
	}

	private WriteRequest takeWrite() throws InterruptedException {
		synchronized (writes) {
			while (!closed.get()) {
				for (Iterator<WriteRequest> iterator = writes.iterator(); iterator.hasNext();) {
					WriteRequest write = iterator.next();
					if (write.kind() != Kind.DATA || !write.blocked() || encryption.isRaw()) {
						iterator.remove();
						return write;
					}
				}
				writes.wait();
			}
			return null;
		}
	}

	private void readLoop() {
		ByteBuffer network = ByteBuffer.allocate(TRANSFER_BUFFER_SIZE);
		ByteBuffer plaintext = ByteBuffer.allocate(TRANSFER_BUFFER_SIZE);
		try {
			while (!closed.get()) {
				awaitReadsEnabled();
				network.clear();
				int count = channel.read(network);
				if (count < 0) {
					fail(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "server closed connection", null));
					return;
				}
				if (count == 0) {
					continue;
				}
				network.flip();
				while (network.hasRemaining()) {
					if (encryption.isRaw()) {
						ByteBuffer raw = network.slice();
						network.position(network.limit());
						awaitRead(dispatchRead(raw, true));
						continue;
					}
					int encryptedBytes = transportReader.maximumInputBytes(network.remaining());
					plaintext.clear();
					plaintext.limit(encryptedBytes);
					ByteBuffer encrypted = network.slice();
					encrypted.limit(encryptedBytes);
					int plaintextBytes = encryption.decrypt(encrypted, plaintext);
					network.position(network.position() + encryptedBytes);
					if (plaintextBytes != encryptedBytes) {
						throw new IOException("transport cipher changed length from " + encryptedBytes + " to " + plaintextBytes);
					}
					plaintext.flip();
					processTransportPlaintext(plaintext);
				}
			}
		} catch (Throwable throwable) {
			fail(throwable instanceof HolepunchFailure failure ? failure : new HolepunchFailure(HolepunchFailure.Kind.IO, "read failed", throwable));
		}
	}

	private void processTransportPlaintext(ByteBuffer plaintext) throws IOException {
		CompletionStage<Void> delivered = null;
		List<TransportFrame> frames = transportReader.feed(plaintext);
		for (TransportFrame frame : frames) {
			switch (frame.type()) {
				case DATA -> delivered = handleData(frame.payload());
				case READY, ACK, COMMIT -> receiveControl(frame.type());
			}
		}
		if (delivered != null) {
			awaitRead(delivered);
		}
	}

	private CompletionStage<Void> handleData(byte[] bytes) {
		long total = receivedBytes.addAndGet(bytes.length);
		if (!takeoverComplete) {
			initialData.writeBytes(bytes);
			DebugLog.log("CLIENT", debugId, "buffering initial application bytes=%d totalReceived=%d", bytes.length, total);
			return null;
		}
		DebugLog.log("CLIENT", debugId, "received application bytes=%d totalReceived=%d %s", bytes.length, total, DebugLog.preview(bytes));
		return dispatchRead(ByteBuffer.wrap(bytes));
	}

	private CompletionStage<Void> dispatchRead(ByteBuffer data) {
		return dispatchRead(data, false);
	}

	private CompletionStage<Void> dispatchRead(ByteBuffer data, boolean rawEra) {
		if (!data.hasRemaining()) {
			return CompletableFuture.completedFuture(null);
		}
		ByteBuffer copy = ByteBuffer.allocate(data.remaining());
		copy.put(data).flip();
		CompletableFuture<Void> completion = new CompletableFuture<>();
		try {
			callbackExecutor.execute(() -> {
				if (!openSucceeded.get()) {
					DebugLog.log("CLIENT", debugId, "dropping read because onOpen has not completed %s", DebugLog.preview(copy));
					completion.complete(null);
					return;
				}
				try {
					if (rawEra) {
						DebugLog.log("CLIENT", debugId, "dispatch onRawRead %s", DebugLog.preview(copy));
						handler.onRawRead(copy.asReadOnlyBuffer());
					} else {
						DebugLog.log("CLIENT", debugId, "dispatch onRead %s", DebugLog.preview(copy));
						handler.onRead(copy.asReadOnlyBuffer());
					}
					completion.complete(null);
				} catch (Throwable throwable) {
					HolepunchFailure failure = new HolepunchFailure(HolepunchFailure.Kind.IO, rawEra ? "onRawRead failed" : "onRead failed", throwable);
					completion.completeExceptionally(failure);
					fail(failure);
				}
			});
		} catch (Throwable throwable) {
			completion.completeExceptionally(throwable);
			fail(new HolepunchFailure(HolepunchFailure.Kind.IO, "read scheduling failed", throwable));
		}
		return completion;
	}

	private void awaitReadsEnabled() throws InterruptedException {
		synchronized (readFlow) {
			while (!closed.get() && readsPaused) {
				readFlow.wait();
			}
		}
	}

	private static void awaitRead(CompletionStage<Void> completion) throws IOException {
		try {
			completion.toCompletableFuture().get();
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new IOException("read callback interrupted", exception);
		} catch (Exception exception) {
			Throwable cause = exception.getCause() == null ? exception : exception.getCause();
			if (cause instanceof IOException ioException) {
				throw ioException;
			}
			throw new IOException("read callback failed", cause);
		}
	}

	@Override
	public void pauseReads() {
		synchronized (readFlow) {
			readsPaused = true;
		}
	}

	@Override
	public void resumeReads() {
		synchronized (readFlow) {
			if (readsPaused) {
				readsPaused = false;
				readFlow.notifyAll();
			}
		}
	}

	private void applyTransition(TransportHandoff.Transition transition) {
		for (TransportFrame.Type type : transition.outbound()) {
			writes.add(new WriteRequest(null, 0, new CompletableFuture<>(), false, false, fromFrameType(type)));
		}
		if (!transition.outbound().isEmpty()) {
			writes.notifyAll();
		}
		if (transition.activatesRaw()) {
			encryption.activateRaw();
			writes.notifyAll();
			DebugLog.log("CLIENT", debugId, "transport switched to raw pass-through");
		}
	}

	private void completeTransition(TransportHandoff.Transition transition) {
		if (transition.prepared() && prepareResult != null) {
			prepareResult.complete(null);
		}
		if (transition.committed() && commitResult != null) {
			commitResult.complete(null);
		}
	}

	private static TransportFrame.Type toFrameType(Kind kind) {
		return switch (kind) {
			case READY -> TransportFrame.Type.READY;
			case ACK -> TransportFrame.Type.ACK;
			case COMMIT -> TransportFrame.Type.COMMIT;
			case DATA -> throw new IllegalArgumentException("DATA is not a handoff control frame");
		};
	}

	private static Kind fromFrameType(TransportFrame.Type type) {
		return switch (type) {
			case READY -> Kind.READY;
			case ACK -> Kind.ACK;
			case COMMIT -> Kind.COMMIT;
			case DATA -> throw new IllegalArgumentException("DATA is not a handoff control frame");
		};
	}

	@Override
	public void close() {
		fail(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
	}

	private void fail(HolepunchFailure failure) {
		if (!closed.compareAndSet(false, true)) {
			DebugLog.log("CLIENT", debugId, "ignoring duplicate close: %s", failure);
			return;
		}
		DebugLog.failure("CLIENT", debugId, "closing connection kind=" + failure.kind(), failure);
		DebugLog.cancel(watchdog);
		synchronized (readFlow) {
			readFlow.notifyAll();
		}
		try {
			channel.close();
		} catch (IOException exception) {
			failure.addSuppressed(exception);
		}
		synchronized (writes) {
			WriteRequest write;
			while ((write = writes.poll()) != null) {
				if (write.kind() == Kind.DATA) {
					pendingBytes.addAndGet(-write.size());
				}
				write.result().completeExceptionally(failure);
			}
			if (prepareResult != null) prepareResult.completeExceptionally(failure);
			if (commitResult != null) commitResult.completeExceptionally(failure);
			writes.notifyAll();
		}
		writerExecutor.shutdownNow();
		readerExecutor.shutdownNow();
		callbackExecutor.execute(() -> handler.onClosed(failure));
	}

	private String debugState() {
		int queueDepth;
		synchronized (writes) {
			queueDepth = writes.size();
		}
		boolean paused;
		synchronized (readFlow) {
			paused = readsPaused;
		}
		return String.format(java.util.Locale.ROOT, "open=%s connected=%s closed=%s openCallback=%s phase=%s raw=%s readsPaused=%s pending=%d queue=%d sent=%d received=%d " + "writerShutdown=%s readerShutdown=%s",
				channel.isOpen(),
				channel.isConnected(), closed.get(), openSucceeded.get(), handoff.phase(), encryption.isRaw(), paused, pendingBytes.get(), queueDepth, sentBytes.get(), receivedBytes.get(), writerExecutor.isShutdown(),
				readerExecutor.isShutdown());
	}

	private static Thread daemon(Runnable runnable, String name) {
		Thread thread = new Thread(runnable, name);
		thread.setDaemon(true);
		return thread;
	}

	private void ensureEncryptedCapacity(int requiredBytes) {
		if (encryptedOutput.capacity() < requiredBytes) {
			encryptedOutput = ByteBuffer.allocate(requiredBytes);
		}
	}

	private static int maximumFrameLength(long maximumPendingBytes) {
		return Math.toIntExact(Math.min(Integer.MAX_VALUE, maximumPendingBytes));
	}

	private enum Kind {
		DATA,
		READY,
		ACK,
		COMMIT
	}

	private record WriteRequest(ByteBuffer data, int size, CompletableFuture<Void> result, boolean blocked, boolean rawEra, Kind kind) {}
}
