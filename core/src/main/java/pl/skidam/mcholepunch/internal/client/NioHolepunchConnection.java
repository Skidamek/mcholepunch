package pl.skidam.mcholepunch.internal.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.HolepunchRequest;
import pl.skidam.mcholepunch.internal.concurrent.SerialExecutor;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.crypto.TransportEncryption;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameReader;
import pl.skidam.mcholepunch.internal.transport.TransportFeed;
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
	private final TransportFeed transportFeed;
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
	private CompletableFuture<Void> commitResult;

	private NioHolepunchConnection(long debugId, SocketChannel channel, HolepunchRequest request, ClientNegotiation.Result result) throws GeneralSecurityException {
		this.debugId = debugId;
		this.channel = channel;
		this.handler = request.handler();
		this.request = request;
		this.callbackExecutor = new SerialExecutor(request.options().callbackExecutor());
		this.encryption = new TransportEncryption(result.decryptor(), MinecraftCrypt.encryptor(result.secret()));
		this.frameReader = result.frameReader();
		this.transportFeed = new TransportFeed(encryption, new TransportFrameReader(TransportFrame.MAX_FRAME_BYTES));
		this.writerExecutor = Executors.newSingleThreadExecutor(runnable -> daemon(runnable, "mcholepunch-writer"));
		this.readerExecutor = Executors.newSingleThreadExecutor(runnable -> daemon(runnable, "mcholepunch-reader"));
		this.watchdog = DebugLog.watchdog("CLIENT", debugId, this::debugState);
	}

	static NioHolepunchConnection negotiate(long debugId, SocketChannel channel, HolepunchRequest request, HolepunchRoute address) throws Exception {
		ClientNegotiation.Result result = ClientNegotiation.negotiate(debugId, channel, request, address);
		NioHolepunchConnection connection = new NioHolepunchConnection(debugId, channel, request, result);
		DebugLog.log("CLIENT", debugId, "waiting for takeover query");
		connection.awaitTakeover(result.deadline());
		channel.socket().setSoTimeout(0);
		DebugLog.log("CLIENT", debugId, "takeover complete; switching to coordinated transport initialBytes=%d", connection.initialDataSnapshot.length);
		connection.dispatchOpen();
		connection.dispatchInitialData();
		connection.writerExecutor.execute(connection::writeLoop);
		connection.readerExecutor.execute(connection::readLoop);
		return connection;
	}

	private void awaitTakeover(long deadline) throws Exception {
		ByteBuffer network = ByteBuffer.allocate(8192);
		channel.socket().setSoTimeout(remainingMillis(deadline));
		while (System.nanoTime() < deadline) {
			network.clear();
			int count;
			try {
				count = channel.read(network);
			} catch (SocketTimeoutException exception) {
				break;
			}
			if (count < 0) {
				throw new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "server closed during takeover", null);
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

	private static int remainingMillis(long deadline) {
		return Math.toIntExact(Math.min(Integer.MAX_VALUE, TimeUnit.NANOSECONDS.toMillis(Math.max(1, deadline - System.nanoTime()))));
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
			} else if (handoff.isCommitting()) {
				pendingBytes.addAndGet(-size);
				result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.IO, "transport commit in flight", null));
			} else {
				boolean rawEra = handoff.isRaw();
				writes.add(new WriteRequest(copy, size, result, rawEra, TransportFrame.Type.DATA));
				DebugLog.log("CLIENT", debugId, "write queued queueDepth=%d pending=%d rawEra=%s", writes.size(), pendingBytes.get(), rawEra);
				writes.notifyAll();
			}
		}
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
			commitResult = new CompletableFuture<>();
			result = commitResult;
			transition = handoff.requestCommit();
			applyTransition(transition);
		}
		completeTransition(transition);
		return result;
	}

	private void receiveControl(TransportFrame.Type type) {
		TransportHandoff.Transition transition;
		synchronized (writes) {
			if (closed.get() || handoff.isRaw()) {
				return;
			}
			transition = handoff.onPeerControlReceived(type);
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
					ByteBuffer frame = write.kind() == TransportFrame.Type.DATA && write.rawEra() ? write.data().duplicate() : switch (write.kind()) {
						case DATA -> TransportFrameCodec.data(write.data());
						case COMMIT -> TransportFrameCodec.commit();
					};
					ensureEncryptedCapacity(frame.remaining());
					encryptedOutput.clear();
					int encryptedBytes = encryption.encrypt(frame, encryptedOutput);
					encryptedOutput.flip();
					DebugLog.log("CLIENT", debugId, "writer sending kind=%s plaintext=%d wire=%d rawEra=%s", write.kind(), frame.limit(), encryptedBytes, write.rawEra());
					ClientNegotiation.writeFully(channel, encryptedOutput);
					if (write.kind() == TransportFrame.Type.DATA) {
						long total = sentBytes.addAndGet(write.size());
						long pending = pendingBytes.addAndGet(-write.size());
						DebugLog.log("CLIENT", debugId, "write complete plaintext=%d totalSent=%d pending=%d", write.size(), total, pending);
					}
					if (write.kind() != TransportFrame.Type.DATA) {
						TransportHandoff.Transition transition;
						synchronized (writes) {
							transition = handoff.onLocalControlSent(write.kind());
							applyTransition(transition);
						}
						completeTransition(transition);
					}
					write.result().complete(null);
				} catch (Throwable throwable) {
					if (write.kind() == TransportFrame.Type.DATA) {
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
			while (!closed.get() && writes.isEmpty()) {
				writes.wait();
			}
			return closed.get() ? null : writes.poll();
		}
	}

	private void readLoop() {
		ByteBuffer network = ByteBuffer.allocate(TRANSFER_BUFFER_SIZE);
		try {
			while (!closed.get()) {
				awaitReadsEnabled();
				network.clear();
				int count = channel.read(network);
				if (count < 0) {
					fail(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "server closed connection", null));
					return;
				}
				network.flip();
				while (network.hasRemaining()) {
					if (transportFeed.isRaw()) {
						ByteBuffer raw = network.slice();
						network.position(network.limit());
						awaitRead(dispatchRead(raw, true));
						continue;
					}
					processTransportPlaintext(network);
				}
			}
		} catch (Throwable throwable) {
			fail(throwable instanceof HolepunchFailure failure ? failure : new HolepunchFailure(HolepunchFailure.Kind.IO, "read failed", throwable));
		}
	}

	private void processTransportPlaintext(ByteBuffer encryptedInput) throws IOException {
		CompletionStage<Void> delivered = null;
		List<TransportFrame> frames = transportFeed.feed(encryptedInput);
		for (TransportFrame frame : frames) {
			switch (frame.type()) {
				case DATA -> delivered = handleData(frame.payload());
				case COMMIT -> receiveControl(frame.type());
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
			writes.add(new WriteRequest(null, 0, new CompletableFuture<>(), false, type));
		}
		if (!transition.outbound().isEmpty()) {
			writes.notifyAll();
		}
		if (transition.activatedRaw()) {
			encryption.activateRaw();
			writes.notifyAll();
			DebugLog.log("CLIENT", debugId, "transport switched to raw pass-through");
		}
	}

	private void completeTransition(TransportHandoff.Transition transition) {
		if (transition.activatedRaw() && commitResult != null) {
			commitResult.complete(null);
		}
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
				if (write.kind() == TransportFrame.Type.DATA) {
					pendingBytes.addAndGet(-write.size());
				}
				write.result().completeExceptionally(failure);
			}
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
		return String.format(Locale.ROOT, "open=%s connected=%s closed=%s openCallback=%s phase=%s raw=%s readsPaused=%s pending=%d queue=%d sent=%d received=%d " + "writerShutdown=%s readerShutdown=%s",
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

	private record WriteRequest(ByteBuffer data, int size, CompletableFuture<Void> result, boolean rawEra, TransportFrame.Type kind) {}
}
