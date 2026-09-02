package pl.skidam.mcholepunch.internal.client;

import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchRequest;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.net.VanillaAddressResolver;

public final class NioHolepunchClient {
	private static final ExecutorService CONNECT_EXECUTOR = Executors.newCachedThreadPool(runnable -> {
		Thread thread = new Thread(runnable, "mcholepunch-connect");
		thread.setDaemon(true);
		return thread;
	});

	private final VanillaAddressResolver resolver;

	public NioHolepunchClient() {
		this(new VanillaAddressResolver());
	}

	NioHolepunchClient(VanillaAddressResolver resolver) {
		this.resolver = resolver;
	}

	public CompletionStage<HolepunchRoute> resolve(String host, int port) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				return resolver.resolve(host, port);
			} catch (Exception e) {
				throw new CompletionException(e);
			}
		}, CONNECT_EXECUTOR);
	}

	public CompletionStage<HolepunchConnection> connect(HolepunchRequest request) {
		return connect(request, null);
	}

	public CompletionStage<HolepunchConnection> connect(HolepunchRequest request, HolepunchRoute resolvedRoute) {
		CompletableFuture<HolepunchConnection> result = new CompletableFuture<>();
		long debugId = DebugLog.nextConnectionId();
		DebugLog.log("CLIENT", debugId, "connect requested host=%s port=%d protocol=%d maxPending=%d", request.host(), request.port(), request.protocolVersion(), request.options().maxPendingWriteBytes());
		CONNECT_EXECUTOR.execute(() -> {
			SocketChannel channel = null;
			try {
				HolepunchRoute route = resolvedRoute == null ? resolver.resolve(request.host(), request.port()) : resolvedRoute;
				DebugLog.log("CLIENT", debugId, "resolved address host=%s port=%d socket=%s", route.host(), route.port(), route.socketAddress());
				channel = SocketChannel.open();
				channel.configureBlocking(true);
				channel.socket().setTcpNoDelay(true);
				channel.socket().setKeepAlive(true);
				channel.socket().connect(route.socketAddress(), Math.toIntExact(request.options().connectTimeout().toMillis()));
				DebugLog.log("CLIENT", debugId, "TCP connected local=%s remote=%s", channel.getLocalAddress(), channel.getRemoteAddress());
				NioHolepunchConnection connection = NioHolepunchConnection.negotiate(debugId, channel, request, route);
				channel = null;
				DebugLog.log("CLIENT", debugId, "connect completed");
				result.complete(connection);
			} catch (Throwable throwable) {
				DebugLog.failure("CLIENT", debugId, "connect failed", throwable);
				if (channel != null) {
					try {
						channel.close();
					} catch (Exception ignored) {
					}
				}
				HolepunchFailure failure = throwable instanceof HolepunchFailure holepunchFailure ? holepunchFailure : new HolepunchFailure(HolepunchFailure.Kind.IO, "connection failed", throwable);
				result.completeExceptionally(failure);
			}
		});
		return result;
	}
}
