package pl.skidam.mcholepunch.server.netty;

import java.util.List;

/**
 * Names the Netty pipeline handlers around a holepunch takeover: where the vanilla cipher handlers
 * anchor, what names the takeover installs under, and which vanilla handlers to strip once it
 * lands. Minecraft pipelines only need {@link #minecraftLogin()}; the long constructor exists for
 * embedding into a differently shaped pipeline.
 */
public record NettyTakeoverSpec(String inboundEncryptionAnchor, String outboundEncryptionAnchor, List<String> removalNames) {
	public NettyTakeoverSpec {
		removalNames = List.copyOf(removalNames);
	}

	public static NettyTakeoverSpec minecraftLogin() {
		return new NettyTakeoverSpec("splitter", "prepender", List.of("timeout", "splitter", "flow_control", "decoder", "prepender", "encoder", "hackfix", "packet_handler", "unbundler", "bundler"));
	}

	public String rawInboundName() {
		return "mcholepunch-raw-inbound";
	}

	public String rawOutboundName() {
		return "mcholepunch-raw-outbound";
	}

	public String cipherDecoderName() {
		return "mcholepunch-cipher-decoder";
	}

	public String cipherEncoderName() {
		return "mcholepunch-cipher-encoder";
	}

	public String negotiatorName() {
		return "mcholepunch-login-negotiator";
	}

	/** Whether the takeover installs under this name, so the strip pass never removes its own handlers. */
	public boolean owns(String handlerName) {
		return rawInboundName().equals(handlerName) || rawOutboundName().equals(handlerName) || cipherDecoderName().equals(handlerName) || cipherEncoderName().equals(handlerName);
	}
}
