package pl.skidam.mcholepunch.server.netty;

import java.util.List;
import java.util.Objects;

public final class NettyTakeoverSpec {
	private final String inboundEncryptionAnchor;
	private final String outboundEncryptionAnchor;
	private final String rawInboundName;
	private final String rawOutboundName;
	private final String cipherDecoderName;
	private final String cipherEncoderName;
	private final String negotiatorName;
	private final List<String> removalNames;

	private NettyTakeoverSpec(Builder builder) {
		inboundEncryptionAnchor = builder.inboundEncryptionAnchor;
		outboundEncryptionAnchor = builder.outboundEncryptionAnchor;
		rawInboundName = builder.rawInboundName;
		rawOutboundName = builder.rawOutboundName;
		cipherDecoderName = builder.cipherDecoderName;
		cipherEncoderName = builder.cipherEncoderName;
		negotiatorName = builder.negotiatorName;
		removalNames = List.copyOf(builder.removalNames);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static NettyTakeoverSpec minecraftLogin() {
		return builder().encryptionAnchors("splitter", "prepender").removalNames(List.of("timeout", "splitter", "flow_control", "decoder", "prepender", "encoder", "hackfix", "packet_handler", "unbundler", "bundler"))
				.build();
	}

	public String inboundEncryptionAnchor() {
		return inboundEncryptionAnchor;
	}

	public String outboundEncryptionAnchor() {
		return outboundEncryptionAnchor;
	}

	public String rawInboundName() {
		return rawInboundName;
	}

	public String rawOutboundName() {
		return rawOutboundName;
	}

	public String cipherDecoderName() {
		return cipherDecoderName;
	}

	public String cipherEncoderName() {
		return cipherEncoderName;
	}

	public String negotiatorName() {
		return negotiatorName;
	}

	public List<String> removalNames() {
		return removalNames;
	}

	public static final class Builder {
		private String inboundEncryptionAnchor;
		private String outboundEncryptionAnchor;
		private String rawInboundName = "mcholepunch-raw-inbound";
		private String rawOutboundName = "mcholepunch-raw-outbound";
		private String cipherDecoderName = "mcholepunch-cipher-decoder";
		private String cipherEncoderName = "mcholepunch-cipher-encoder";
		private String negotiatorName = "mcholepunch-login-negotiator";
		private List<String> removalNames = List.of();

		public Builder encryptionAnchors(String inbound, String outbound) {
			inboundEncryptionAnchor = Objects.requireNonNull(inbound, "inbound");
			outboundEncryptionAnchor = Objects.requireNonNull(outbound, "outbound");
			return this;
		}

		public Builder rawInboundName(String value) {
			rawInboundName = Objects.requireNonNull(value, "rawInboundName");
			return this;
		}

		public Builder rawOutboundName(String value) {
			rawOutboundName = Objects.requireNonNull(value, "rawOutboundName");
			return this;
		}

		public Builder cipherNames(String decoder, String encoder) {
			cipherDecoderName = Objects.requireNonNull(decoder, "decoder");
			cipherEncoderName = Objects.requireNonNull(encoder, "encoder");
			return this;
		}

		public Builder negotiatorName(String value) {
			negotiatorName = Objects.requireNonNull(value, "negotiatorName");
			return this;
		}

		public Builder removalNames(List<String> values) {
			removalNames = List.copyOf(values);
			return this;
		}

		public NettyTakeoverSpec build() {
			if (inboundEncryptionAnchor == null || outboundEncryptionAnchor == null) {
				throw new IllegalStateException("encryption anchors are required");
			}
			return new NettyTakeoverSpec(this);
		}
	}
}
