package io.kafbat.ui.emitter;

import com.google.common.annotations.VisibleForTesting;
import io.kafbat.ui.config.ClustersProperties;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

public class PollingSettings {

  private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(1_000);
  private static final long DEFAULT_MAX_BYTES_PER_ROUND = 32L * 1024 * 1024;

  private final Duration pollTimeout;
  private final Supplier<PollingThrottler> throttlerSupplier;
  private final long maxBytesPerRound;

  public static PollingSettings create(ClustersProperties.Cluster cluster,
                                       ClustersProperties clustersProperties) {
    var pollingProps = Optional.ofNullable(clustersProperties.getPolling())
        .orElseGet(ClustersProperties.PollingProperties::new);

    var pollTimeout = pollingProps.getPollTimeoutMs() != null
        ? Duration.ofMillis(pollingProps.getPollTimeoutMs())
        : DEFAULT_POLL_TIMEOUT;

    return new PollingSettings(
        pollTimeout,
        PollingThrottler.throttlerSupplier(cluster),
        DEFAULT_MAX_BYTES_PER_ROUND
    );
  }

  public static PollingSettings createDefault() {
    return createDefault(DEFAULT_MAX_BYTES_PER_ROUND);
  }

  @VisibleForTesting
  static PollingSettings createDefault(long maxBytesPerRound) {
    return new PollingSettings(
        DEFAULT_POLL_TIMEOUT,
        PollingThrottler::noop,
        maxBytesPerRound
    );
  }

  private PollingSettings(Duration pollTimeout,
                          Supplier<PollingThrottler> throttlerSupplier,
                          long maxBytesPerRound) {
    this.pollTimeout = pollTimeout;
    this.throttlerSupplier = throttlerSupplier;
    this.maxBytesPerRound = maxBytesPerRound;
  }

  public Duration getPollTimeout() {
    return pollTimeout;
  }

  public PollingThrottler getPollingThrottler() {
    return throttlerSupplier.get();
  }

  // bytes retained by a polling round with grown window (see RangePollingEmitter)
  public long getMaxBytesPerRound() {
    return maxBytesPerRound;
  }
}
