package io.github.resilience4j.springboot3.thread.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Resilience4j thread type.
 *
 * <p>Property prefix: {@code resilience4j.thread}.</p>
 */
@ConfigurationProperties(prefix = "resilience4j.thread")
public class ThreadTypeProperties {

    /**
     * The thread type to be used by Resilience4j's internal schedulers.
     * Supported values:
     * <ul>
     *     <li>{@code platform} (default): traditional platform threads.</li>
     *     <li>{@code virtual}: Java virtual threads (Project Loom).</li>
     * </ul>
     */
    private String type = "platform";

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
