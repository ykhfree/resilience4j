package io.github.resilience4j.ratelimiter.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

/**
 * Base class for tests that need to run in both platform and virtual thread modes.
 * Handles thread mode configuration and cleanup.
 */
public abstract class ThreadModeTestBase {

    protected static final String SYS_PROP_KEY = "resilience4j.thread.type";
    
    @Parameterized.Parameter
    public String threadMode;
    
    private String originalPropertyValue;

    @Parameterized.Parameters(name = "threadMode={0}")
    public static Object[][] threadModes() {
        return new Object[][] {
            {"platform"}, // Default platform threads
            {"virtual"}   // Virtual threads
        };
    }

    @Before
    public void setUpThreadMode() {
        // Save original property value
        originalPropertyValue = System.getProperty(SYS_PROP_KEY);
        
        // Configure thread mode for test
        if ("virtual".equals(threadMode)) {
            System.setProperty(SYS_PROP_KEY, "virtual");
        } else {
            System.clearProperty(SYS_PROP_KEY);
        }
    }

    @After
    public void cleanUpThreadMode() {
        // Restore original property value
        if (originalPropertyValue != null) {
            System.setProperty(SYS_PROP_KEY, originalPropertyValue);
        } else {
            System.clearProperty(SYS_PROP_KEY);
        }
    }

    /**
     * Returns true if running in virtual thread mode.
     */
    protected boolean isVirtualThreadMode() {
        return "virtual".equals(threadMode);
    }
    
    /**
     * Returns a descriptive string for the current thread mode.
     */
    protected String getThreadModeDescription() {
        return isVirtualThreadMode() ? "Virtual Thread Mode" : "Platform Thread Mode";
    }
}