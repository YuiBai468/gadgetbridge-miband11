/*  Copyright (C) 2026  Gadgetbridge contributors

    This file is part of Gadgetbridge.

    Gadgetbridge is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Gadgetbridge is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>. */
package nodomain.freeyourgadget.gadgetbridge.util;

import androidx.annotation.Nullable;

import nodomain.freeyourgadget.gadgetbridge.GBApplication;
import nodomain.freeyourgadget.gadgetbridge.entities.XiaomiActivitySample;
import nodomain.freeyourgadget.gadgetbridge.model.ActivityKind;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pushes health readings to a user-configured HTTP endpoint as JSON.
 *
 * <p>Gadgetbridge can already export its database periodically, but the smallest
 * export interval is one hour — far too slow for reacting to a live reading.
 * This pushes readings the moment they arrive instead.
 *
 * <p>Where each metric comes from:
 * <ul>
 *   <li>{@code hr} — streamed live by the band, hooked in {@code XiaomiHealthService}</li>
 *   <li>{@code stress}, {@code spo2}, {@code steps}, {@code sleep} — written to the
 *       database, hooked in {@code XiaomiSampleProvider}. Nothing but HR is a live
 *       stream: stress and SpO2 arrive from on-demand measurements or periodic
 *       syncs, sleep only after a sync.</li>
 * </ul>
 *
 * <p>Design notes:
 * <ul>
 *   <li>Everything happens on a single background thread. The BLE callback that
 *       calls {@link #push} must never block on the network.</li>
 *   <li>All errors are swallowed and logged. A dead endpoint, no network or a
 *       wrong URL must never crash the device service.</li>
 *   <li>Repeated identical values are dropped, and there is a minimum interval
 *       between pushes, so a batch sync of a day's samples cannot flood the
 *       endpoint.</li>
 *   <li>The pending queue is bounded: if the endpoint is slow or dead we drop
 *       new work rather than grow an unbounded backlog.</li>
 * </ul>
 */
public final class HealthPush {
    private static final Logger LOG = LoggerFactory.getLogger(HealthPush.class);

    public static final String PREF_ENABLED = "health_push_enabled";
    public static final String PREF_URL = "health_push_url";
    public static final String PREF_TOKEN = "health_push_token";
    public static final String PREF_MIN_INTERVAL_S = "health_push_min_interval";

    private static final int CONNECT_TIMEOUT_MS = 3000;
    private static final int READ_TIMEOUT_MS = 3000;

    /** Drop new readings once this many are already waiting to be sent. */
    private static final int MAX_PENDING = 32;

    private static final ExecutorService POOL = Executors.newSingleThreadExecutor(r -> {
        final Thread t = new Thread(r, "HealthPush");
        t.setDaemon(true);
        return t;
    });

    /** metric name -> {last value, last push wall clock} */
    private static final Map<String, long[]> LAST = new ConcurrentHashMap<>();

    private static final AtomicInteger PENDING = new AtomicInteger();
    private static final AtomicInteger PUSHED = new AtomicInteger();
    private static final AtomicInteger FAILED = new AtomicInteger();
    private static final AtomicInteger DROPPED = new AtomicInteger();

    private HealthPush() {
    }

    public static int getPushedCount() {
        return PUSHED.get();
    }

    public static int getFailedCount() {
        return FAILED.get();
    }

    public static int getDroppedCount() {
        return DROPPED.get();
    }

    /**
     * Queue a reading for delivery. Safe to call from any thread; returns
     * immediately.
     *
     * @param metric short metric name, e.g. {@code hr}, {@code stress}, {@code spo2}
     * @param value  the reading; non-positive values are ignored
     */
    public static void push(final String metric, final int value) {
        push(metric, value, null);
    }

    public static void push(final String metric, final int value, @Nullable final String text) {
        if (metric == null || value <= 0) {
            return;
        }
        final Prefs prefs;
        try {
            prefs = GBApplication.getPrefs();
        } catch (final Exception e) {
            return;                     // app not initialised yet
        }
        if (prefs == null || !prefs.getBoolean(PREF_ENABLED, false)) {
            return;
        }
        final String url = prefs.getString(PREF_URL, "");
        if (url == null || url.trim().isEmpty()) {
            return;
        }
        final String token = prefs.getString(PREF_TOKEN, "");
        int minInterval = 10;
        try {
            minInterval = Integer.parseInt(prefs.getString(PREF_MIN_INTERVAL_S, "10").trim());
        } catch (final Exception ignored) {
        }
        // 去重键要带上 text：否则"跑步开始"和"跑步结束"的 value 都是 1，
        // 第二条会被当成重复值给丢掉。
        final String key = (text == null || text.isEmpty()) ? metric : metric + "|" + text;
        if (!shouldSend(key, value, minInterval)) {
            return;
        }
        if (PENDING.get() >= MAX_PENDING) {
            DROPPED.incrementAndGet();
            return;
        }
        PENDING.incrementAndGet();
        POOL.execute(() -> {
            try {
                doPost(url.trim(), token == null ? "" : token.trim(), metric, value, text);
            } finally {
                PENDING.decrementAndGet();
            }
        });
    }

    /**
     * Push an event that carries no meaningful numeric value, such as a workout
     * starting or finishing. The {@code text} is what distinguishes events, and
     * is also what they de-duplicate on.
     */
    public static void pushEvent(final String metric, @Nullable final String text) {
        push(metric, 1, text);
    }

    /**
     * Push everything interesting a Xiaomi sample carries. Unset values (0, or
     * "not measured") are skipped by {@link #push}.
     *
     * @param includeSteps pass false for batch syncs, where the step counter
     *                     changes on every sample and is not worth reporting
     */
    public static void pushSample(@Nullable final XiaomiActivitySample sample,
                                  final boolean includeSteps) {
        if (sample == null) {
            return;
        }
        push("hr", sample.getHeartRate());
        push("stress", sample.getStress());
        push("spo2", sample.getSpo2());
        if (includeSteps) {
            push("steps", sample.getSteps());
        }
        final ActivityKind kind = sample.getKind();
        if (kind == null || kind == ActivityKind.UNKNOWN || kind == ActivityKind.NOT_MEASURED) {
            return;
        }
        // Only report sleep/wake, not every sport
        if (kind == ActivityKind.LIGHT_SLEEP || kind == ActivityKind.DEEP_SLEEP
                || kind == ActivityKind.REM_SLEEP || kind == ActivityKind.AWAKE_SLEEP) {
            push("sleep", kind.getCode(), kind.name());
        } else if (kind == ActivityKind.ACTIVITY || kind == ActivityKind.WALKING) {
            push("awake", kind.getCode(), kind.name());
        }
    }

    /**
     * @return true if this reading is worth sending: different from the last
     * value, or at least {@code minInterval} seconds have passed.
     */
    private static boolean shouldSend(final String metric, final int value, final int minInterval) {
        final long now = System.currentTimeMillis();
        final long[] prev = LAST.get(metric);
        if (prev == null) {
            LAST.put(metric, new long[]{value, now});
            return true;
        }
        final boolean changed = prev[0] != value;
        final boolean stale = now - prev[1] >= minInterval * 1000L;
        if (!changed && !stale) {
            return false;
        }
        LAST.put(metric, new long[]{value, now});
        return true;
    }

    private static void doPost(final String url, final String token, final String metric,
                               final int value, @Nullable final String text) {
        HttpURLConnection conn = null;
        try {
            final JSONObject body = new JSONObject();
            body.put("metric", metric);
            body.put("value", value);
            if (text != null && !text.isEmpty()) {
                body.put("text", text);
            }
            body.put("ts", System.currentTimeMillis() / 1000L);
            body.put("source", "gadgetbridge");

            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
            if (!token.isEmpty()) {
                conn.setRequestProperty("X-Token", token);
            }
            final byte[] payload = body.toString().getBytes(StandardCharsets.UTF_8);
            conn.setFixedLengthStreamingMode(payload.length);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload);
            }
            final int code = conn.getResponseCode();
            if (code >= 200 && code < 300) {
                PUSHED.incrementAndGet();
            } else {
                FAILED.incrementAndGet();
                LOG.warn("Health push got HTTP {} for {}", code, metric);
            }
            try (java.io.InputStream is = code >= 400 ? conn.getErrorStream() : conn.getInputStream()) {
                if (is != null) {
                    final byte[] buf = new byte[256];
                    //noinspection StatementWithEmptyBody
                    while (is.read(buf) > 0) {
                        // discard
                    }
                }
            } catch (final Exception ignored) {
            }
        } catch (final Exception e) {
            FAILED.incrementAndGet();
            LOG.debug("Health push failed for {}: {}", metric, e.getMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}
