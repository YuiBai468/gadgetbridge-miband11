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

import android.content.Context;

import nodomain.freeyourgadget.gadgetbridge.GBApplication;

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
 * Pushes live health readings to an HTTP endpoint as JSON.
 *
 * <p>Gadgetbridge can already export its database periodically, but the smallest
 * export interval is one hour — far too slow for reacting to a live reading.
 * This pushes each reading the moment it arrives instead.
 *
 * <p>Configuration lives in the main preferences screen ("Health push").
 *
 * <p>Design notes:
 * <ul>
 *   <li>Everything happens on a single background thread. The BLE callback that
 *       calls {@link #push} must never block on the network.</li>
 *   <li>All errors are swallowed and logged. A dead endpoint, no network or a
 *       wrong URL must never crash the device service.</li>
 *   <li>Repeated identical values are dropped, and there is a minimum interval
 *       between pushes, so we do not hammer the endpoint (or the phone's radio)
 *       when the band reports the same reading every second.</li>
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

    private static final ExecutorService POOL = Executors.newSingleThreadExecutor(r -> {
        final Thread t = new Thread(r, "HealthPush");
        t.setDaemon(true);
        return t;
    });

    /** metric name -> (last value, last push wall clock) */
    private static final Map<String, long[]> LAST = new ConcurrentHashMap<>();

    /** Dropped pushes since process start, for the settings summary. */
    private static final AtomicInteger PUSHED = new AtomicInteger();
    private static final AtomicInteger FAILED = new AtomicInteger();

    private HealthPush() {
    }

    public static int getPushedCount() {
        return PUSHED.get();
    }

    public static int getFailedCount() {
        return FAILED.get();
    }

    /**
     * Queue a reading for delivery. Safe to call from any thread; returns
     * immediately.
     *
     * @param context any context, used to read preferences
     * @param metric  short metric name, e.g. {@code hr}
     * @param value   the reading
     */
    public static void push(final Context context, final String metric, final int value) {
        if (context == null || metric == null) {
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
        if (!shouldSend(metric, value, minInterval)) {
            return;
        }
        final String finalUrl = url.trim();
        final String finalToken = token == null ? "" : token.trim();
        POOL.execute(() -> doPost(finalUrl, finalToken, metric, value));
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

    private static void doPost(final String url, final String token,
                               final String metric, final int value) {
        HttpURLConnection conn = null;
        try {
            final JSONObject body = new JSONObject();
            body.put("metric", metric);
            body.put("value", value);
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
            // Drain so the connection can be reused / closed cleanly
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
