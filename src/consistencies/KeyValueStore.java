package consistencies;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.TimeZone;
import java.util.Queue;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

/**
 *
 * @author nrupa
 */

class StoreValue {

    private Long timestamp;
    private String value;

    public StoreValue(long time, String val) {
        timestamp = time;
        value = val;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "TimeStamp= " + getTimestamp() + " Value= " + getValue();
    }

}

public class KeyValueStore extends Verticle {

    private static Hashtable<String, ArrayList<StoreValue>> store = null;

    //    private static Hashtable<String, PriorityQueue<StoreValue>> Ordered_store = null;
//    private Hashtable<String, ReentrantLock> key_locks;
//    private Hashtable<String, ReentrantLock> ahead_req_locks;
//    private ReentrantLock Causal_lock = new ReentrantLock();
    private Comparator<StoreValue> comparator = new Comparator<StoreValue>() {
        @Override
        public int compare(StoreValue o1, StoreValue o2) {
            return o1.getTimestamp().compareTo(o2.getTimestamp());
        }
    };

    public KeyValueStore() {
        store = new Hashtable<String, ArrayList<StoreValue>>();
//        Ordered_store = new Hashtable<String, PriorityQueue<StoreValue>>();

//        key_locks = new Hashtable<String, ReentrantLock>();
//        ahead_req_locks = new Hashtable<>();
    }

    @Override
    public void start() {
        final KeyValueStore keyValueStore = new KeyValueStore();
        final RouteMatcher routeMatcher = new RouteMatcher();
        final HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(32767);
        server.setUsePooledBuffers(true);
        server.setReceiveBufferSize(4 * 1024);
        routeMatcher.get("/put", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String value = map.get("value");
                final String consistency = map.get("consistency");
                final Integer region = Integer.parseInt(map.get("region"));
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        if (store.containsKey(key)) {
                            StoreValue sv = new StoreValue(Skews.handleSkew(timestamp, region), value);
                            store.get(key).add(sv);
                        } else {
                            ArrayList<StoreValue> queue = new ArrayList<StoreValue>();
                            queue.add(new StoreValue(Skews.handleSkew(timestamp, region), value));
                            store.put(key, queue);
                        }
                    }
                }).start();

                String response = "stored";
                req.response().putHeader("Content-Type", "text/plain");
                req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                req.response().end(response);
                req.response().close();
            }
        });
        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                String consistency = map.get("consistency");
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                String response = "";
                ArrayList<StoreValue> values = null;
                try {
                    values = store.get(key);
                    if (values != null) {
                        for (StoreValue val : values) {
                            response = response + val.getValue() + " ";
                        }
                    } else {
                        response = "";
                        System.out.println("How can thsi be null?");
                    }

                } catch (NullPointerException ne) {
                }
//                }
                /* Do NOT change the format the response. It will return a string of
                 * values separated by spaces */
                req.response()
                        .putHeader("Content-Type", "text/plain");
                if (response
                        != null) {
                    req.response().putHeader("Content-Length",
                            String.valueOf(response.length()));
                }

                req.response().end(response);
                req.response().close();
            }
        });
        // Handler for when the AHEAD is called
        routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
                    @Override
                    public void handle(final HttpServerRequest req
                    ) {
                        MultiMap map = req.params();
                        String key = map.get("key");
//                System.out.println("Ahead reqyest received for key" + key);
                        final Long timestamp = Long.parseLong(map.get("timestamp"));
                        req.response().putHeader("Content-Type", "text/plain");
                        req.response().end();
                        req.response().close();
                    }
                }
        );
        // Handler for when the COMPLETE is called
        routeMatcher.get(
                "/complete", new Handler<HttpServerRequest>() {
                    @Override
                    public void handle(final HttpServerRequest req
                    ) {
                        MultiMap map = req.params();
                        String key = map.get("key");
                        final Long timestamp = Long.parseLong(map.get("timestamp"));
                        req.response().putHeader("Content-Type", "text/plain");
                        req.response().end();
                        req.response().close();
                    }
                }
        );
        // Clears this stored keys. Do not change this
        routeMatcher.get(
                "/reset", new Handler<HttpServerRequest>() {
                    @Override
                    public void handle(final HttpServerRequest req
                    ) {
                        KeyValueStore.store.clear();
                        req.response().putHeader("Content-Type", "text/plain");
                        req.response().end();
                        req.response().close();
                    }
                }
        );
        routeMatcher.noMatch(
                new Handler<HttpServerRequest>() {
                    @Override
                    public void handle(final HttpServerRequest req
                    ) {
                        req.response().putHeader("Content-Type", "text/html");
                        String response = "Not found.";
                        req.response().putHeader("Content-Length",
                                String.valueOf(response.length()));
                        req.response().end(response);
                        req.response().close();
                    }
                }
        );
        server.requestHandler(routeMatcher);

        server.listen(
                8080);
    }

}