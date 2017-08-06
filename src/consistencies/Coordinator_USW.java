package consistencies;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

class Key_Value implements Comparator<Key_Value> {

    String key;
    String value;
    Long timestamp;
    Integer region;

    public Key_Value(String k, String v, long time, int r) {
        key = k;
        value = v;
        timestamp = time;
        region = r;
    }

    @Override
    public int compare(Key_Value o1, Key_Value o2) {
        return o1.timestamp.compareTo(o2.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Key_Value) {
            Key_Value l = (Key_Value) o;
            if (l.key.equals(this.key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash + Objects.hashCode(this.key);
        return hash;
    }

    @Override
    public String toString() {
        return "Key=" + key + "Value=" + value + "TimeStamp=" + timestamp;
    }

}

/**
 * US EAST Coordinator
 *
 * @author Nrupa
 */
public class Coordinator extends Verticle {

    // This integer variable tells you what region you are in
    // 1 for US-E, 2 for US-W, 3 for Singapore
    private static int region = 1;

    // Default mode: Strongly consistent
    // Options: causal, eventual, strong
    private static String consistencyType = "eventual";

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances. Be sure to match the regions with their DNS!
     * Do the same for the 3 Coordinators as well.
     */
    private static final String dataCenterUSE = "ec2-54-88-155-11.compute-1.amazonaws.com";
    private static final String dataCenterUSW = "ec2-54-173-114-86.compute-1.amazonaws.com";
    private static final String dataCenterSING = "ec2-54-172-117-139.compute-1.amazonaws.com";

    private static final String coordinatorUSE = "ec2-54-172-118-40.compute-1.amazonaws.com";
    private static final String coordinatorUSW = "ec2-54-152-200-217.compute-1.amazonaws.com";
    private static final String coordinatorSING = "ec2-54-172-238-99.compute-1.amazonaws.com";

    private final Map<String, Queue<Key_Value>> key_map = new Hashtable<>();
    private Map<String, ReentrantLock> key_locks = new Hashtable<>();
    private Comparator<Key_Value> comparator = new Comparator<Key_Value>() {
        @Override
        public int compare(Key_Value o1, Key_Value o2) {
            return o1.timestamp.compareTo(o2.timestamp);
        }
    };

    @Override
    public void start() {
        KeyValueLib.dataCenters.put(dataCenterUSE, 1);
        KeyValueLib.dataCenters.put(dataCenterUSW, 2);
        KeyValueLib.dataCenters.put(dataCenterSING, 3);
        KeyValueLib.coordinators.put(coordinatorUSE, 1);
        KeyValueLib.coordinators.put(coordinatorUSW, 2);
        KeyValueLib.coordinators.put(coordinatorSING, 3);
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
                final Long timestamp = Long.parseLong(map.get("timestamp"));
                final String forwarded = map.get("forward");
                final String forwardedRegion = map.get("region");

                Key_Value key_Value = new Key_Value(key, value, timestamp, region);

                System.out.println("Object Lock acquired");
                if (!key_locks.containsKey(key)) {
                    key_locks.put(key, new ReentrantLock());

                }

                if (key_map.containsKey(key)) {
                    System.out.println("IF: Putting in PUT Map");
                    key_map.get(key).add(key_Value);
                } else {
                    System.out.println("ELSE: Putting in PUT Map");
                    Queue<Key_Value> key_value_q = new PriorityBlockingQueue<Key_Value>(10, comparator);
                    key_value_q.add(key_Value);
                    key_map.put(key, key_value_q);
                }

                Thread t = new Thread(new Runnable() {
                    public void run() {
                        if (forwarded == null) {
                            /**
                             * this means the reqeust is from client, so we ash
                             * it and forward it to the appropriate coordinator
                             */
                            System.out.println("Handling request directly from client");
                            int target_DC = getHash(key);
                            System.out.println("");
                            System.out.println("Key Map="+key_map.toString());
                            Key_Value recentValue = getRecentKeyValuePair(key);
                            System.out.println(Thread.currentThread().getName() + " key=" + key);
                            if (target_DC == Constants.US_EAST) {
                                if (consistencyType.equals("eventual")) {
                                    try {
                                        KeyValueLib.PUT(dataCenterUSE, key, value, timestamp + "", "eventual");
                                        KeyValueLib.PUT(dataCenterUSW, key, value, timestamp + "", "eventual");
                                        KeyValueLib.PUT(dataCenterSING, key, value, timestamp + "", "eventual");
                                    } catch (IOException io) {
                                    }
                                } else {
                                    try {
                                        key_locks.get(key).lock();
                                        System.out.println("Informing data centers of Ahead request");
                                        KeyValueLib.AHEAD(key, timestamp + "");
                                        KeyValueLib.PUT(dataCenterUSE, key, value, timestamp + "", "strong");
                                        KeyValueLib.PUT(dataCenterUSW, key, value, timestamp + "", "strong");
                                        KeyValueLib.PUT(dataCenterSING, key, value, timestamp + "", "strong");
                                        System.out.println("Informing data centers of Complete request");
                                        KeyValueLib.COMPLETE(key, timestamp + "");
                                        key_locks.get(key).unlock();
                                    } catch (IOException io) {
                                    }
                                }
                            } else {
                                try {
                                    switch (target_DC) {

                                        case 2: {
                                            KeyValueLib.FORWARD(coordinatorUSW, key, value, timestamp + "");
                                            break;
                                        }
                                        case 3: {
                                            KeyValueLib.FORWARD(coordinatorSING, key, value, timestamp + "");
                                            break;
                                        }
                                    }
                                } catch (IOException io) {
                                }
                            }
                        } else {
                            /**
                             * here means that the reuest is form another
                             * coordinator
                             */
                            System.out.println("Handling request for this coordinator");
                            try {
                                if (consistencyType.equals("eventual")) {
                                    try {
                                        KeyValueLib.PUT(dataCenterUSE, key, value, timestamp + "", "eventual");
                                        KeyValueLib.PUT(dataCenterUSW, key, value, timestamp + "", "eventual");
                                        KeyValueLib.PUT(dataCenterSING, key, value, timestamp + "", "eventual");
                                    } catch (IOException io) {
                                    }

                                } else {
                                    System.out.println("Informing data centers of Ahead request");
                                    KeyValueLib.AHEAD(key, timestamp + "");
                                    try {
                                        key_locks.get(key).lock();
                                        KeyValueLib.PUT(dataCenterUSE, key, value, timestamp + "", "strong");
                                        KeyValueLib.PUT(dataCenterUSW, key, value, timestamp + "", "strong");
                                        KeyValueLib.PUT(dataCenterSING, key, value, timestamp + "", "strong");
                                        System.out.println("Informing data centers of Complete request");
                                        KeyValueLib.COMPLETE(key, timestamp + "");
                                        key_locks.get(key).unlock();
                                    } catch (IOException io) {
                                    }
//                                    key_locks.get(key).unlock();
                                }
                            } catch (IOException e) {
                            }
                        }

                    }

                }
                );
                t.start();

                req.response().end(); // Do not remove this
            }
        });

        routeMatcher.get(
                "/get", new Handler<HttpServerRequest>() {
                    @Override
                    public void handle(final HttpServerRequest req
                    ) {
                        MultiMap map = req.params();
                        final String key = map.get("key");
                        final Long timestamp = Long.parseLong(map.get("timestamp"));
                        System.out.println("Waiting for put to complete...");
                        if (consistencyType.equals("eventual")) {
                            System.out.println("Executing GET... for " + key);
                            Thread t = new Thread(new Runnable() {
                                public void run() {
                                    try {
                                        String response = KeyValueLib.GET(dataCenterUSE, key, timestamp + "", consistencyType);
                                        req.response().end(response);
                                    } catch (IOException io) {
                                    }
                                }
                            });
                            t.start();
                        }

//                        key_locks.get(key).unlock();
                    }
                }
        );
        /* This endpoint is used by the grader to change the consistency level */
        routeMatcher.get(
                "/consistency", new Handler<HttpServerRequest>() {
                    @Override
                    public void handle(final HttpServerRequest req
                    ) {
                        MultiMap map = req.params();
                        consistencyType = map.get("consistency");
                        req.response().end();
                    }
                }
        );
        /* BONUS HANDLERS BELOW */
        routeMatcher.get(
                "/forwardcount", new Handler<HttpServerRequest>() {
                    @Override
                    public void handle(final HttpServerRequest req
                    ) {
                        req.response().end(KeyValueLib.COUNT());
                    }
                }
        );

        routeMatcher.get(
                "/reset", new Handler<HttpServerRequest>() {
                    @Override
                    public void handle(final HttpServerRequest req
                    ) {
                        KeyValueLib.RESET();
                        req.response().end();
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

    public synchronized Key_Value getRecentKeyValuePair(String key) {
        Queue<Key_Value> q = key_map.get(key);
        Key_Value key_Value = q.poll();
        return key_Value;
    }

    /**
     *
     * @param key
     * @return
     */
    public int getHash(String key) {
        int sum = 0;
        for (int i = 0; i < key.length(); i++) {
            sum += key.charAt(i) - 'a';
        }
        return Math.abs(sum) % 3 + 1;
    }

}