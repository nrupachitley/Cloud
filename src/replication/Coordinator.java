package replication;

import java.io.IOException;
import java.util.TimeZone;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

    //Default mode: replication. Possible string values are "replication" and "sharding"
    private static String storageType = "replication";

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances
     */
    private static final String dataCenter1 = "ec2-54-88-192-240.compute-1.amazonaws.com";
    private static final String dataCenter2 = "ec2-54-164-8-169.compute-1.amazonaws.com";
    private static final String dataCenter3 = "ec2-54-85-11-54.compute-1.amazonaws.com";
    private static boolean notifyPoorGet;
    private final Object lock = new Object();
    private final Object lock2 = new Object();
    private final Object GETRequestlock = new Object();
    private Map<String, Queue<String>> key_map_put;
    private Map<String, ReentrantLock> key_locks;
    /**
     * Appending Time stamp to key value pair;
     */
    private Comparator<String> comparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            String timeStamp1 = o1.split(",")[0];
            String timeStamp2 = o2.split(",")[0];
            return timeStamp1.compareToIgnoreCase(timeStamp2);
        }
    };

    public Coordinator() {
        this.key_map_put = new Hashtable<String, Queue<String>>();
        this.key_locks = new Hashtable<String, ReentrantLock>();
        Coordinator.notifyPoorGet = false;
    }

    @Override
    public void start() {
        //DO NOT MODIFY THIS
        KeyValueLib.dataCenters.put(dataCenter1, 1);
        KeyValueLib.dataCenters.put(dataCenter2, 2);
        KeyValueLib.dataCenters.put(dataCenter3, 3);
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

                System.out.println("Key=" + key + " value=" + value);
                final String timestamp = new Timestamp(System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset()).toString();

                System.out.println("Object Lock acquired");
                if (!key_locks.containsKey(key)) {
                    key_locks.put(key, new ReentrantLock());
                }

                if (key_map_put.containsKey(key)) {
                    System.out.println("IF: Putting in PUT Map");
                    key_map_put.get(key).add(timestamp + "," + value);
                } else {
                    System.out.println("ELSE: Putting in PUT Map");
                    Queue<String> _key_queue = new PriorityBlockingQueue<String>(10, comparator);
                    _key_queue.add(timestamp + "," + value);
                    key_map_put.put(key, _key_queue);
                }

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        key_locks.get(key).lock();
                        System.out.println(Thread.currentThread().getName() + " key=" + key);
                        String recentValue = getRecentKeyValuePair(key);
                        try {
                            KeyValueLib.PUT(dataCenter1, key, recentValue);
                            System.out.println(Thread.currentThread().getName() + " key=" + key);
                            System.out.println("sending to " + dataCenter1);
                            KeyValueLib.PUT(dataCenter2, key, recentValue);
                            System.out.println(Thread.currentThread().getName() + " key=" + key);
                            System.out.println("sending to " + dataCenter2);
                            KeyValueLib.PUT(dataCenter3, key, recentValue);
                            System.out.println(Thread.currentThread().getName() + " key=" + key);
                            System.out.println("sending to " + dataCenter3);
//                            notifyGet(key);
                        } catch (IOException io) {
                        } finally {
                            key_locks.get(key).unlock();
                        }

                    }
                }).start();
                req.response().end(); //Do not remove this
            }
        });

        routeMatcher.get("/get", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                MultiMap map = req.params();
                final String key = map.get("key");
                final String loc = map.get("loc");
                System.out.println("Key=" + key + " location=" + loc);
                System.out.println("Waiting for put to complete...");
                key_locks.get(key).lock();
                System.out.println("Executing GET... for " + key);
                System.out.println("Sending to " + loc + "Waiting for Value...");
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        String value = "";
                        try {
                            System.out.println(key_map_put);
                            value = KeyValueLib.GET(getDatacenter(loc), key);
                        } catch (IOException io) {
                        }
                        System.out.println("value=" + value);
                        if (value != null && !value.equals("")) {
                            req.response().end(value); //Default response = 0
                        } else {
                            req.response().end("0"); //Default response = 0
                        }
                    }
                });
                t.start();
                key_locks.get(key).unlock();
            }
        });

        routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
                    @Override
                    public void handle(final HttpServerRequest req
                    ) {
                        MultiMap map = req.params();
                        storageType = map.get("storage");
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

        server.listen(8080);
    }

    public synchronized String getRecentKeyValuePair(String key) {
        Queue<String> q = key_map_put.get(key);
        try {
            String[] tokens = q.poll().split(",");
            return tokens[1];
        } catch (NullPointerException ne) {
            return "0";
        }

    }

    public String getDatacenter(String loc) {
        String dns = "";
        switch (loc) {
            case "1": {
                dns = dataCenter1;
                break;
            }
            case "2": {
                dns = dataCenter2;
                break;
            }
            case "3": {
                dns = dataCenter3;
                break;
            }
        }
        return dns;
    }

}