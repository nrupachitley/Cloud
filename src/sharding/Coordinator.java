package sharding;

import java.io.IOException;
import java.util.TimeZone;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
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
    private static String storageType = "sharding";

    /**
     * TODO: Set the values of the following variables to the DNS names of your
     * three dataCenter instances
     */
    private static final String dataCenter1 = "ec2-54-85-87-12.compute-1.amazonaws.com";
    private static final String dataCenter2 = "ec2-54-85-174-62.compute-1.amazonaws.com";
    private static final String dataCenter3 = "ec2-54-84-195-85.compute-1.amazonaws.com";

    private Map<String, Queue<String>> key_map_put;
    private Map<String, ReentrantLock> key_locks;
    private Map<Integer, ReentrantLock> DC_locks;
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

    /**
     *
     */
    public Coordinator() {
        this.key_map_put = new Hashtable<String, Queue<String>>();
        this.key_locks = new Hashtable<>();
        this.DC_locks = new Hashtable<>();
        DC_locks.put(1, new ReentrantLock());
        DC_locks.put(2, new ReentrantLock());
        DC_locks.put(3, new ReentrantLock());
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
                if (!key_locks.containsKey(key)) {
                    key_locks.put(key, new ReentrantLock());
                }
                if (key_map_put.containsKey(key)) {
                    key_map_put.get(key).add(timestamp + "," + value);
                } else {
                    Queue<String> _key_queue = new PriorityBlockingQueue<String>(10, comparator);
                    _key_queue.add(timestamp + "," + value);
                    key_map_put.put(key, _key_queue);
                }

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            final int hash = getHash(key);
                            System.out.println("Hash=" + hash);
                            /**
                             * One key will always go in one data center only.
                             * So we can lock on Key
                             */
                            DC_locks.get(hash).lock();
                            final String recentValue = getRecentKeyValuePair(key);
                            System.out.println("sending to " + "dataCenter" + hash);
                            KeyValueLib.PUT(getDatacenter(hash + ""), key, recentValue);
                        } catch (IOException io) {
                        } finally {
                            DC_locks.get(getHash(key)).unlock();
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
                final int hash = getHash(key);
                DC_locks.get(hash).lock();
                System.out.println("getting from " + loc + " Waiting for Value...");
                Thread t = new Thread(new Runnable() {
                    public void run() {

                        System.out.println("GET: Querying location: " + hash);

                        String value = "";
                        try {
                            if (!loc.equals(hash + "")) {
                                value = "0";
                            } else {
                                System.out.println("Starting GET");
                                value = KeyValueLib.GET(getDatacenter(hash + ""), key);
                                System.out.println("Ending GET");
                            }
                            System.out.println("value=" + value);
                        } catch (IOException io) {
                        }
                        req.response().end(value); //Default response = 0

                    }
                });
                t.start();
                DC_locks.get(getHash(key)).unlock();

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
        });

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

    /**
     *
     * @param key
     * @return
     */
    public synchronized String getRecentKeyValuePair(String key) {
        Queue<String> q = key_map_put.get(key);
        String[] tokens = q.poll().split(",");
        return tokens[1];
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

    /**
     *
     * @param loc
     * @return
     */
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