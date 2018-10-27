package redis.db;

import redis.exception.ExpirationDateException;

import java.time.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.util.Arrays.asList;

public class KeyValueDb {
    private static final String SET_CONFIRMATION = "OK";
    private static final String NIL = "nil";
    private ConcurrentHashMap<String, DataOperations> map;
    private Clock clock;

    public KeyValueDb(Clock clock) {
        this.clock = clock;
        this.map = new ConcurrentHashMap<>();
    }

    public String DBSIZE() {
        return Integer.toString(map.size());
    }

    public String DEL(String key) {
        Supplier<String> getFunc = () -> map.remove(key).getData(clock);
        return executeQueryWithErrorHandling(getFunc, key);
    }

    public String GET(String key) {
        Supplier<String> getFunc = () -> {
            DataOperations dataOperations = map.get(key);
            if (dataOperations != null) {
                return dataOperations.getData(clock);
            } else {
                return NIL;
            }
        };
        return executeQueryWithErrorHandling(getFunc, key);
    }

    public String INCR(String key) {
        Supplier<String> incrFunc = () -> {
            map.get(key).increment(clock);
            return key;
        };
        return executeQueryWithErrorHandling(incrFunc, key);
    }

    public String SET(String key, Integer value) {
        if (!map.containsKey(key)) {
            DataOperations data = new DataOperations();
            data.set(value);
            map.put(key, data);
            return SET_CONFIRMATION;
        } else {
            return NIL;
        }
    }

    public String SET(String key, Integer value, LocalDateTime time) {
        if (!map.containsKey(key)) {
            DataOperations data = new DataOperations();
            data.set(value, time);
            map.put(key, data);
            return SET_CONFIRMATION;
        } else {
            return NIL;
        }
    }

    public String ZADD(Set<ZaddArgs> args, String key, String... scores) {
        if (!map.containsKey(key)) {
            map.put(key, new DataOperations());
        }
        return Integer.toString(map.get(key).addScores(args, asList(scores), clock));
    }

    public String ZCARD(String key) {
        Supplier<String> zcardFunc = () -> Integer.toString(map.get(key).getSetSize(clock));
        return executeQueryWithErrorHandling(zcardFunc, key);
    }

    public String ZRANK(String key, String secondKey) {
        Supplier<String> zrankFunc = () -> map.get(key).getRank(secondKey, clock);
        return executeQueryWithErrorHandling(zrankFunc, key);
    }

    public String ZRANGE(String key, int start, int end, ZRangeArgs withScore) {
        Supplier<String> zrangeFunc = () -> {
            int size = Integer.parseInt(this.ZCARD(key));

            return map.get(key).getRange(start, end, withScore, size, clock).toString();
        };

        return executeQueryWithErrorHandling(zrangeFunc, key);
    }

    private String executeQueryWithErrorHandling(Supplier<String> runnable, String key) {
        try {
            return runnable.get();
        } catch (ExpirationDateException exception) {
            map.remove(key);
            return NIL;
        }
    }
}
