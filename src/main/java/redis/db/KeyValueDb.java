package redis.db;

import redis.exception.ExpirationDateException;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class KeyValueDb {
    private static final String SET_CONFIRMATION = "OK";
    private ConcurrentHashMap<String, DataOperations> map;
    private Clock clock;

    public KeyValueDb(Clock clock) {
        this.clock = clock;
        this.map = new ConcurrentHashMap<>();
    }

    public String DBSIZE() {
        return Integer.toString(map.size());
    }

    //TODO: any another type of exception catch here.
    public String DEL(String key) {
        Supplier<String> getFunc = () -> map.remove(key).getData(clock);
        return errorHandling(getFunc, key);
    }

    public String GET(String key) {
        Supplier<String> getFunc = () -> map.get(key).getData(clock);
        return errorHandling(getFunc, key);
    }

    public String INCR(String key) {
        Supplier<String> incrFunc = () -> {
            map.get(key).increment();
            return key;
        };
        return errorHandling(incrFunc, key);
    }

    public String SET(String key, Integer value) {
        DataOperations data = new DataOperations();
        data.set(value);
        map.put(key, data);
        return SET_CONFIRMATION;
    }

    public String SET(String key, Integer value, LocalDateTime time) {
        DataOperations data = new DataOperations();
        data.set(value, time);
        map.put(key, data);
        return SET_CONFIRMATION;
    }

    public String ZADD(String... scores) {
        //TODO: ADD Scores
        return "0";
    }

    public String ZCARD(String key) {
        Supplier<String> zcardFunc = () -> Integer.toString(map.get(key).getSetSize());
        return errorHandling(zcardFunc, key);
    }

    public String ZRANK(String key) {
        Supplier<String> zrankFunc = () -> map.get(key).getRank(key);
        return errorHandling(zrankFunc, key);
    }

    public String ZRANGE(String key, int start, int end, String withScore) {
        Supplier<String> zrangeFunc = () -> {
            int size = Integer.parseInt(this.ZCARD(key));

            return map.get(key).getRange(start, end, withScore, size).toString();
        };

        return errorHandling(zrangeFunc, key);
    }

    private String errorHandling(Supplier<String> runnable, String key) {
        try {
            return runnable.get();
        } catch (ExpirationDateException exception) {
            map.remove(key);
            return "No Data";
        }
    }
}
