package redis.db;

import org.junit.Before;
import org.junit.Test;

import java.time.*;
import java.util.HashSet;

import static java.time.Instant.parse;
import static java.time.ZoneId.systemDefault;
import static org.junit.Assert.*;
import static redis.db.ZRangeArgs.WITHSCORES;
import static redis.db.ZaddArgs.CH;
import static redis.db.ZaddArgs.INCR;
import static redis.db.ZaddArgs.XX;

public class KeyValueDbTest {

    private KeyValueDb db;
    private String mykey = "mykey";

    @Before
    public void setUp() {
        db = new KeyValueDb(Clock.fixed(parse("2018-10-26T00:00:00.00Z"), systemDefault()));
    }

    @Test
    public void DBSIZE() {
        String dbsize = db.DBSIZE();
        assertEquals("0", dbsize);
    }

    @Test
    public void DEL() {
        int expected = 1;
        db.SET(mykey, expected);
        db.DEL(mykey);
        String response = db.GET(mykey);
        assertEquals("nil", response);
    }

    @Test
    public void INCR() {
        int expected = 1;
        db.SET(mykey, expected);
        db.INCR(mykey);
        String response = db.GET(mykey);
        assertEquals(Integer.toString(expected + 1), response);
    }

    @Test
    public void SET() {
        int expected = 1;
        db.SET(mykey, expected);
        String response = db.GET(mykey);
        assertEquals(Integer.toString(expected), response);
    }

    @Test
    public void SETGivenDateIsValid() {
        int expected = 1;
        db.SET(mykey, expected, LocalDateTime.now());
        String response = db.GET(mykey);
        assertEquals(Integer.toString(expected), response);
    }

    @Test
    public void SETGivenDateIsNotValid() {
        db = new KeyValueDb(Clock.fixed(parse("2018-10-26T00:00:00.00Z"), systemDefault()));
        int expected = 1;
        db.SET(mykey, expected, LocalDateTime.of(LocalDate.of(2018, 10, 24), LocalTime.now()));
        String response = db.GET(mykey);
        assertEquals("nil", response);
    }

    @Test
    public void ZADD() {
        String response = db.ZADD(new HashSet<>(), mykey, "1", "one");
        assertEquals("1", response);
    }

    @Test
    public void ZADDXx() {
        db.ZADD(new HashSet<>(), mykey, "1", "one");
        HashSet<ZaddArgs> args = new HashSet<>();
        args.add(XX);
        String response = db.ZADD(args, mykey, "2", "one");
        assertEquals("2", response);
    }

    @Test
    public void ZADDCh() {
        db.ZADD(new HashSet<>(), mykey, "1", "one");
        HashSet<ZaddArgs> args = new HashSet<>();
        args.add(CH);
        String response = db.ZADD(args, mykey, "2", "one");
        assertEquals("1", response);
    }

    @Test
    public void ZADDChMultipleScores() {
        db.ZADD(new HashSet<>(), mykey, "1", "one");
        db.ZADD(new HashSet<>(), mykey, "2", "two");
        db.ZADD(new HashSet<>(), mykey, "3", "three");
        db.ZADD(new HashSet<>(), mykey, "4", "four");
        HashSet<ZaddArgs> args = new HashSet<>();
        args.add(CH);
        String response = db.ZADD(args, mykey, "2", "one", "2", "three", "2", "four");
        assertEquals("3", response);
    }

    @Test
    public void ZADDIncr() {
        db.ZADD(new HashSet<>(), mykey, "1", "one");
        HashSet<ZaddArgs> args = new HashSet<>();
        args.add(INCR);
        String response = db.ZADD(args, mykey, "2", "one");
        assertEquals("3", response);
    }

    @Test
    public void ZCARD() {
        db.ZADD(new HashSet<>(), mykey, "1", "one");
        String size = db.ZCARD(mykey);
        assertEquals("1", size);
    }

    @Test
    public void ZRANK() {
        db.ZADD(new HashSet<>(), mykey, "1", "one");
        db.ZADD(new HashSet<>(), mykey, "2", "two");
        String size = db.ZRANK(mykey, "two");
        assertEquals("1", size);
    }

    @Test
    public void ZRANGE() {
        db.ZADD(new HashSet<>(), mykey, "1", "one");
        db.ZADD(new HashSet<>(), mykey, "2", "two");
        db.ZADD(new HashSet<>(), mykey, "2", "three");
        String response = db.ZRANGE(mykey, 0, 2, null);
        assertEquals("[one, three]", response);
    }

    @Test
    public void ZRANGEWithScore() {
        db.ZADD(new HashSet<>(), mykey, "1", "one");
        db.ZADD(new HashSet<>(), mykey, "2", "two");
        db.ZADD(new HashSet<>(), mykey, "2", "three");
        String response = db.ZRANGE(mykey, 0, 2, WITHSCORES);
        assertEquals("[one, 1, three, 2]", response);
    }
}