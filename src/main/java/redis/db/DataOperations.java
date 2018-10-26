package redis.db;

import redis.exception.ExpirationDateException;

import java.time.*;
import java.util.*;

class DataOperations {
    private static final int DUMMY_SCORE = 0;
    private static final String NIL = "nil";
    private static final String WITH_SCORES_COMMAND_VERIFIER = "WITHSCORES";
    private volatile int data;
    private volatile SortedSet<Score> set = new TreeSet<>(Comparator.comparing(Score::getScore));
    private volatile LocalDateTime time;

    String getData(Clock clock) {
        synchronized (this) {
            validateData(clock);
            return Integer.toString(data);
        }
    }

    int set(int newData) {
        synchronized (this) {
            this.data = newData;
            return data;
        }
    }

    int set(int newData, LocalDateTime time) {
        synchronized (this) {
            this.data = newData;
            this.time = time;
            return data;
        }
    }

    int increment(Clock clock) {
        synchronized (this) {
            validateData(clock);
            this.data = data + 1;
            return data;
        }
    }

    //TODO: need to impl XX,NX,CH,INCR
    boolean addScore(String key, int score, Clock clock) {
        synchronized (this) {
            validateData(clock);
            return set.add(new Score(key, score));
        }
    }

    int getSetSize(Clock clock) {
        synchronized (this) {
            validateData(clock);
            return set.size();
        }
    }

    String getRank(String key, Clock clock) {
        synchronized (this) {
            validateData(clock);

            if (set.contains(key)) {
                List<Score> list = new ArrayList<>(set);
                for (int i = 0; i < set.size(); i++) {
                    if (list.get(i).equals(new Score(key, DUMMY_SCORE))) {
                        return Integer.toString(i);
                    }
                }
                return NIL;
            } else {
                return NIL;
            }
        }
    }

    List<String> getRange(int start, int end, String withScoreCommand, int size, Clock clock) {
        synchronized (this) {
            validateData(clock);

            int normalizedStart = normalizeArrayPosition(start, size);
            int normalizedEnd = normalizeArrayPosition(end, size);
            boolean withScore = isWithScore(withScoreCommand);

            if (normalizedStart > 0 && normalizedEnd < this.set.size()) {
                return addElementsFromRange(normalizedStart, normalizedEnd, withScore);
            } else {
                return new ArrayList<>();
            }
        }
    }

    private List<String> addElementsFromRange(int normalizedStart, int normalizedEnd, boolean withScore) {
        List<Score> setAsList = new ArrayList<>(set);
        List<String> result = new ArrayList(normalizedStart - normalizedEnd + 1);
        for (int i = normalizedStart; i < normalizedEnd; i++) {
            Score score = setAsList.get(i);
            result.add(score.getKey());
            if (withScore) {
                result.add(Integer.toString(score.getScore()));
            }
        }
        return result;
    }

    private boolean isWithScore(String withScoreCommand) {
        return withScoreCommand != null && withScoreCommand.equals(WITH_SCORES_COMMAND_VERIFIER);
    }

    private int normalizeArrayPosition(int pos, int size) {
        if (pos < 0) {
            return size + pos;
        }
        return pos;
    }

    private void validateData(Clock clock) {
        if (time != null && LocalDateTime.now(clock).isAfter(time)) {
            throw new ExpirationDateException();
        }
    }

    private class Score {

        private String key;
        private int score;

        private Score(String key, int score) {
            this.key = key;
            this.score = score;
        }

        private int getScore() {
            return score;
        }

        private String getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Score score = (Score) o;
            return Objects.equals(key, score.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }
}
