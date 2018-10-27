package redis.db;

import redis.exception.*;

import java.time.*;
import java.util.*;

import static java.util.Collections.sort;
import static redis.db.ZaddArgs.*;

class DataOperations {
    private static final int DUMMY_SCORE = 0;
    private static final String NIL = "nil";
    private volatile int data;
    private volatile Set<Score> set = new HashSet<>();
    private volatile LocalDateTime time;

    //TODO: need to impl XX,NX,CH,INCR
    int addScores(Set<ZaddArgs> args, List<String> scores, Clock clock) {
        if (args.contains(INCR) && scores.size() != 2) {
            throw new IncrOptionPreConditionException();
        }
        int count = 0;
        for (int i = 0; i < scores.size(); i = i + 2) {
            count += addScore(scores.get(i + 1), Integer.parseInt(scores.get(i)), clock, args);
        }
        return count;
    }

    String getData(Clock clock) {
        synchronized (this) {
            validateData(clock);
            return Integer.toString(data);
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

            if (set.contains(new Score(key, DUMMY_SCORE))) {
                List<Score> list = new ArrayList<>(set);
                Collections.sort(list, Comparator.comparing(Score::getKey));
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

    List<String> getRange(int start, int end, ZRangeArgs withScoreCommand, int size, Clock clock) {
        synchronized (this) {
            validateData(clock);

            int normalizedStart = normalizeArrayPosition(start, size);
            int normalizedEnd = normalizeArrayPosition(end, size);
            boolean withScore = isWithScore(withScoreCommand);

            if (normalizedStart >= 0 && normalizedEnd < this.set.size()) {
                return addElementsFromRange(normalizedStart, normalizedEnd, withScore);
            } else {
                return new ArrayList<>();
            }
        }
    }

    int increment(Clock clock) {
        synchronized (this) {
            validateData(clock);
            this.data = data + 1;
            return data;
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

    private List<String> addElementsFromRange(int normalizedStart, int normalizedEnd, boolean withScore) {
        List<Score> setAsList = new ArrayList<>(set);
        sort(setAsList, Comparator.comparing(Score::getKey));
        List<String> result = new ArrayList();
        for (int i = normalizedStart; i < normalizedEnd; i++) {
            Score score = setAsList.get(i);
            result.add(score.getKey());
            if (withScore) {
                result.add(Integer.toString(score.getScore()));
            }
        }
        return result;
    }

    private int addScore(String key, int score, Clock clock, Set<ZaddArgs> args) {
        validateData(clock);

        if (set == null) {
            set = new TreeSet<>();
        }

        //TODO: o contains por algum motivo nao consegue achar o valor
        if (set.contains(new Score(key, DUMMY_SCORE)) && (!args.contains(NX) || args.contains(INCR))) {
            int scoreUpdated = updateScore(key, score, args.contains(INCR));
            return convertResult(args, scoreUpdated);

        } else if (!args.contains(XX)) {
            set.add(new Score(key, score));
            return 1;
        }

        throw new UnrecognizedArgException();
    }

    private int convertResult(Set<ZaddArgs> args, int scoreUpdated) {
        if (args.contains(INCR)) {
            return scoreUpdated;
        } else {
            return args.contains(CH) ? 1 : scoreUpdated;
        }
    }

    private boolean isWithScore(ZRangeArgs withScoreCommand) {
        return withScoreCommand != null;
    }

    private int normalizeArrayPosition(int pos, int size) {
        if (pos < 0) {
            return size + pos;
        }
        return pos;
    }

    private int updateScore(String key, int scoreNumber, boolean isINCR) {
        Score score = set.stream().filter(s -> s.getKey().equals(key))
                .findFirst()
                .orElseThrow(NullPointerException::new);
        score.setScore(isINCR ? score.getScore() + scoreNumber : scoreNumber);
        return score.getScore();
    }

    private void validateData(Clock clock) {
        if (time != null && LocalDateTime.now(clock).isAfter(time)) {
            throw new ExpirationDateException();
        }
    }

    private static class Score {

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

        private void setScore(int score) {
            this.score = score;
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
