package redis.db;

import redis.exception.*;

import java.time.*;
import java.util.*;

import static redis.db.ZaddArgs.*;

class DataOperations {
    private static final int DUMMY_SCORE = 0;
    private static final String NIL = "nil";
    private static final String WITH_SCORES_COMMAND_VERIFIER = "WITHSCORES";
    private volatile int data;
    private volatile SortedSet<Score> set = new TreeSet<>(Comparator.comparing(Score::getScore));
    private volatile LocalDateTime time;

    //TODO: need to impl XX,NX,CH,INCR
    int addScores(Set<ZaddArgs> args, List<String> scores, Clock clock) {
        if (args.contains(INCR) && scores.size() != 2) {
            throw new IncrOptionPreConditionException();
        }
        int count = 0;
        for (int i = 0; i < scores.size(); i = i + 2) {
            count += addScore(scores.get(i), Integer.parseInt(scores.get(i + 1)), clock, args);
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

    private int addScore(String key, int score, Clock clock, Set<ZaddArgs> args) {
        validateData(clock);

        if (set == null) {
            set = new TreeSet<>();
        }

        if (set.contains(key) && (!args.contains(NX) || args.contains(INCR))) {
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
            return args.contains(CH) ? 1 : 0;
        }
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
