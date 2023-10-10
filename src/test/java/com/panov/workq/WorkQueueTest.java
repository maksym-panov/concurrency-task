package com.panov.workq;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.*;

public class WorkQueueTest {

    @Test
    @DisplayName("Works with complex tasks and results")
    void worksWithComplexTasks() throws InterruptedException {
        // given
        List<String> task1 = new ArrayList<>() {{ add("test1"); add("test2"); add("test3"); }};
        List<String> task2 = new ArrayList<>() {{ add("test1"); add("test2"); add("test2"); }};
        BiFunction<List<String>, WorkQueue<List<String>, Set<String>>, Set<String>> handler =
                (l, wq) -> {
                    Set<String> resultSet = new HashSet<>();
                    resultSet.addAll(l);
                    return resultSet;
                };
        var underTest = new WorkQueue<>(handler, 10, 3, 1000);
        // when
        underTest.add(task1);
        underTest.add(task2);
        List<Set<String>> results = underTest.execute();
        // then
        assertThat(results).isNotNull();
        assertThat(results).hasSize(2);
        assertThat(results.get(0)).hasSize(3);
        assertThat(results.get(1)).hasSize(2);
        assertThat(results.get(0)).containsAll(List.of("test1", "test2", "test3"));
        assertThat(results.get(1)).containsAll(List.of("test1", "test2"));
    }

    @Test
    @DisplayName("Maintains the same order of results as it was in tasks")
    void maintainsOrder() throws InterruptedException {
        // given
        String t1 = "task1";
        String t2 = "task22";
        String t3 = "task333";
        String t4 = "task4444";
        BiFunction<String, WorkQueue<String, Integer>, Integer> handler =
                (str, wq) -> str.length();
        var underTest = new WorkQueue<>(handler, 10, 2, 2000);
        // when
        underTest.addAll(List.of(t1, t2, t3, t4));
        List<Integer> results = underTest.execute();
        // then
        assertThat(results).isNotNull();
        assertThat(results).hasSize(4);
        assertThat(results.get(0)).isEqualTo(t1.length());
        assertThat(results.get(1)).isEqualTo(t2.length());
        assertThat(results.get(2)).isEqualTo(t3.length());
        assertThat(results.get(3)).isEqualTo(t4.length());
    }

    @Test
    @DisplayName("Does not accept more elements than provided MAX_QUEUE_SIZE")
    void innerQueueHasElementsLimit() {
        // given
        String t1 = "1", t2 = "2", t3 = "3", t4 = "4", t5 = "5";
        BiFunction<String, WorkQueue<String, String>, String> handler = (t, wq) -> t;
        var underTest = new WorkQueue<>(handler, 3, 1, 1000);
        // when
        underTest.addAll(List.of(t1, t2, t3));
        // then
        assertThatThrownBy(
                () -> underTest.add(t4)
        ).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                () -> underTest.add(t4)
        ).hasMessage("Tasks queue limit is reached");
        assertThatThrownBy(
                () -> underTest.addAll(List.of(t4, t5))
        ).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                () -> underTest.addAll(List.of(t4, t5))
        ).hasMessage("Tasks queue limit is reached");
    }

    @Test
    @DisplayName("Does not work longer than TIMEOUT")
    void doesNotWorkLongerThanTimeout() {
        // given
        String task = "task";
        BiFunction<String, WorkQueue<String, String>, String> handler = (t, wq) -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return t;
        };
        var underTest = new WorkQueue<>(handler, 5, 3, 1000);
        // when
        underTest.add(task);
        // then
        assertThatThrownBy(underTest::execute).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(underTest::execute).hasMessage("Process has been executing too long");
    }

    @Test
    @DisplayName("Does not execute more tasks simultaneously than MAX_WORKERS_NUM")
    void hasExecutingThreadsLimit() throws InterruptedException {
        // given
        String task1 = "task1";
        String task2 = "task2";
        String task3 = "task3";
        String task4 = "task4";
        BiFunction<String, WorkQueue<String, String>, String> handler = (t, wq) -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return t;
        };
        var underTest = new WorkQueue<>(handler, 10, 2, 10000);
        // when
        underTest.addAll(List.of(task1, task2, task3, task4));
        underTest.execute();
        // then
        Thread.sleep(1000);
        assertThat(Thread.activeCount()).isEqualTo(1 + 2);
        Thread.sleep(2000);
        assertThat(Thread.activeCount()).isEqualTo(1 + 2);
    }

    @Test
    @DisplayName("Uses workers efficiently")
    void usesWorkersEfficiently() throws InterruptedException {
        // given
        int task1 = 200;
        int task2 = 400;
        int task3 = 800;
        int task4 = 500;
        BiFunction<Integer, WorkQueue<Integer, Integer>, Integer> handler = (t, wq) -> {
            try {
                Thread.sleep(t);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return t;
        };
        var underTest = new WorkQueue<>(handler, 10, 5, 1000);
        // when
        underTest.addAll(List.of(task1, task2, task3, task4));
        var result = underTest.execute();
        // then
        assertThat(result).isNotNull();
        assertThat(result).hasSize(4);
        assertThat(result.get(0)).isEqualTo(200);
        assertThat(result.get(1)).isEqualTo(400);
        assertThat(result.get(2)).isEqualTo(800);
        assertThat(result.get(3)).isEqualTo(500);
    }

    @Test
    @DisplayName("Accepts and executes tasks even after execution started")
    void acceptsNewTasksDuringExecution() throws InterruptedException {
        // given
        String task1 = "task1";
        String task2 = "2ksat";
        BiFunction<String, WorkQueue<String, String>, String> handler = (t, wq) -> {
            if (t.length() == 5) {
                wq.add(t.substring(1));
                wq.addAll(List.of(t.substring(2), t.substring(3)));
            }
            return t;
        };
        var underTest = new WorkQueue<>(handler, 10, 4, 1000);
        // when
        underTest.add(task1);
        underTest.add(task2);
        List<String> results = underTest.execute();
        // then
        assertThat(results).isNotNull();
        assertThat(results).hasSize(8);
        assertThat(results).containsAll(List.of(
            "task1", "ask1", "sk1", "k1",
            "2ksat", "ksat", "sat", "at"
        ));
    }
}
