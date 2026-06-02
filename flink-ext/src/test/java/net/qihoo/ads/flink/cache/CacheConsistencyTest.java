package net.qihoo.ads.flink.cache;

import java.util.*;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author zhuming
 * @date 2022/6/6 18:23
 */
class Person {
    public String name;
    public int age;
    private static Person person;
    static {
        person = new Person("360", 30);
    }
    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    //singleton class
    public static Person getInstance(){
        return person;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return age == person.age && Objects.equals(name, person.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age);
    }

    public Person agg(Person person){
        return new Person(person.name+this.name, person.age+this.age);
    }

}
class ConsistencyTestSource extends RichParallelSourceFunction<Person> {
    @Override
    public void run(SourceContext<Person> ctx) {
        ctx.collect(Person.getInstance());
    }

    @Override
    public void cancel() {

    }
}

class ConsistencyTestAggFunc extends CacheAggFunc<Person, Person, Person> {

    @Override
    public Person createAccumulator() {
        return  new Person("", 0);
    }

    @Override
    public Person add(Person value, Person accumulator) {
        return value.agg(accumulator);
    }

    @Override
    public Person getResult(Person accumulator) {
        return accumulator;
    }

    @Override
    public Person merge(Person a, Person b) {
        return a.agg(b);
    }
}

public class CacheConsistencyTest {

    private void doTest(int curTimes, int totalTimes) throws Exception {
        if (curTimes <= totalTimes) {
            System.out.println("consistency test time " + curTimes + "/" + totalTimes);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.getCheckpointConfig().setCheckpointInterval(100);
            env.getConfig().setAutoWatermarkInterval(50L);

            DataStream<Person> data = env.addSource(new ConsistencyTestSource()).setParallelism(1)
                    .transform("cacheAggregator",
                            BasicTypeInfo.of(Person.class),
                            new CacheAggregator<>(new ConsistencyTestAggFunc(), v -> v.name
                                    , 100, 10,100,3000, false)).setParallelism(1)
                    .process(new ProcessFunction<Person, Person>() {
                        @Override
                        public void processElement(Person person, ProcessFunction<Person, Person>.Context context, Collector<Person> collector)  {
                            collector.collect(person);
                        }
                    });
            Iterator<Person> outputIter = data.executeAndCollect();

            while (outputIter.hasNext()) {
                // when POJO passed a flink operator, class native reference will be changed. so just compare POJO by overriding hashcode
                Assert.assertEquals(Person.getInstance(), outputIter.next());
            }



        }
    }

    @Test
    public void testConsistency() throws Exception {
        int totalTimes = 1;
        doTest(1, totalTimes);
    }

}



