/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hwc;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class ShortestPath {
    private static final int MAX_ITERATIONS = 30;
    public static final long START_NODE_LONG = 17274l;
    public static final String START_NODE = "17274";

    public static void main(String[] args) throws Exception {

        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Read the input file using the Dataset API
        DataSet<String> inputDataset = env.readTextFile("src/main/resources/CA-GrQc.txt");

        // create edge pairs
        DataSet<Tuple2<Long, Long>> graph = inputDataset.map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String s) throws Exception {
                return new Tuple2<>(Long.parseLong(s.split("\t")[0].trim()), Long.parseLong(s.split("\t")[1].trim()));
            }
        });


        DataSet<Tuple3<Long, Long, String>> shortestPath = graph.map(new MapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, String>>() {
            @Override
            public Tuple3<Long, Long, String> map(Tuple2<Long, Long> graphTuple2) throws Exception {
                Tuple2<Long, Long> currentTuple = graphTuple2;
                if (currentTuple.f0 == START_NODE_LONG)
                    return new Tuple3<>(currentTuple.f0, 0l, START_NODE);
                return new Tuple3<>(currentTuple.f0, Long.MAX_VALUE, "");
            }
        }).distinct();

        int vertexID = 0;
        DeltaIteration<Tuple3<Long, Long, String>, Tuple3<Long, Long, String>> iteration =
                shortestPath.iterateDelta(shortestPath.filter(t -> t.f0 == START_NODE_LONG), MAX_ITERATIONS, vertexID);

        DataSet<Tuple3<Long, Long, String>> changes = iteration.getWorkset().join(graph).where(0).equalTo(0)
                .with(new JoinNeighbours())
                .join(iteration.getSolutionSet()).where(0).equalTo(0)
                .with(new UpdateDistances());

        DataSet<Tuple3<Long, Long, String>> result = iteration.closeWith(changes, changes);
        result.map(t -> "17274 | " + String.format("%1$-5s", t.f0) + " | " + String.format("%1$-80s", t.f2) + " | " + (t.f1 == Long.MAX_VALUE ? "INFINITY" : t.f1))
                .writeAsText("src/main/resources/output.txt").setParallelism(1);

        // execute program
        env.execute("Shortest Path Job");
    }
}


@ForwardedFieldsFirst("f1->f1")
@ForwardedFieldsSecond("f1->f0")
final class JoinNeighbours implements
        JoinFunction<Tuple3<Long, Long, String>, Tuple2<Long, Long>, Tuple3<Long, Long, String>> {

    @Override
    public Tuple3<Long, Long, String> join(Tuple3<Long, Long, String> vertex, Tuple2<Long, Long> edge) {
        return new Tuple3<Long, Long, String>(edge.f1, vertex.f1 + 1, vertex.f2 + ", " + edge.f1);
    }
}

@ForwardedFieldsFirst("*")
final class UpdateDistances implements
        FlatJoinFunction<Tuple3<Long, Long, String>, Tuple3<Long, Long, String>, Tuple3<Long, Long, String>> {

    @Override
    public void join(Tuple3<Long, Long, String> current, Tuple3<Long, Long, String> old, Collector<Tuple3<Long, Long, String>> out) {
        if (current.f1 < old.f1) {
            out.collect(current);
        }
    }

}
