/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;

import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

	  public static class ExclamationBolt extends BaseRichBolt {
	    OutputCollector _collector;
	    
	    CountMetric _countMetric;
		MultiCountMetric _wordCountMetric;
		ReducedMetric _wordLengthMeanMetric;

	    @Override
	    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	      _collector = collector;
	      /////////////
	      initMetrics(context);
	    }

	    @Override
	    public void execute(Tuple tuple) {
	      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
	      _collector.ack(tuple);
	      
	      updateMetrics(tuple.getString(0));
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("word"));
	    }
	    
	    void initMetrics(TopologyContext context){
	    	_countMetric = new CountMetric();
	    	_wordCountMetric = new MultiCountMetric();
	    	_wordLengthMeanMetric = new ReducedMetric(new MeanReducer());
	    	
	    	context.registerMetric("execute_count", _countMetric, 5);
	    	context.registerMetric("word_count", _wordCountMetric, 60);
	    	context.registerMetric("word_legnth", _wordLengthMeanMetric, 60);
	    }

	    void updateMetrics(String word){
	    	_countMetric.incr();
	    	_wordCountMetric.scope(word).incr();
	    	_wordLengthMeanMetric.update(word.length());
	    }
	    

	  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 10);
    builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
