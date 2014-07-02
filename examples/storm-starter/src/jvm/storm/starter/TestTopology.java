package storm.starter;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

import storm.starter.bolt.BoltA;
import storm.starter.spout.TestStatSpout;

/**
 * This is a basic example of a Storm topology.
 */

public class TestTopology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    /*builder.setSpout("bike", new BikeStatSpout(), 6);
    // On émet du spout vers 2 bolts, à savoir le bolt de puisance et le bolt de cylindrée
    builder.setBolt("puiss", new PuissanceTransformBolt(), 3).shuffleGrouping("bike");
    builder.setBolt("cylinder", new CylindreTransformBolt(), 3).shuffleGrouping("bike");
    // on regroupe les résutats de traitement de puissance et de cylindrée dans le bolt puisscylRegroup
    builder.setBolt("PuissCyl", new PuissCylRegroupBolt(), 3).fieldsGrouping("puiss", new Fields("bike-power")).fieldsGrouping("cylinder", new Fields("bike-engineSize"));
     */
    
    builder.setSpout("spout", new TestStatSpout(), 6);
    builder.setBolt("bolt1", new BoltA(), 3).shuffleGrouping("spout");
    builder.setBolt("bolt2", new BoltA(), 3).shuffleGrouping("bolt1");
    builder.setBolt("bolt3", new BoltA(), 3).shuffleGrouping("bolt2");
    builder.setBolt("bolt4", new BoltA(), 3).shuffleGrouping("bolt3");
    builder.setBolt("bolt5", new BoltA(), 3).shuffleGrouping("bolt4");
    builder.setBolt("bolt6", new BoltA(), 3).shuffleGrouping("bolt5");
    builder.setBolt("bolt7", new BoltA(), 3).shuffleGrouping("bolt6");
    builder.setBolt("bolt8", new BoltA(), 3).shuffleGrouping("bolt7");
    
    
    Config conf = new Config();
//    conf.setMessageTimeoutSecs(2);
//    conf.setMaxSpoutPending(3);
    conf.setDebug(true);

    
    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
      
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(3000);
      System.out.print("FIN DU FLUX");
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
