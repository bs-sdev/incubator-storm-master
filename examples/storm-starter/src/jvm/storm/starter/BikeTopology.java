package storm.starter;


import storm.starter.bolt.CylindreTransformBolt;
import storm.starter.bolt.PuissCylRegroupBolt;
import storm.starter.bolt.PuissanceTransformBolt;
import storm.starter.spout.BikeStatSpout;
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

/**
 * This is a basic example of a Storm topology.
 */

public class BikeTopology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("bike", new BikeStatSpout(), 6);
    // On émet du spout vers 2 bolts, à savoir le bolt de puisance et le bolt de cylindrée
    builder.setBolt("puiss", new PuissanceTransformBolt(), 3).shuffleGrouping("bike");
    builder.setBolt("cylinder", new CylindreTransformBolt(), 3).shuffleGrouping("bike");
    // on regroupe les résutats de traitement de puissance et de cylindrée dans le bolt puisscylRegroup
    builder.setBolt("PuissCyl", new PuissCylRegroupBolt(), 3).fieldsGrouping("puiss", new Fields("bike-power")).fieldsGrouping("cylinder", new Fields("bike-engineSize"));

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      //StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      System.out.print("FIN DU FLUX");
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
