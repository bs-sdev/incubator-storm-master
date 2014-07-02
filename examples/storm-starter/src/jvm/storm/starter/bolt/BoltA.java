package storm.starter.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class BoltA extends BaseRichBolt{
	OutputCollector _collector;
	
	// Déclaration d'un compteur partagé
	static int idGenerator = 0;
	
	private int id;
	
	@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      synchronized(BoltA.class) {
    	  this.id = idGenerator;
    	  idGenerator++;
    	  System.out.println("Je suis le bolt " + this.id);
      }
    }

    @Override
    public void execute(Tuple tuple) {
    	//System.out.println("Valeur du tuple => \"" + tuple + "\"");
    	synchronized(BoltA.class) {
    		System.out.println("BoltA tuple : " + tuple);
    		System.out.flush();
    	}
    	String res = tuple.toString();
    	
    	//System.out.println("Valeur 0 dans le tuple => " + res);
    	String delimiter = ", ";
    	
		// Tableau de chaines afin de récupérer, en dernière valeur, la moto
    	// Après observation dans le debug, ce sera à l'index 3 => tokensVal[3]
    	String[] tokensVal = res.split(delimiter);
    	
    	Values maValeur = new Values(tuple);
    	
    	// emission du tuple vers le Bolt auquel il est lié
    	_collector.emit(tuple, maValeur);
    	//_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	// A VOIR 
      declarer.declare(new Fields("res_A"));
    }

}
