package storm.starter.spout;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.starter.spout.BikeStatSpout;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class BikeStatSpout extends BaseRichSpout{
	 public static Logger LOG = LoggerFactory.getLogger(BikeStatSpout.class);
	    boolean _isDistributed;
	    SpoutOutputCollector _collector;
	    
	    // Déclaration d'un compteur d'émission de tuple
	    static int cptTupleSpout = 0;

	    public BikeStatSpout() {
	        this(true);
	    }

	    public BikeStatSpout(boolean isDistributed) {
	        _isDistributed = isDistributed;
	    }


	    // Dans cette fonction, on récupère toutes les motos issue d'un fichier JSON ???? À voir avec M. TEDESCHI
	    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	        _collector = collector;
	    }

	    public void close() {

	    }

	    // Dans cette fonction, nous allons émettre toutes les motos vers les Bolt
	    public synchronized void nextTuple() {
	        Utils.sleep(100);

		    // Declaration du parser JSON
	        JSONParser parser = new JSONParser();
	        
			try {
				// Obtention du path pour acceder au fichier JSON pour le traitement
				System.out.println(System.getProperty("user.dir"));
				
				// Accès au fichier JSON contenant les données a traiter
				FileReader myFile = new FileReader(System.getProperty("user.dir") + "/src/jvm/storm/starter/spout/base_2roue.json");
				
				// Parsing du fichier JSON pour parcours ultérieur
				Object obj = parser.parse(myFile);
				
				// Cast en objet JSON
				JSONObject jsonObject = (JSONObject) obj;

				// Déclaration du tableau qui contiendra chaque entrée du fichier json (contiendra toutes les motos)
				JSONArray array = new JSONArray();
				
				// Récupération des motos
				array = (JSONArray) jsonObject.get("motorcycles");
				
				// System.out.println("res array =>" + array);
				
				// parcours du fichier JSON pour afficher chaque moto
				for(int i = 0; i < array.size(); i++) {
					 _collector.emit(new Values(array.get(i)));
					 synchronized(BikeStatSpout.class) {
						 cptTupleSpout++;
						 System.out.println("BIKESTATSPOUT : cptTupleSpout => " + cptTupleSpout);
						 System.out.flush();
					 }
				}
			} 
			catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("ERREUR DE FICHIER");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("ERREUR D'I/O");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("ERREUR DE PARSE");
			}
	        
	        /*final String[] words = new String[] {"GSXR", "R6", "Ninja", "CBR"};
	        final Random rand = new Random();
	        final String word = words[rand.nextInt(words.length)];
	        _collector.emit(new Values(word));*/
	    }

	    public void ack(Object msgId) {

	    }

	    public void fail(Object msgId) {

	    }

	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("bikes"));
	    }

	    public Map<String, Object> getComponentConfiguration() {
	        if(!_isDistributed) {
	            Map<String, Object> ret = new HashMap<String, Object>();
	            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
	            return ret;
	        } else {
	            return null;
	        }
	    }
}
