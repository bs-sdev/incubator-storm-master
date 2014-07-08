package storm.starter.bolt;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class CylindreTransformBolt extends BaseRichBolt{
	OutputCollector _collector;
	
	// Déclaration d'un compteur partagé
	static int nbTuple = 0;
	
	@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public synchronized void execute(Tuple tuple) {
    	//System.out.println("CYLINDRETRANSFORM : Valeur du tuple => \"" + tuple + "\"");
    	synchronized(CylindreTransformBolt.class) {
    		nbTuple++;
    		System.out.println("CYLINDRETRANSFORM nbTuple : " + nbTuple);
    		System.out.flush();
    	}
    	String res = tuple.toString();
    	
    	//System.out.println("Valeur 0 dans le tuple => " + res);
    	String delimiter = ", ";
    	
		// Tableau de chaines afin de récupérer, en dernière valeur, la moto
    	// Après observation dans le debug, ce sera à l'index 3 => tokensVal[3]
    	String[] tokensVal = res.split(delimiter);
    	
    	/*for(String val : tokensVal){
    		System.out.println("Valeur reçue ===> " + val);
    	}*/
    	
    	// Récupération de la longueur pour avoir le dernier index de chaine et enlever le caractère "]"
    	int longueur = tokensVal[3].length();
    	
    	// On élimine les crochets pour obtenir le champs JSON
    	String moto = tokensVal[3].substring(1, (longueur - 1));
    	
    	//System.out.println("MAJ Moto \"" + moto + "\"");
    	
    	// Declaration du parser JSON
        JSONParser parser = new JSONParser();
    	
     // Parsing du fichier JSON pour parcours ultérieur
		try {
			// Parsing de la chaine de caractère contenant la moto en objet JSON
			Object obj = parser.parse(moto);
			
			JSONObject jsonMoto = (JSONObject) obj;
	    	
			//System.out.println("On a parsé le string en objet JSON, son contenu : " + jsonMoto.get("cylinder") + ", " + jsonMoto.get("name") + ", " + jsonMoto.get("power"));
		
			// Récupération de la puissance à tester
			int engineSizeToCheck = Integer.parseInt((String) jsonMoto.get("engineSize"));
			
			String engineSizeModifiedField = "";
			
			// Comparaison des cylindrées
			if(engineSizeToCheck <= 600){
				engineSizeModifiedField = "S";
	    	}	
	    	else if(engineSizeToCheck > 600 && engineSizeToCheck <= 900){
	    		engineSizeModifiedField = "M";
	    	}
	    	else{
	    		engineSizeModifiedField = "L";
	    	}
			// On modifie la partie "power" de la portion JSON manipulée
			jsonMoto.put("engineSize", engineSizeModifiedField);
			
			tokensVal[3] = "[" + jsonMoto + "]";
			
			// Reconstruction du tuple pour l'ajout dans le collector du Bolt
			res = tokensVal[0] + ", " + tokensVal[1] + ", " + tokensVal[2] + ", " + tokensVal[3] ;
			
			Values maValeur = new Values(res);
			
			// emission du tuple vers le Bolt auquel il est lié
			_collector.emit(tuple, maValeur);
    		_collector.ack(tuple);
    		
    		//System.out.println("CYLINDRETRANSFORM : Valeur ajoutée dans le collector => " + new Values(res));
    		synchronized(CylindreTransformBolt.class) {
    			System.out.println("CYLINDRETRANSFORM Compteur de tuple reçu : " + nbTuple);
    			System.out.println("tuple reçu par CYLINDRETRANSFORM" + tuple);
    			System.out.println("tuple émis par CYLINDRETRANSFORM" + maValeur);
    			System.out.flush();
    		}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//System.out.println("CYLINDRETRANSFORM Compteur de tuple reçu : " + nbTuple);
		
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	// A VOIR
      declarer.declare(new Fields("bike-engineSize"));
    }
}
