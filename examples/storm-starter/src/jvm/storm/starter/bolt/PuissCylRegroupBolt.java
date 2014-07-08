package storm.starter.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PuissCylRegroupBolt extends BaseRichBolt{
	OutputCollector _collector;
	
	// déclaration du tableau de compteur
	// Il permettra de répertorier le nombre de motos dont la puissance est de catégorie A, B ou C
	// Il permettra de répertorier le nombre de motos dont la cyindrée est de catégorie S, M ou L
	static Map<String,Integer> counterHashMap = new HashMap<String,Integer>();

	// Compteur de tupleTotal reçu
	static int cptTuple = 0;
	
	// Compteur de tuple de puissance
	static int cptPower = 0;
	
	// Compteur de tuple de cylindrée
	static int cptEngineSize = 0;
	
	@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      synchronized(PuissCylRegroupBolt.class) {
	      counterHashMap.put("nbA", 0);
	      counterHashMap.put("nbB", 0);
	      counterHashMap.put("nbC", 0);
	      counterHashMap.put("nbS", 0);
	      counterHashMap.put("nbM", 0);
	      counterHashMap.put("nbL", 0);
      }
    }

    @Override
    public synchronized void execute(Tuple tuple) {
    	//System.out.println("PUISSCYLREGROUP : Valeur du tuple => \"" + tuple + "\"");
    	
        // Parsing du fichier JSON pour parcours ultérieur
		try {
			JSONObject jsonMoto = getJSON(tuple);
	    	
			System.out.println("On a parsé le string en objet JSON, son contenu : " + jsonMoto.get("engineSize") + ", " + jsonMoto.get("name") + ", " + jsonMoto.get("power"));
		
			String powerToCheck = "", engineSizeToCheck = "";
			
			synchronized(PuissCylRegroupBolt.class) {
				cptTuple++;
				System.out.println("PUISSCYLREGROUP Compteur de tuple reçu : " + cptTuple);
    			System.out.println("tuple reçu par PUISSCYLREGROUP" + tuple);
			}
				
			// Compteur temporaire qui contiendra l'ancienne valeur du compteur dans la Map (ex: ancienne valeur de nbC)
			int cptTemp = 0;
			
			// Récupération de la puissance à tester
			if(jsonMoto.get("power") instanceof String){
				///////
				powerTransform(jsonMoto);
				///////
			}
				
			if(jsonMoto.get("engineSize") instanceof String) {
				///////
				engineSizeTransform(jsonMoto);
				///////
			}
			// Affichage du tableau de compteur
			synchronized(PuissCylRegroupBolt.class) {
				System.out.println("PUISSCYLREGROUP CPTTUPLE " + cptTuple + " ET CPTPOWER " + cptPower + " ET CPTENGINESIZE " + cptEngineSize);
				for(Entry<String, Integer> entry : counterHashMap.entrySet()){
		    		System.out.println(entry.getKey() + " => " + entry.getValue());
		    	}
			}

		} catch (/*ParseException*/Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("PUISSCYLREGROUP : FIN");
		/*synchronized(PuissCylRegroupBolt.class) {
			showCounters();
		}*/
    }

    // Fonction de transformation de la puissance issue du tuple parsé en JSON précédemment.
    public void powerTransform(JSONObject jsonMoto){
    	// Entier pour tester le type entier de la puissance
		int testIntPower;
		
		String powerToCheck = "";
		
		int cptTemp = 0;
		
		// On implémente un système de try/catch afin de constater si le format de la puissance reçue
		// via le json (issu du tuple) est de type entier ou non. Si de type entier, on ne fait rien, si de
		// type String, alors on effectue le traitement
		try{
			testIntPower = Integer.parseInt((String)jsonMoto.get("power"));
		}
		// On a un type String, ce que l'on recherche, on effectue donc le traitement
		catch(NumberFormatException e){
			System.out.println("Puissance test :  Le format n'étant pas un entier, on effectue le traitement");
			
			powerToCheck = (String) jsonMoto.get("power");
			System.out.println("POWER VAUT : " + powerToCheck);
			
			if(powerToCheck.equalsIgnoreCase("A")){
				synchronized(PuissCylRegroupBolt.class) {
					cptTemp = counterHashMap.get("nbA");
					cptTemp++;
					cptPower++;
					//System.out.println("Valeur de CPTTEMP = " + cptTemp + " et POWER " + powerToCheck);
					counterHashMap.put("nbA", cptTemp);	
				}
	    	}	
	    	else if(powerToCheck.equalsIgnoreCase("B")){
	    		synchronized(PuissCylRegroupBolt.class) {
		    		cptTemp = counterHashMap.get("nbB");
		    		cptTemp++;
		    		cptPower++;
		    		//System.out.println("Valeur de CPTTEMP = " + cptTemp + " et POWER " + powerToCheck);
					counterHashMap.put("nbB", cptTemp);	
	    		}
	    	}
	    	else if(powerToCheck.equalsIgnoreCase("C")){
	    		synchronized(PuissCylRegroupBolt.class) {
		    		cptTemp = counterHashMap.get("nbC");
		    		cptTemp++;
		    		cptPower++;
		    		//System.out.println("Valeur de CPTTEMP = " + cptTemp + " et POWER " + powerToCheck);
					counterHashMap.put("nbC", cptTemp);	
	    		}
	    	}
		}
    }
    
    
    
    public void engineSizeTransform(JSONObject jsonMoto){
    	// Entier pour tester le type entier de la cylindrée
		int testIntEngine;
		
		String engineSizeToCheck = "";
		
		int cptTemp = 0;
		
		// On implémente un système de try/catch afin de constater si le format de la cylindrée reçue
		// via le json (issu du tuple) est de type entier ou non. Si de type entier, on ne fait rien, si de
		// type String, alors on effectue le traitement
		try{
			testIntEngine = Integer.parseInt((String)jsonMoto.get("engineSize"));
		}
		// On a un type String, ce que l'on recherche, on effectue donc le traitement
		catch(NumberFormatException e){
			System.out.println("Cylindrée test :  Le format n'étant pas un entier, on effectue le traitement");
			
			engineSizeToCheck = (String) jsonMoto.get("engineSize");
			System.out.println("ENGINESIZE VAUT : " + engineSizeToCheck);
			
			if(engineSizeToCheck.equalsIgnoreCase("S")){
				synchronized(PuissCylRegroupBolt.class) {
					cptTemp = counterHashMap.get("nbS");
					cptTemp++;
					cptEngineSize++;
					//System.out.println("Valeur de CPTTEMP = " + cptTemp);
					counterHashMap.put("nbS", cptTemp);
				}
			}
			else if(engineSizeToCheck.equalsIgnoreCase("M")){
				synchronized(PuissCylRegroupBolt.class) {
					cptTemp = counterHashMap.get("nbM");
					cptTemp++;
					cptEngineSize++;
					//System.out.println("Valeur de CPTTEMP = " + cptTemp);
					counterHashMap.put("nbM", cptTemp);
				}
			}
			else if(engineSizeToCheck.equalsIgnoreCase("L")){
				synchronized(PuissCylRegroupBolt.class) {
					cptTemp = counterHashMap.get("nbL");
					cptTemp++;
					cptEngineSize++;
					//System.out.println("Valeur de CPTTEMP = " + cptTemp);
					counterHashMap.put("nbL", cptTemp);
				}
			}
		}
    }
    
    
    
    // Fonction permettant de transformer un tuple pour en récupérer que les informations nécessaires
    // à savoir, l'objet "moto"
    public JSONObject getJSON(Tuple tuple){
    	// Transformation du tuple en string
    	String res = tuple.toString();
    	
    	//System.out.println("Valeur 0 dans le tuple => " + res);
    	String delimiter = ", ";
    	
		// Tableau de chaines afin de récupérer, en dernière valeur, la moto
    	// Après observation dans le debug, ce sera à l'index 3 => tokensVal[3]
    	String[] tokensVal = res.split(delimiter);
    	
    	/*for(String val : tokensVal){
    		System.out.println("	BRAAAAAAAAAA Valeur reçue ===> " + val);
    	}*/
    	
    	// Récupération de la longueur pour avoir le dernier index de chaine et enlever le caractère "]"
    	int longueur = tokensVal[6].length();
    	
    	// On élimine les crochets pour obtenir le champs JSON
    	String moto = tokensVal[6].substring(1, (longueur - 2));
    	
    	//System.out.println("	BRAAAAAAAAAA Valeur 6 = " + moto);
    	
    	//System.out.println("MAJ Moto \"" + moto + "\"");
    	
    	// Declaration du parser JSON
        JSONParser parser = new JSONParser();
    	
        // Déclaration d'un object pour le résultat du parse ultérieur
        Object obj;
        
        JSONObject jsonMoto = new JSONObject();
        
        try{
	        // Parsing de la chaine de caractère contenant la moto en objet JSON
	        obj = parser.parse(moto);
	     	
	        // Récupération de la moto
	     	jsonMoto = (JSONObject) obj;
    	}
        catch(Exception e){
        	e.printStackTrace();
        }
	     	
    	return jsonMoto;
    }
    
    
    // Fonction permettant l'affichage des compteurs de cylindrée et de puissance
    // Affichage du tableau de compteur (comme pb de synchro, j'ai mis le contenu de 
    // la fonction diirectement dans le code de execute un peu plus haut)
    public void showCounters(){
    	for(Entry<String, Integer> entry : counterHashMap.entrySet()){
    		System.out.println(entry.getKey() + " => " + entry.getValue());
    	}
    }
    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	// A VOIR 
      declarer.declare(new Fields("resultGrouped"));
    }

}
