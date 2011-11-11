package org.apache.avro;

import java.util.List;
//import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
/**
 * 
 * TO NOTE: 
 * 	1) Cannot write arrays. Can only write objects that inherit from java.util.List. This is due to 
 * 		the fact that the avro machinery tries to cast all inputs declared as arrays to java.util.List
 * 		...looking for a way around
 * 	2) input args are put in an Object[T],in which arrays are stored as GenericData.Array[T],
 * 		and strings are of class Utf8
 *
 */

public class JAvroInter{
	
	String OUTPUT_FILE_NAME;
	String INPUT_FILE_NAME;
	public Object[] inputs;
	
	public JAvroInter(String outputFile, String inputFile) throws IOException, IllegalAccessException,InstantiationException,ClassNotFoundException{
		OUTPUT_FILE_NAME = outputFile;
		INPUT_FILE_NAME = inputFile;
		this.readAvroFile();
	}
	
	//turn into switch statement
	public String getAvroType(Object item){
		if (item == null){
			return "\"null\"";
		}
		Class c = item.getClass();
		String name = c.getName();
		/**
		if (name == "java.util.Arrays$ArrayList" || name == "scala.collection.JavaConversions$MutableBufferWrapper" 
				|| name == "org.apache.avro.generic.GenericData$Array"){
		**/
		if (item instanceof List){
			// fix below for when arrays are empty??
			return String.format("{ \"type\":\"array\", \"items\": %s}", getAvroType(((List)item).get(0)));
		}		 
		else if (name == "java.lang.Double" || name == "double"){
			return "\"double\"";
		}
		else if (name == "java.lang.Integer" || name == "int"){
			return "\"int\"";
		}
		else if (name == "java.lang.String" || name == "org.apache.avro.util.Utf8"){
			return "\"string\"";
		}
		else if (name == "java.lang.Float" || name == "float"){
			return "\"float\"";
		}
		else if (name == "java.lang.Long" || name == "long"){
			return "\"long\"";
		}
		else if (name == "java.lang.Boolean" || name == "boolean"){
			return "\"boolean\"";
		}
		else {
			System.out.println(name);
			throw new RuntimeException("Unknown Argument Type to Write to Avro File");
		}
	}
	
	
	public String makeSchema(Object[] args){
		String schema = "{\n"
			+"\t\"type\": \"record\",\n"
			+"\t\"name\": \"args\",\n"
			+"\t\"namespace\": \"JAVAMODULE\",\n"
			+"\t\"fields\": [\n";		
		String size = "\t\t{ \"name\": \"size\"	, \"type\": \"int\"	}";
		if (args.length > 0){
			size += ",";
		}
		schema += size;
		String type, entry;
		int count = 1;
		for (Object arg: args){
			type = this.getAvroType(arg);
			entry = String.format("\n"
				+"\t\t{ \"name\": \"arg%d\"	, \"type\": %s	}", count, type);
			if (arg!= args[args.length-1]){
				entry += ",";
			}
			schema += entry;
			count += 1;
		}
		String close = "\n"
			+ "\t]\n}";
		schema += close;
		return schema;
	}
	
	public void writeAvroFile(Object[] args) throws IOException{
		
		String s= this.makeSchema(args);		
		Schema schema = (new Schema.Parser()).parse(s);		
		GenericRecord datum = new GenericData.Record(schema);
		
		datum.put("size", args.length);				
		int count = 1;
		for (Object arg: args){	
			datum.put(String.format("arg%d",count), arg);
			count++;
		}
				
		File file = new File(OUTPUT_FILE_NAME);
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		dataFileWriter.create(schema,file);
		dataFileWriter.append(datum);
		dataFileWriter.close();		
	}	
		
	public void readAvroFile() throws IOException, ClassNotFoundException, IllegalAccessException,InstantiationException{
		File file = new File(INPUT_FILE_NAME);
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,reader);
		
		GenericRecord record = dataFileReader.next();
		this.store(record);
	}
	
	/**
	 * this method takes the input data, presumably from args.avro, and stores it in the array inputs
	 */	
	public void store(GenericRecord record) throws InstantiationException, IllegalAccessException{
		int size = (java.lang.Integer)record.get("size");
		inputs = new Object[size];
		Object child, parent;
		for (int i=0; i < size; i++){
			inputs[i] = record.get(String.format("arg%d",i+1));		
		}		
	}	 
	
	public <T> T returnInput(int i){
		return (T)inputs[i];
	}

	// doesn't do recursive casting for arrays
	public <T> T[] returnArrayInput(int i, T[] example){
		return (T[])((List)(inputs[i])).toArray(example);
	}

		
	public void printInputs(){
		System.out.println("begin printing args");
		for (Object a: inputs){
			if (a == null){
				System.out.println("THE ARG... is null");
			}
			else {
				System.out.println("THE ARG..." + a);
				System.out.println("ITS CLASS..." + a.getClass());
			}
		}		
		System.out.println("end printing args");		
	}
	
	public static void main(String[] args) throws IOException, IllegalAccessException, ClassNotFoundException, InstantiationException{
		
		JAvroInter j = new JAvroInter("results.avro", "args.avro");
		//j.writeAvroFile();
		Object[] argz = j.inputs;		
		double[] a = {1,2,3,5};
		System.out.println("all good");
		System.out.println(argz[1] instanceof List);
		System.out.println(argz[1].getClass());
		
		//List lis = java.util.Arrays.asList(ArrayUtils.toObject(a));
	}
	/**
	 *  The below commented out methods were made in an attempt to convert avro given classes,
	 *  i.e. GenericData.Array to more conventional classes, like Object[]...but it seems far
	 *  far easier just to stick with the avro given classes to avoid tremendous casting
	 *  and peculiar type complications
	 * 
	 * 
	 * 
	// the input must have a subclass of GenericData.Array...intended to directly come from format()
	// the formatting of children of generic arrays that aren't more arrays could be done more 
	// cleanly...
	public Object[] convertGenericArray(Object genericArray)throws InstantiationException, IllegalAccessException{
		
		Object child = ((org.apache.avro.generic.GenericData.Array)genericArray).get(0);
		if (child.getClass().getName() == "org.apache.avro.generic.GenericData$Array"){
			for (int i=0; i < ((List)genericArray).size(); i++){
				((List)genericArray).set(i, this.convertGenericArray(((List)genericArray).get(i)));
			}
		}
		//same formatting changes that are below in format method, this isn't working??
		else if (child.getClass().getName() == "org.apache.avro.util.Utf8"){
			for (int i=0; i < ((List)genericArray).size(); i++){
				((List)genericArray).set(i, ((List)genericArray).get(i).toString());
			}
		}
		
		// how to figure out type 
		//need to instead use toArray(T[] a) to cast it to array of proper type, not Object[]
		
		return ((List)genericArray).toArray();
		//problem is what's going in is an object array, not an array of whatever type is necessary
		//could manually figure out 
		//return ((List)genericArray).toArray(this.makeArray(child.getClass().newInstance()));
		
		//return new Object[3];
	
	}	
	
	public <T> T[] makeArray(T item){		
		return (T[])java.lang.reflect.Array.newInstance(item.getClass(), 0);
	}
	
	public void format(int i){
		String className = inputs[i].getClass().getName();
		if (className == "org.apache.avro.util.Utf8"){
			inputs[i] = inputs[i].toString();			
			// other formatting changes that need to be done??
		}
	}
	**/		
}