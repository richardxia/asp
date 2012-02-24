package org.apache.avro;

import java.util.List;
//import java.util.ArrayList;
import java.io.File;
import java.io.IOException;

//must add these below files to java classpath
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;


/*
 * 
 * 
 * how to set the java classpath so these files can be anywhere?
 * wait, where should they be put, if not in here?
 * oh, put them in the java packages files?
 * 
 * if we decide just to arbitrarily put them somewhere 
 */
import java.io.*;
/**
 * 
 * TO NOTE: 
 * 	1) Cannot write arrays. Can only write objects that inherit from java.util.List. This is due to 
 * 		the fact that the avro machinery tries to cast all inputs declared as arrays to java.util.List
 * 		...looking for a way around
 * 	2) input args are put in an Object[T],in which arrays are stored as GenericData.Array[T],
 * 		and strings are of class Utf8
 * 
 * 	3) when calling returnStored(index), must use the wrapper classes not primitive types. 
 * 		i.e. write Integer i = xed(index) instead of int i = x.returnStored(index)
 * 		or write Double d = x.returnStored(index) instead of double d = x.returnStored(index)
 *
 */

public class JAvroInter{
	
	String OUTPUT_FILE_NAME;
	String INPUT_FILE_NAME;
	Schema schema;
	public Object[] stored;
	
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
			if (count != args.length){
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
		this.schema = schema;
		
		GenericRecord datum = new GenericData.Record(schema);
		
		datum.put("size", args.length);				
		int count = 1;
		for (Object arg: args){	
			datum.put(String.format("arg%d",count), arg);
			count++;
		}				
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		if (OUTPUT_FILE_NAME == "System.out"){
			dataFileWriter.create(schema,System.out);
		}
		else {
			File file = new File(OUTPUT_FILE_NAME);
			dataFileWriter.create(schema,file);
		}
		dataFileWriter.append(datum);
		dataFileWriter.close();		
	}	
		
	public void readAvroFile() throws IOException, ClassNotFoundException, IllegalAccessException,InstantiationException{
		File file = new File(INPUT_FILE_NAME);
		double start= System.nanoTime();
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		double end = System.nanoTime();
		double elapsed = end-start;
		//System.out.println("rdinstant:"+elapsed);
		GenericRecord record;
		
		if (INPUT_FILE_NAME == "System.in"){
			DataFileStream dfs = new DataFileStream(System.in, reader);
			record = (GenericRecord)dfs.next();
		}
		else{
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file,reader);
			record = dataFileReader.next();
		}				
		this.store(record);
	}
	
	/**
	 * this method takes the input data, presumably from args.avro, and stores it in the array stored
	 */	
	public void store(GenericRecord record) throws InstantiationException, IllegalAccessException{
		int size = (java.lang.Integer)record.get("size");
		stored = new Object[size];
		Object item;
		for (int i=0; i < size; i++){
			//convert org.apache.avro.util.Utf8 to String format
			item = record.get(String.format("arg%d",i+1));
			if (item instanceof org.apache.avro.util.Utf8){
				stored[i] = item.toString();
			}
			else{
				stored[i] = item;
			}
		}		
	}	 
	
	/**
	 * returns the item in stored at the specified index.
	 */
	
	public <T> T returnStored(int index){
		System.stderr.println("INSIDE RETURN STORE FOR INDEX:" + 0);
		return stored[index].asInstanceOf[T]; 
	}
	
	/**
	 * returns the whole array stored
	 */
	
	public Object[] returnStored(){
		return stored;
	}
	
	/**
	 * Only use if a List subclass (i.e. GenericData.Array) is in stored[index].
	 * Converts the List subclass to an array of the type of the example.
	 * NOTE: does not recursively convert. i.e. a list of lists will be converted
	 * to an array of lists. 
	 */
	
	public <T> T[] returnStoredArray(int index, T[] example){
		return (T[])((List)(stored[index])).toArray(example);
	}

		
	public void printStored(){
		System.out.println("begin printing args");
		for (Object a: stored){
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
		/**
		double start = System.nanoTime();
		JAvroInter j = new JAvroInter("results.avro", "results.avro");
		double end = System.nanoTime();
		double elapsed = end- start;
		System.out.println("i-time:"+ elapsed);
		**/
	
		
		try{
		//BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		//String words = br.readLine();
		//System.out.println("sys in: "+ words);
		JAvroInter j = new JAvroInter("results.avro", "args.avro");
		
		j.printStored();
		
		Object[] arr = {8,9,10,22};
		j.writeAvroFile(arr);
		System.out.println("DONE");
		
		/**
		Integer d = j.returnStored(1);
		System.out.println("here's d: " + d);
		//String s = j.returnStored(2);
		//System.out.println(s);
		Object[] results = new Object[1];
		results[0]  = "asdfasdf";
		j.writeAvroFile(results);
		
		//need to print contents of system.out???????????
		//BufferedReader br = new BufferedReader(new InputStreamReader(System.out));
		//File f = new File("System.out");
		//BufferedReader br = new BufferedReader(new FileReader(f));
		
		//System.out.println("here's out: " + br.read());
		/**
		JAvroInter w = new JAvroInter("results.avro", "System.out");
		String q = w.returnStored(0);
		System.out.println("returned val:" + q);
		**/
		}
		catch(IOException ioe){
			System.out.println("caught urrror: "+ ioe);
		}
		
		
		
		/**
		System.out.println("begin making a");
		Object[] a = new Double[100000];
		for (int i=0; i<100000; i++){
			a[i] = 1.0;
		}
		System.out.println("end making a");
		
		start = System.nanoTime();
		//j.writeAvroFile(a);
		end = System.nanoTime();
		elapsed = end-start;
		System.out.println("w-time:"+ elapsed);
		
		start = System.nanoTime();
		Double str = j.returnStored(1);
		end = System.nanoTime();
		elapsed = end-start;
		System.out.println("r-time:" + elapsed);
		
		Object[] arr = {1,2,3};
		int i = (Integer)(arr[1]);
		//List lis = java.util.Arrays.asList(ArrayUtils.toObject(a));
		**/ 
		 
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
		String className = stored[i].getClass().getName();
		if (className == "org.apache.avro.util.Utf8"){
			stored[i] = stored[i].toString();			
			// other formatting changes that need to be done??
		}
	}
	**/		
}