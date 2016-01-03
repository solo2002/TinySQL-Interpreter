
import java.awt.EventQueue;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.Block;
import storageManager.Disk;
import storageManager.Field;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.SchemaManager;
import storageManager.Tuple;

public class TinyParser {
	public SchemaManager schema_manager;
	public MainMemory mem;
	public Disk disk;
	public long start = 0;
	private File outputFile;
	public TinyParser()
	{
		mem = new MainMemory();
	    disk = new Disk();
	    schema_manager = new SchemaManager(mem,disk);
	    outputFile = new File("outputResult.txt");
	    try(PrintWriter out = new PrintWriter(outputFile))
	    {//print results to a file
	    	out.println("Queries executes:");
	    	out.close();
	    } catch (FileNotFoundException e)
	    {
	    	e.printStackTrace();
	    } catch (IOException e1)
	    {
	    	e1.printStackTrace();
	    }
	    start = System.currentTimeMillis(); 
		disk.resetDiskIOs();
		disk.resetDiskTimer();
	}
	public void timeReset()
	{
		start = System.currentTimeMillis(); 
		disk.resetDiskIOs();
		disk.resetDiskTimer();	
	}
	public void timeCalculartor()
	{
	    System.out.print("Calculated elapse time = " + disk.getDiskTimer() + " ms" + "\n");
	    System.out.print("Calculated Disk I/Os = " + disk.getDiskIOs() + "\n");
	    System.out.println("===============================");
	    System.out.println();
	}
	public void parser(String str)
	{  
		Pattern p = null;
		Matcher m = null;
		String patternCreate = "^CREATE TABLE ([\\w]+) \\((.*)\\)$";//match CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)
		String patternDrop = "^DROP TABLE ([\\w]+)$";//match DROP TABLE table_name
		String patternDelC = "^DELETE FROM ([\\w]+) WHERE ([\\w]+) = \"(.*)\"$"; //DELETE FROM course WHERE grade = "E"
		String patternDel = "^DELETE FROM ([\\w]+)$";//DELETE FROM course 
		String patternInsert = "^INSERT INTO ([\\w]+) \\((.*)\\) VALUES \\((.*)\\)$";//match insert with values
		String patternInsertFrom = "^INSERT INTO ([\\w]+) \\((.*)\\) SELECT \\* FROM ([\\w]+)$";//match INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course
		String selectAll = "^SELECT \\* FROM (.*)$";//match SELECT * FROM course
		String select2 = "^SELECT (.*) FROM ([\\w]+)$";//SELECT sid, course.grade FROM course; SELECT sid, grade FROM course
		String selectDistinct = "^SELECT DISTINCT (.*) FROM ([\\w]+)$";//SELECT DISTINCT grade FROM course; SELECT DISTINCT * FROM course
		String selectAllOrder = "^SELECT (.*) FROM ([\\w]+) ORDER BY ([\\w]+)$";//SELECT * FROM course ORDER BY exam
		String selectPlus = "^SELECT (.*) FROM ([\\w]+) WHERE (.*)$"; //= ([\\w]+)$";//SELECT * FROM course WHERE exam + homework = 200
		String selectWhereOrder = "^SELECT \\* FROM (\\w*), (\\w*) WHERE \\1.(\\w*) = \\2.\\3 ORDER BY (.*)$";//SELECT * FROM course, course2 WHERE course.sid = course2.sid ORDER BY course.exam
		String projectNatrualJoin = "^SELECT ([a-z0-9 ,\\.]*) FROM (\\w*), (\\w*) WHERE \\2.(\\w*) = \\3.\\4$";//SELECT course.sid, course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid
		String projectDistinctNatrualJoin = "^SELECT DISTINCT ([a-z0-9 ,\\.]*) FROM (\\w*), (\\w*) WHERE \\2.(\\w*) = \\3.\\4$";//SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid
		String naturalJoin = "^SELECT \\* FROM (\\w*), (\\w*) WHERE \\1.(\\w*) = \\2.\\3 AND (.*)$";//SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam = 100 AND course2.exam = 100
		String naturalJoinCompli = "^SELECT DISTINCT ([a-z0-9 ,\\.]*) FROM (\\w*), (\\w*) WHERE \\2.(\\w*) = \\3.\\4 AND (.*) ORDER BY (.*)$";//The longest one
		String naturalJoin3 = "^SELECT \\* FROM (\\w*), (\\w*), (\\w*) WHERE \\1.(\\w*)=\\3.\\4 AND \\1.(\\w*)=\\2.\\5 AND \\2.(\\w*)=\\3.\\6$";//SELECT * FROM r, s, t WHERE r.a=t.a AND r.b=s.b AND s.c=t.c
		String crossJoin6 = "^SELECT \\* FROM (\\w*), (\\w*), (\\w*), (\\w*), (\\w*), (\\w*)$";//SELECT * FROM t1, t2, t3, t4, t5, t6
		
		if (Pattern.matches(patternCreate, str))
		{//CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)
			timeReset();
			p = Pattern.compile(patternCreate);
			m = p.matcher(str);
			m.find();
			String table_name = m.group(1);
			String fieldNameAndType = m.group(2);
			String[] NameAndType = fieldNameAndType.split(", | ");
			createTable(table_name, NameAndType);
			timeCalculartor();
			//System.out.println("We created a table " + table_name);
		}
		else if (Pattern.matches(selectPlus, str))
		{//SELECT * FROM course WHERE [ exam = 100 OR homework = 100 ] AND project = 100
			//SELECT * FROM course WHERE exam = 100; SELECT * FROM course WHERE grade = "A";
			//SELECT * FROM course WHERE exam = 100 AND project = 100; SELECT * FROM course WHERE exam = 100 OR exam = 99
			//SELECT * FROM course WHERE NOT exam = 0; SELECT * FROM course WHERE exam > 70
			//SELECT * FROM course WHERE exam = 100 OR homework = 100 AND project = 100; SELECT * FROM course WHERE exam + homework = 200
			//SELECT * FROM course WHERE ( exam * 30 + homework * 20 + project * 50 ) / 100 = 100
			timeReset();
			p = Pattern.compile(selectPlus);
			m = p.matcher(str);
			m.find();
			String table_name = m.group(2);
			//String field_name1, field_name2;
			String where = m.group(3);
			String[] whereContent = where.split(" |\"");
			ArrayList<String> whereClause = new ArrayList<String>(Arrays.asList(whereContent));
			whereClause.removeAll(Arrays.asList(null,""));//remove the space element
			ArrayList<String> postfix = infixToPostfix(whereClause);
			Relation relation_reference = schema_manager.getRelation(table_name);
			System.out.println(str + ": ");
			for (int i = 0; i < relation_reference.getNumOfBlocks(); i++)
			{
				relation_reference.getBlock(i, 5);//read from disk to mem at block 5
				for (int j = 0; j < relation_reference.getSchema().getTuplesPerBlock(); j++)
				{
					Tuple tuple = mem.getBlock(5).getTuple(j);
					String tupleVal = postfixCompute(tuple, postfix);
					if(Boolean.parseBoolean(tupleVal))	
						System.out.println(tuple);
				}
			}
			timeCalculartor();
		}
		
		else if (Pattern.matches(selectAllOrder, str))
		{//SELECT * FROM course ORDER BY exam
			p = Pattern.compile(selectAllOrder);
			m = p.matcher(str);
			m.find();
			String fieldName = m.group(3);
			String table_name = m.group(2);
			Relation relation_reference = schema_manager.getRelation(table_name);
			ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
			int totalTupleNum = relation_reference.getNumOfTuples();
			System.out.println(str + ": ");
			if (relation_reference.getNumOfBlocks() <= 10)
			{//if the table has less than 10 blocks, we can sort it in one time
				timeReset();
				relation_reference.getBlocks(0, 0, relation_reference.getNumOfBlocks());
				tupleList = mem.getTuples(0, relation_reference.getNumOfBlocks());
				Heap.sort(tupleList, relation_reference.getSchema(), fieldName);//sort by a single field 
				for (int i = 0; i < tupleList.size(); i++)
				{//print sorted tuples
					System.out.println(tupleList.get(i).toString());
				}
				timeCalculartor();
			}
			else
			{//the table has less than 10 blocks, we can't sort it in one time;
				//we have to use 2 pass algorithm 
				timeReset();
				ArrayList<Tuple> subList = new ArrayList<Tuple>();
				int numOfSubList = (relation_reference.getNumOfBlocks() + 9) / 10;
				//for each sublist, we read it to mem, sort it, and send back to disk
				for (int i = 0; i < (totalTupleNum - totalTupleNum%10); i=i+10)
				{//
					relation_reference.getBlocks(0+i, 0, 10);//every time read 10 tuples
					subList.addAll(mem.getTuples(0, 10));
					Heap.sort(subList, relation_reference.getSchema(), fieldName);//sort by a single field 
					mem.setTuples(0, subList);
					relation_reference.setBlocks(0+i, 0, 10);//write back to disk
					subList.clear();//empty sublist
				}
				//sort the left blocks which number less than 10
				if (totalTupleNum - totalTupleNum%10 != 0)
				{
					relation_reference.getBlocks(totalTupleNum - totalTupleNum%10, 0, totalTupleNum%10);
					subList.addAll(mem.getTuples(0, totalTupleNum%10));
					Heap.sort(subList, relation_reference.getSchema(), fieldName);//sort by a single field 
					mem.setTuples(0, subList);
					relation_reference.setBlocks(totalTupleNum - totalTupleNum%10, 0, totalTupleNum%10);//write back to disk
					subList.clear();//empty sublist
				}
				//first pass done; sublists sorted.	
				ArrayList<TuplePosition> list = new ArrayList<TuplePosition>();
				for (int i = 0; i < numOfSubList; i++)
				{//Add the block from each sublist to Mem
					relation_reference.getBlock(i*10, i);
					TuplePosition tupleP = new TuplePosition(mem.getBlock(i).getTuple(0), i, 0, 10*i);
					//record the tuple position by using TuplePosition
					list.add(tupleP);
				}
				while (!list.isEmpty())
				{
					Heap.sortP(list, relation_reference.getSchema(), fieldName);
					TuplePosition tp1 = list.get(0);
					System.out.println(tp1.tuple);//print the smallest one
					list.remove(0);//remove the smallest tuple from the list
					insertToList(tp1, relation_reference, list);
				}	
				timeCalculartor();
			}
		}
		else if (Pattern.matches(selectWhereOrder, str))
		{//"^SELECT \\* FROM (\\w*), (\\w*) WHERE \\1.(\\w*) = \\2.\\3 ORDER BY (.*)$" for 
			//SELECT * FROM course, course2 WHERE course.sid = course2.sid ORDER BY course.exam
			p = Pattern.compile(selectWhereOrder);
			m = p.matcher(str);
			m.find();
			timeReset();
			System.out.println(str+": ");
			selectNatrualJoinWithOrder(m.group(1), m.group(2), m.group(3), m.group(4));			
			timeCalculartor();
		}
		else if (Pattern.matches(naturalJoinCompli, str))
		{//SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid AND [
		//course.exam > course2.exam OR course.grade = \"A\" AND course2.grade = \"A\" ] ORDER BY course.exam
			p = Pattern.compile(naturalJoinCompli);
			m = p.matcher(str);
			m.find();
			timeReset();
			System.out.println(str+": ");
			naturalJoinComplicate(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6));		
			timeCalculartor();
		}
		else if (Pattern.matches(projectDistinctNatrualJoin, str))
		{//SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid
			timeReset();
			System.out.println(str + ": ");
			p = Pattern.compile(projectDistinctNatrualJoin);
			m = p.matcher(str);
			m.find();
			projectDistinctNatrualJoin(m.group(1), m.group(2), m.group(3), m.group(4));
			timeCalculartor();
		}
		else if (Pattern.matches(projectNatrualJoin, str))
		{//SELECT course.sid, course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid
			System.out.println(str + ": ");
			timeReset();
			p = Pattern.compile(projectNatrualJoin);
			m = p.matcher(str);
			m.find();
			projectNatrualJoin(m.group(1), m.group(2), m.group(3), m.group(4));
			timeCalculartor();
		}
		else if (Pattern.matches(naturalJoin, str))
		{//SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam = 100 AND course2.exam = 100
		 //SELECT * FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam = 100 OR course2.exam = 100 ]
		//SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam > course2.exam
		//SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam > course2.exam AND course.homework = 100
		//SELECT * FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam > course2.exam OR course.homework = 100 ]
			System.out.println(str + ": ");
			timeReset();
			p = Pattern.compile(naturalJoin);
			m = p.matcher(str);
			m.find();
			selectNatrualJoin(m.group(1), m.group(2), m.group(3), m.group(4));
			timeCalculartor();
		}
		else if (Pattern.matches(naturalJoin3, str))
		{//SELECT * FROM r, s, t WHERE r.a=t.a AND r.b=s.b AND s.c=t.c
			System.out.println(str + ": ");
			timeReset();
			p = Pattern.compile(naturalJoin3);
			m = p.matcher(str);
			m.find();
			naturalJoinThree(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6));
			timeCalculartor();
		}
		else if (Pattern.matches(crossJoin6, str))
		{//SELECT * FROM t1, t2, t3, t4, t5, t6
			System.out.println(str + ": ");
			timeReset();
			p = Pattern.compile(crossJoin6);
			m = p.matcher(str);
			m.find();
			crossJoin6(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6));
			timeCalculartor();
		}
		
		else if (Pattern.matches(selectDistinct, str))
		{//SELECT DISTINCT grade FROM course; SELECT DISTINCT * FROM course
			p = Pattern.compile(selectDistinct);
			m = p.matcher(str);
			m.find();
			String fieldName = m.group(1);
			String table_name = m.group(2);
			Relation relation_reference = schema_manager.getRelation(table_name);
			ArrayList<Tuple> tupleList = new ArrayList<Tuple>();//size of 10 blocks, for sorting
			ArrayList<String>fieldNameList = new ArrayList<String>();
			fieldNameList =  relation_reference.getSchema().getFieldNames();
			int totalTupleNum = relation_reference.getNumOfTuples();
			String nullstr = null;
			if (fieldName.equals("*"))
			{//SELECT DISTINCT * FROM course
				System.out.println(str+": ");
				timeReset();
				if (relation_reference.getNumOfBlocks() <= 10)
				{//if the table has less than 10 blocks, we can sort it in one time
					relation_reference.getBlocks(0, 0, relation_reference.getNumOfBlocks());
					tupleList = mem.getTuples(0, relation_reference.getNumOfBlocks());
					Heap.sort(tupleList, relation_reference.getSchema(), nullstr);//sort by a single field
					Tuple tuple = tupleList.get(0);
					String fieldValue0 = tuple.toString();//the content of tuple
					System.out.println(fieldValue0);
					for (int i = 1; i < tupleList.size(); i++)
					{//print DISTINCT tuple FROM course
						String fieldValue = tupleList.get(i).toString();
						if (fieldValue.equals(fieldValue0))
							continue;
						else
						{
							System.out.println(fieldValue);
							fieldValue0 = fieldValue;
						}
					}
					timeCalculartor();
				}
				else
				{//the table has more than 10 blocks, we can't sort it in one time;
					//we have to use 2 pass algorithm 
					timeReset();
					int numOfSubList = 0;
					String sortByList = null;
					ArrayList<Tuple> subList = new ArrayList<Tuple>();
					numOfSubList = (relation_reference.getNumOfBlocks() + 9) / 10;
					//for each sublist, we read it to mem, sort it, and send back to disk
					for (int i = 0; i < (totalTupleNum - totalTupleNum%10); i=i+10)
					{//
						relation_reference.getBlocks(0+i, 0, 10);//every time read 10 tuples
						subList.addAll(mem.getTuples(0, 10));
						Heap.sort(subList, relation_reference.getSchema(), sortByList);//sort by all fields 
						mem.setTuples(0, subList);
						relation_reference.setBlocks(0+i, 0, 10);//write back to disk
						subList.clear();//empty sublist
					}
					//sort the left blocks which number less than 10
					if (totalTupleNum - totalTupleNum%10 != 0)
					{
						relation_reference.getBlocks(totalTupleNum - totalTupleNum%10, 0, totalTupleNum%10);
						subList.addAll(mem.getTuples(0, totalTupleNum%10));
						Heap.sort(subList, relation_reference.getSchema(), sortByList); 
						mem.setTuples(0, subList);
						relation_reference.setBlocks(totalTupleNum - totalTupleNum%10, 0, totalTupleNum%10);//write back to disk
						subList.clear();//empty sublist
					}
					//first pass done; sublists sorted.	
					ArrayList<TuplePosition> list = new ArrayList<TuplePosition>();
					for (int i = 0; i < numOfSubList; i++)
					{//Add the block from each sublist to Mem
						relation_reference.getBlock(i*10, i);
						TuplePosition tupleP = new TuplePosition(mem.getBlock(i).getTuple(0), i, 0, 10*i);
						list.add(tupleP);
					}
					Heap.sortP(list, relation_reference.getSchema(), sortByList);
					TuplePosition current = list.get(0);//the minimum one
					System.out.println(current.tuple);
					String currentMin = current.tuple.toString();					
					while (!list.isEmpty())
					{
						Heap.sortP(list, relation_reference.getSchema(), sortByList);
						TuplePosition tp1 = list.get(0);
						String tp1Value = tp1.tuple.toString();
						if (!currentMin.equals(tp1Value))
						{//print out distinct, and update string value
							System.out.println(tp1.tuple);
							currentMin = tp1Value;
						}
						list.remove(0);
						insertToList(tp1, relation_reference, list);
					}	
					timeCalculartor();
				}
			}
			else
			{//SELECT DISTINCT grade FROM course
				timeReset();
				System.out.println(str+": ");
				if (relation_reference.getNumOfBlocks() <= 10)
				{//if the table has less than 10 blocks, we can sort it in one time
					relation_reference.getBlocks(0, 0, relation_reference.getNumOfBlocks());
					tupleList = mem.getTuples(0, relation_reference.getNumOfBlocks());
					Heap.sort(tupleList, relation_reference.getSchema(), fieldName);//sort by a single field 
					Tuple tuple = tupleList.get(0);
					String fieldValue0 = tuple.getField(fieldName).toString();//the content of first field
					System.out.println(fieldValue0);
					for (int i = 0; i < tupleList.size(); i++)
					{//print DISTINCT grade FROM course
						String fieldValue = tupleList.get(i).getField(fieldName).toString();
						if (fieldValue.equals(fieldValue0))
							continue;
						else
						{
							System.out.println(fieldValue);
							fieldValue0 = fieldValue;
						}
					}
					timeCalculartor();
				}
				else
				{//the table has more than 10 blocks, we can't sort it in one time;
					//we have to use 2 pass algorithm 
					int numOfSubList = 0;
					ArrayList<Tuple> subList = new ArrayList<Tuple>();
					numOfSubList = (relation_reference.getNumOfBlocks() + 9) / 10;
					//for each sublist, we read it to mem, sort it, and send back to disk
					timeReset();
					for (int i = 0; i < (totalTupleNum - totalTupleNum%10); i=i+10)
					{//
						relation_reference.getBlocks(0+i, 0, 10);//every time read 10 tuples
						subList.addAll(mem.getTuples(0, 10));
						Heap.sort(subList, relation_reference.getSchema(), fieldName);//sort by a single field 
						mem.setTuples(0, subList);
						relation_reference.setBlocks(0+i, 0, 10);//write back to disk
						subList.clear();//empty sublist
					}
					//sort the left blocks which number less than 10
					if (totalTupleNum - totalTupleNum%10 != 0)
					{
						relation_reference.getBlocks(totalTupleNum - totalTupleNum%10, 0, totalTupleNum%10);
						subList.addAll(mem.getTuples(0, totalTupleNum%10));
						Heap.sort(subList, relation_reference.getSchema(), fieldName);//sort by a single field 
						mem.setTuples(0, subList);
						relation_reference.setBlocks(totalTupleNum - totalTupleNum%10, 0, totalTupleNum%10);//write back to disk
						subList.clear();//empty sublist
					}
					//first pass done; sublists sorted.	
					//System.out.println("relation_reference" +"\n" + relation_reference);
					ArrayList<TuplePosition> list = new ArrayList<TuplePosition>();
					for (int i = 0; i < numOfSubList; i++)
					{//Add the block from each sublist to Mem
						relation_reference.getBlock(i*10, i);
						TuplePosition tupleP = new TuplePosition(mem.getBlock(i).getTuple(0), i, 0, 10*i);
						list.add(tupleP);
					}
					
					Heap.sortP(list, relation_reference.getSchema(), fieldName);
					TuplePosition current = list.get(0);//the minimum one
					String currentMin = current.tuple.getField(fieldName).toString();
					System.out.println(currentMin);
					while (!list.isEmpty())
					{
						Heap.sortP(list, relation_reference.getSchema(), fieldName);
						TuplePosition tp1 = list.get(0);
						String tp1Value = tp1.tuple.getField(fieldName).toString();
						if (!currentMin.equals(tp1Value))
						{//print out distinct, and update string value
							System.out.println(tp1.tuple.getField(fieldName).toString());
							currentMin = tp1Value;
						}
						list.remove(0);
						insertToList(tp1, relation_reference, list);
					}
					timeCalculartor();
				}
			}
		}
		else if (Pattern.matches(selectAll, str))
		{//SELECT * FROM course;SELECT * FROM course, course2
			p = Pattern.compile(selectAll);
			m = p.matcher(str);
			m.find();
			String table_name = m.group(1);
			ArrayList<String> tableList = new ArrayList<String>(Arrays.asList(table_name.split(", ")));//convert to arrayList
			if (tableList.size() == 1)
			{//SELECT * FROM course
				timeReset();
				Relation relation_reference = schema_manager.getRelation(table_name);
				int totalTupleNum = relation_reference.getNumOfTuples();
				int numOfTuple = 0;
				System.out.println(str + ":");
				for (int i = 0; i < relation_reference.getNumOfBlocks(); i++)
				{
					relation_reference.getBlock(i, 5);//read from disk to mem at block 5
					for (int j = 0; j < relation_reference.getSchema().getTuplesPerBlock() && numOfTuple < totalTupleNum; j++)
					{
						Tuple tuple = mem.getBlock(5).getTuple(j);
						numOfTuple++;
						System.out.println(tuple);
					}
				}
				timeCalculartor();
			}
			
			else if (tableList.size() == 2)
			{//SELECT * FROM course, course2: cross product
				timeReset();
				int numOfTuple = 0;
				System.out.println(str);		
				Relation relation1 = schema_manager.getRelation(tableList.get(0));
				Relation relation2 = schema_manager.getRelation(tableList.get(1));
				int totalTupleNum2 = relation2.getNumOfTuples();
				for (int i = 0; i < relation2.getNumOfBlocks(); i++)
				{
					relation2.getBlock(i, i);
				}
				ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
				tupleList.addAll(mem.getTuples(0, relation2.getNumOfBlocks()));
				ArrayList<String> fieldName1 = relation1.getSchema().getFieldNames();
				ArrayList<String> fieldName2 = relation2.getSchema().getFieldNames();
				for (String s : fieldName2)
				{
					System.out.print(s + "\t");
				}
				for (String s : fieldName1)
				{
					System.out.print(s + "\t");
				}
				System.out.println();
				for (int i = 0; i < relation1.getNumOfBlocks(); i++)
				{
					relation1.getBlock(i, relation2.getNumOfBlocks());
					for (int j = 0; j < relation1.getSchema().getTuplesPerBlock(); j++)
					{
						Tuple crossTuple = mem.getBlock(relation2.getNumOfBlocks()).getTuple(j);
						for (Tuple t : tupleList)
						{
							System.out.print(t + " ");
							System.out.print(crossTuple + "\n");
						}
					}
				}
				
			}timeCalculartor();
		}
		else if (Pattern.matches(select2, str))
		{//SELECT sid, course.grade FROM course; SELECT sid, grade FROM course
			if (Pattern.matches(select2, str))
			p = Pattern.compile(select2);
			m = p.matcher(str);
			m.find();
			String[] fName = m.group(1).split("[, .]");//field name is splitted by comma, space, and dot
			String table_name = m.group(2);
			Relation relation_reference = schema_manager.getRelation(table_name);
			timeReset();
			System.out.println(str +": ");
			for(String s : fName)
			{
				if (relation_reference.getSchema().fieldNameExists(s))
				{
					for (int i = 0; i < relation_reference.getNumOfBlocks(); i++)
					{
						relation_reference.getBlock(i, 5);//read from disk to mem at block 5
						//Tuple tuple  = mem.getBlock(5).getTuple(0);//read tuple from mem
						for (int j = 0; j < relation_reference.getSchema().getTuplesPerBlock(); j++)
						{
							//if (mem.getBlock(5).getTuple(j).getField(fName).toString().equals(value))
								System.out.println(mem.getBlock(5).getTuple(j).getField(s).toString());
						}
						//System.out.print(s + ": " + tuple.getField(s).toString() +"\t");
					}
				}
				System.out.println();
			}timeCalculartor();
		}
		
		else if (Pattern.matches(patternDelC, str))
		{//DELETE FROM course WHERE grade = "E"
			p = Pattern.compile(patternDelC);
			m = p.matcher(str);
			m.find();
			//String table_name = m.group(1);
			String field_name = m.group(2);
			String field_value = m.group(3);
			Relation relation_reference = schema_manager.getRelation(m.group(1));
			timeReset();
			System.out.println(str);
			for(int i = 0; i < relation_reference.getNumOfBlocks(); i++)
			{
				relation_reference.getBlock(i, i);//read relation (disk) block i to mem block i
				for (int j = 0; j < relation_reference.getSchema().getTuplesPerBlock(); j++)
				{
					Tuple tuple  = mem.getBlock(i).getTuple(j);
					//tuple.getField(field_name) return a field, not a string
					if (tuple.getField(field_name).toString().equals(field_value))
							mem.getBlock(i).invalidateTuple(j);//delete
				}
				relation_reference.setBlock(i, i);
			}
			System.out.println("After delete: " + relation_reference);
			timeCalculartor();
		}
		else if (Pattern.matches(patternDel, str))
		{//DELETE FROM course 
			p = Pattern.compile(patternDel);
			m = p.matcher(str);
			m.find();
			Relation relation_reference = schema_manager.getRelation(m.group(1));
			timeReset();
			System.out.println(str);
			for(int i = 0; i < relation_reference.getNumOfBlocks(); i++)
			{
				mem.getBlock(i).invalidateTuples();//read to mem and Erase all the tuples in the bloc
				relation_reference.setBlock(i, i);//write back to disk
			}
			System.out.println("After delete: " + relation_reference);
			timeCalculartor();
		}
		
		else if (Pattern.matches(patternDrop, str))
		{
			p = Pattern.compile(patternDrop);
			m = p.matcher(str);
			m.find();
			String table_name = m.group(1);
			timeReset();
			System.out.println(str);
			schema_manager.deleteRelation(table_name);//drop a table
			System.out.println("We deleted table: " + table_name);
			timeCalculartor();
		}
		else if (Pattern.matches(patternInsert, str))
		{//INSERT INTO course (sid, homework, project, exam, grade) VALUES (2, 0, 100, 100, "E")
			timeReset();
			//System.out.println(str);
			insertTuple(patternInsert, str);
			timeCalculartor();
		}
		else if (Pattern.matches(patternInsertFrom, str))
		{//INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course
		//here the only test query here is to select all from 'course' and insert into 'course'
			Relation relation_reference = schema_manager.getRelation("course");
			int originalNoOfTuples = relation_reference.getNumOfTuples();
			int numOfField = relation_reference.getSchema().getNumOfFields();//attribute number, here is just 5
			//System.out.print(numOfField + " test!!!!should be 5");
			if (relation_reference.isNull() || originalNoOfTuples == 0)
			{
				System.out.println("The 'course' table is empty");
				return;
			}
			timeReset();
			System.out.println(str);
			for(int i = 0; i < originalNoOfTuples; i++)
			{
				relation_reference.getBlock(i, i);//writ relation (in disk) block i to memory block i
				relation_reference.setBlock(originalNoOfTuples+i, i);//writ memory block i to relation (in disk) block end 
			}		
			System.out.println(relation_reference);
			timeCalculartor();
		}
		else
		{
			System.out.println(str + ": ");
			System.out.println("Sorry, the query you submitted is non-executable. Please check the format and content of the query.");
		}
		
	}
	
	private void crossJoin6(String tableName1, String tableName2, String tableName3, String tableName4, 
			String tableName5,String tableName6) 
	{
		Relation relation_reference1 = schema_manager.getRelation(tableName1);
		Relation relation_reference2 = schema_manager.getRelation(tableName2);
		Relation relation_reference3 = schema_manager.getRelation(tableName3);
		Relation relation_reference4 = schema_manager.getRelation(tableName4);
		Relation relation_reference5 = schema_manager.getRelation(tableName5);
		Relation relation_reference6 = schema_manager.getRelation(tableName6);
		for(int i=0; i<10; i++)
			mem.getBlock(i).clear();
		relation_reference6.getBlock(0, 0);
		relation_reference5.getBlock(0, 1);
		int[] startToEnd1 = new int[2];
		startToEnd1[0] = 0;
		startToEnd1[1] = 0;
		int[] startToEnd2 = memoryCrossJoin(startToEnd1, startToEnd1[1]+1, relation_reference6.getRelationName(), relation_reference5.getRelationName(),false);
		relation_reference4.getBlock(0, startToEnd2[1]+1);
		int[] startToEnd3 = memoryCrossJoin(startToEnd2, startToEnd2[1]+1, "", relation_reference4.getRelationName(),false);
		relation_reference3.getBlock(0, startToEnd3[1]+1);
		int[] startToEnd4 = memoryCrossJoin(startToEnd3, startToEnd3[1]+1, "", relation_reference3.getRelationName(),false);
		relation_reference2.getBlock(0, startToEnd4[1]+1);
		memoryCrossJoin(startToEnd4, startToEnd4[1]+1, "", relation_reference2.getRelationName(),true);
		Relation new_relation = schema_manager.getRelation("crossjoin"+relation_reference2.getRelationName());
		this.crossJoinOutPut(new_relation, relation_reference1);
		schema_manager.deleteRelation("crossjoin"+relation_reference5.getRelationName());
		schema_manager.deleteRelation("crossjoin"+relation_reference4.getRelationName());
		schema_manager.deleteRelation("crossjoin"+relation_reference3.getRelationName());
		schema_manager.deleteRelation("crossjoin"+relation_reference2.getRelationName());
		
	}
	private void crossJoinOutPut(Relation relation1, Relation relation2) 
	{
		ArrayList<String> filedNames1 = relation1.getSchema().getFieldNames();
		ArrayList<String> filedNames2 = relation2.getSchema().getFieldNames();
		String productFieldnames = "";
		for(int i=0; i<filedNames1.size(); i++)
		{
			productFieldnames += filedNames1.get(i) + '\t';
			System.out.print(filedNames1.get(i) + '\t');
		}
		
		for(int i=0; i<filedNames2.size(); i++)
		{
			productFieldnames += relation2.getRelationName() + '.' + filedNames2.get(i) + '\t';
			System.out.print(relation2.getRelationName() + '.' + filedNames2.get(i) + '\t');
		}
		this.writeOutput(productFieldnames);
		System.out.println();
		
		mem.getBlock(0).clear();
		mem.getBlock(1).clear();
		relation2.getBlock(0, 1);
		for(int i=0; i<relation1.getNumOfBlocks(); i++)
		{
			relation1.getBlock(i, 0);
			for(int j=0; j<mem.getBlock(0).getNumTuples(); j++){	    		
    			for(int k=0; k<mem.getBlock(1).getNumTuples(); k++)
    			{
    				writeOutput(mem.getBlock(0).getTuple(j)+""+mem.getBlock(1).getTuple(k));
    				System.out.println(mem.getBlock(0).getTuple(j)+""+mem.getBlock(1).getTuple(k));
    			}	    		
	    	}			
		}
	}
	private int[] memoryCrossJoin(int[] startToEnd1, int memIndex2, String relationName1,
			String relationName2, boolean ifStore) {
		Tuple tuple1 = mem.getBlock(startToEnd1[0]).getTuple(0);
		Tuple tuple2 = mem.getBlock(memIndex2).getTuple(0);
		ArrayList<String> fieldNames1= tuple1.getSchema().getFieldNames();
		for(int i=0; i<fieldNames1.size(); i++){
			if(!fieldNames1.get(i).contains(".")){
				fieldNames1.set(i, relationName1+"."+fieldNames1.get(i));
			}
		}
    	ArrayList<FieldType> fieldTypes1= tuple1.getSchema().getFieldTypes();
    	ArrayList<String> fieldNames2= tuple2.getSchema().getFieldNames();
    	for(int i=0; i<fieldNames2.size(); i++){
			if(!fieldNames2.get(i).contains(".")){
				fieldNames2.set(i, relationName2+"."+fieldNames2.get(i));
			}
		}
    	ArrayList<FieldType> fieldTypes2= tuple2.getSchema().getFieldTypes();
    	ArrayList<String> sortNameList = new ArrayList<String>();
    	for(int i=0; i<fieldNames2.size(); i++){		
			fieldNames1.add(fieldNames2.get(i));
			fieldTypes1.add(fieldTypes2.get(i));
    	}
    
    	Schema newSchema=new Schema(fieldNames1,fieldTypes1);
	    Relation new_relation_reference=schema_manager.createRelation("crossjoin"+relationName2,newSchema);
	    Block block2 = mem.getBlock(memIndex2);
	    int memIndex3 = memIndex2 + 1;
	    if(memIndex3>9)
			memIndex3 = memIndex3%10;
	    if(startToEnd1[1]<startToEnd1[0])
	    	startToEnd1[1] += 10;
	    for(int i=startToEnd1[0]; i<=startToEnd1[1]; i++){
	    	Block block1 = mem.getBlock(i%10);
	    	for(int j=0; j<block1.getNumTuples(); j++){
	    		for(int k=0; k<block2.getNumTuples(); k++){
	    			Tuple newTuple = new_relation_reference.createTuple();
	    			this.joinTwoTuples(block1.getTuple(j), block2.getTuple(k), newTuple);
	    			if(ifStore){
	    				this.appendTupleToRelation(new_relation_reference, mem, memIndex2+1, newTuple);
	    			}
	    			else{
		    			if(mem.getBlock(memIndex3).isFull()){
		    				memIndex3++;
		    				if(memIndex3>9)
		    					memIndex3 = memIndex3%10;
		    			}
		    			mem.getBlock(memIndex3).appendTuple(newTuple);
	    			}
	    		}
	    	}
	    	block1.clear();
	    }
	    block2.clear();
	    int[] startAndEnd = new int[2];
	    startAndEnd[0] = memIndex2+1;
	    startAndEnd[1] = memIndex3;
	    return startAndEnd;
	}
	private void naturalJoinThree(String tableName1, String tableName2,String tableName3, String joinFieldName13,
			String joinFieldName12,String joinFieldName23) 
	{
		Relation relation_reference1 = schema_manager.getRelation(tableName1);
		Relation relation_reference2 = schema_manager.getRelation(tableName2);
		Relation relation_reference3 = schema_manager.getRelation(tableName3);
		String[] sortNameArray = new String[1];
	    sortNameArray[0] = joinFieldName13;
	    firstPassSort(relation_reference1, sortNameArray);//sort and write back to disk
	    firstPassSort(relation_reference3, sortNameArray);//sort and write back to disk
	    Relation relation1JoinRelation3 = natrualJoin(relation_reference1, relation_reference3, sortNameArray, true);
	    String[] sortNameArray2 = new String[2];
	    sortNameArray2[0] = joinFieldName12;
	    sortNameArray2[1] = joinFieldName23;
	    firstPassSort(relation1JoinRelation3, sortNameArray2);//sort and write back to disk
	    //System.out.println("relation1JoinRelation3: "+ relation1JoinRelation3);
	    firstPassSort(relation_reference2, sortNameArray2);//sort and write back to disk
	    //System.out.println("relation_reference2: "+ relation_reference2);
	    Relation finalRelation = natrualJoin(relation1JoinRelation3, relation_reference2, sortNameArray2, false);
	    schema_manager.deleteRelation(relation1JoinRelation3.getRelationName());
	    schema_manager.deleteRelation(finalRelation.getRelationName());
		
	}
	private Relation natrualJoin(Relation relation_reference1, Relation relation_reference2, 
			String[] sortNameArray, boolean ifStoreBack ){
    	//create new relation using relation1 and relation2
    	ArrayList<String> fieldNames1= relation_reference1.getSchema().getFieldNames();
    	ArrayList<FieldType> fieldTypes1= relation_reference1.getSchema().getFieldTypes();
    	ArrayList<String> fieldNames2= relation_reference2.getSchema().getFieldNames();
    	ArrayList<FieldType> fieldTypes2= relation_reference2.getSchema().getFieldTypes();
    	ArrayList<String> sortNameList = new ArrayList<String>();
    	for(int i=0; i<sortNameArray.length; i++)
    	{
    		sortNameList.add(sortNameArray[i]);
    	}
    	for(int i=0; i<fieldNames2.size(); i++){
    		if(!sortNameList.contains(fieldNames2.get(i)))
    		{
    			fieldNames1.add(fieldNames2.get(i));
    			fieldTypes1.add(fieldTypes2.get(i));
    		}
    	}
    
    	Schema newSchema=new Schema(fieldNames1,fieldTypes1);
	    Relation new_relation_reference=schema_manager.createRelation(relation_reference1.getRelationName()+"naturaljoin"+relation_reference2.getRelationName(),newSchema);
    	//write all fieldNames to output
	    if(!ifStoreBack)
	    {
	    	writeOutput(newSchema.fieldNamesToString());
	    	System.out.println(newSchema.fieldNamesToString());
	    }
		//second pass of two passes
    	Heap2 heap1 = new Heap2(sortNameArray);
    	Heap2 heap2 = new Heap2(sortNameArray);
    	int relationBlockNum1 = relation_reference1.getNumOfBlocks();
    	int relationBlockNum2 = relation_reference2.getNumOfBlocks();
    	int sublistNum1 = (relationBlockNum1+9)/10;
    	int sublistNum2 = (relationBlockNum2+9)/10;
    	
    	for(int i=0; i<sublistNum1; i++){//put the first block of each sublist of relation1 into memory, starting from 0
	    	relation_reference1.getBlock(i*10, i);
	    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i).getTuple(0),i,0,i*10);
			heap1.insert(tuplePlus);
    	}
    	for(int i=0; i<sublistNum2; i++){//put the first block of each sublist of relation2 into memory, starting from sublistNum1
	    	relation_reference2.getBlock(i*10, i+sublistNum1);
	    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i+sublistNum1).getTuple(0),i+sublistNum1,0,i*10);
			heap2.insert(tuplePlus);
    	}
    	ArrayList<Tuple> tupleList1 = new ArrayList<Tuple>();//used to store tuples has the same value with relaiton2
    	ArrayList<Tuple> tupleList2 = new ArrayList<Tuple>();
		 while(heap1.length()>1 && heap2.length()>1){
		    	TuplePosition tuplePlus1 = heap1.minTuple();
				TuplePosition tuplePlus2 = heap2.minTuple();
				
				if(tuplePlus1.tuple.getField(sortNameArray[0]).integer>tuplePlus2.tuple.getField(sortNameArray[0]).integer){
					heap2.delete(1);
					insertToHeap(tuplePlus2, relation_reference2, heap2);
				}
				else if(tuplePlus1.tuple.getField(sortNameArray[0]).integer<tuplePlus2.tuple.getField(sortNameArray[0]).integer){
					heap1.delete(1);
					insertToHeap(tuplePlus1, relation_reference1, heap1);
				}
				else{
					
					//process tuplePlus1,tuplePlus2;
					
					tupleList1.clear();
					tupleList2.clear();
					tupleList1.add(tuplePlus1.tuple);
					tupleList2.add(tuplePlus2.tuple);
					heap1.delete(1);
					insertToHeap(tuplePlus1, relation_reference1, heap1);
					heap2.delete(1);
					insertToHeap(tuplePlus2, relation_reference2, heap2);
					
					while(heap1.length()>1 && heap1.compareTo(tuplePlus1, heap1.minTuple())==0){
						TuplePosition newTuplePlus1 = heap1.minTuple();
						//process newTuplePlus1,tuplePlus2;
						tupleList1.add(newTuplePlus1.tuple);
						
						heap1.delete(1);
						insertToHeap(newTuplePlus1, relation_reference1, heap1);
					}
					while(heap2.length()>1 && heap2.compareTo(tuplePlus2, heap2.minTuple())==0){
						TuplePosition newTuplePlus2 = heap2.minTuple();
						//process tuplePlus1,newTuplePlus2;
						tupleList2.add(newTuplePlus2.tuple);
						
						heap2.delete(1);
						insertToHeap(newTuplePlus2, relation_reference2, heap2);
					}
					for(int i=0; i<tupleList1.size(); i++){
						for(int j=0; j<tupleList2.size(); j++){
							Tuple newTuple = new_relation_reference.createTuple();
							newTuple = spliceTwoTuples(tuplePlus1.tuple, tuplePlus2.tuple, newTuple);
							if(ifStoreBack){
								appendTupleToRelation(new_relation_reference, mem, 9, newTuple);
							}
							else{
								writeOutput(newTuple.toString());
								System.out.println(newTuple.toString());
							}
						}
					}
					
				}  
		  }
		 return new_relation_reference;
    }
	private Tuple spliceTwoTuples(Tuple t1, Tuple t2, Tuple newTuple) 
	{
		ArrayList<String> fieldNames = newTuple.getSchema().getFieldNames();
    	for(int i=0; i<newTuple.getNumOfFields(); i++){
    		if(i<t1.getNumOfFields()){
    			if(t1.getField(i).type == FieldType.INT){
    				newTuple.setField(i,t1.getField(i).integer);
    			}
    			else{
    				newTuple.setField(i,t1.getField(i).str);
    			}	
    		}
    		else{    			
    			if(newTuple.getField(i).type == FieldType.INT){
    				newTuple.setField(fieldNames.get(i),t2.getField(fieldNames.get(i)).integer);
    			}
    			else{
    				newTuple.setField(fieldNames.get(i),t2.getField(fieldNames.get(i)).str);
    			}	
    		}
								
		}
		
		return newTuple;
	}
	private void naturalJoinComplicate(String projectNameList, String tableName1, String tableName2, 
			String joinFieldName, String whereClause, String orderFieldName) 
	{
		Relation relation_reference1 = schema_manager.getRelation(tableName1);
		Relation relation_reference2 = schema_manager.getRelation(tableName2);
		String[] sortNameArray = new String[1];
	    sortNameArray[0] = joinFieldName; 
	    firstPassSort(relation_reference1, sortNameArray);//sort and write back to disk
	    firstPassSort(relation_reference2, sortNameArray);//sort and write back to disk
	    String[] outputNameArray = projectNameList.split(", ");
	    String[] orderNameArray = new String[1];
	    orderNameArray[0] = orderFieldName;
	    naturalJoinComplicateProcess(relation_reference1, relation_reference2, sortNameArray, outputNameArray, whereClause, orderNameArray);
		
	}
	private void naturalJoinComplicateProcess(Relation relation_reference1, Relation relation_reference2, String[] sortNameArray, 
			String[] outputNameArray, String whereClause, String[] orderNameArray) 
	{
		ArrayList<String> fieldNames1= relation_reference1.getSchema().getFieldNames();
    	for(int i=0; i<fieldNames1.size(); i++)
    	{
    		String newName = relation_reference1.getRelationName() + "." + fieldNames1.get(i);
    		fieldNames1.set(i, newName);
    	}
    	ArrayList<FieldType> fieldTypes1= relation_reference1.getSchema().getFieldTypes();
    	ArrayList<String> fieldNames2= relation_reference2.getSchema().getFieldNames();
    	for(int i=0; i<fieldNames2.size(); i++){
    		String newName = relation_reference2.getRelationName() + "." + fieldNames2.get(i);
    		fieldNames2.set(i, newName);
    	}
    	ArrayList<FieldType> fieldTypes2= relation_reference2.getSchema().getFieldTypes();
    	fieldNames1.addAll(fieldNames2);
    	fieldTypes1.addAll(fieldTypes2);
    	Schema newSchema=new Schema(fieldNames1,fieldTypes1);

	    Relation new_relation_reference=schema_manager.createRelation("newrelation",newSchema);
	    
		//second pass of two passes
    	Heap2 heap1 = new Heap2(sortNameArray);
    	Heap2 heap2 = new Heap2(sortNameArray);
    	int relationBlockNum1 = relation_reference1.getNumOfBlocks();
    	int relationBlockNum2 = relation_reference2.getNumOfBlocks();
    	int sublistNum1 = (relationBlockNum1+9)/10;
    	int sublistNum2 = (relationBlockNum2+9)/10;
    	
    	for(int i=0; i<sublistNum1; i++)
    	{//put the first block of each sublist of relation1 into memory, starting from 0
	    	relation_reference1.getBlock(i*10, i);
	    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i).getTuple(0),i,0,i*10);
			heap1.insert(tuplePlus);
    	}
    	for(int i=0; i<sublistNum2; i++){//put the first block of each sublist of relation2 into memory, starting from sublistNum1
	    	relation_reference2.getBlock(i*10, i+sublistNum1);
	    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i+sublistNum1).getTuple(0),i+sublistNum1,0,i*10);
			heap2.insert(tuplePlus);
    	}
    	    	
		 while(heap1.length()>1 && heap2.length()>1)
		 {
		    	TuplePosition tp1 = heap1.minTuple();
				TuplePosition tp2 = heap2.minTuple();
				
				if(tp1.tuple.getField(sortNameArray[0]).integer>tp2.tuple.getField(sortNameArray[0]).integer)
				{
					heap2.delete(1);
					insertToHeap(tp2, relation_reference2, heap2);
				}
				else if(tp1.tuple.getField(sortNameArray[0]).integer<tp2.tuple.getField(sortNameArray[0]).integer)
				{
					heap1.delete(1);
					insertToHeap(tp1, relation_reference1, heap1);
				}
				else{
					tp1.tuple.getNumOfFields();
					//process tuplePlus1,tuplePlus2;
					Tuple newTuple = new_relation_reference.createTuple();
					newTuple = joinTwoTuples(tp1.tuple, tp2.tuple, newTuple);
					if(this.evaluateTuple(newTuple, toPostfix(whereClause))){						
						appendTupleToRelation(new_relation_reference, mem, 9, newTuple);
					}
					heap1.delete(1);
					insertToHeap(tp1, relation_reference1, heap1);
					heap2.delete(1);
					insertToHeap(tp2, relation_reference2, heap2);
					
					while(heap1.length()>1 && heap1.compareTo(tp1, heap1.minTuple())==0)
					{
						TuplePosition newTuplePlus1 = heap1.minTuple();
						//process newTuplePlus1,tuplePlus2;
						newTuple = new_relation_reference.createTuple();
						newTuple = joinTwoTuples(newTuplePlus1.tuple, tp2.tuple, newTuple);
						if(this.evaluateTuple(newTuple, toPostfix(whereClause)))
						{						
							appendTupleToRelation(new_relation_reference, mem, 9, newTuple);
						}
						
						heap1.delete(1);
						insertToHeap(newTuplePlus1, relation_reference1, heap1);
					}
					while(heap2.length()>1 && heap2.compareTo(tp2, heap2.minTuple())==0)
					{
						TuplePosition newTuplePlus2 = heap2.minTuple();
						//process tuplePlus1,newTuplePlus2;
						newTuple = new_relation_reference.createTuple();
						newTuple = joinTwoTuples(tp1.tuple, newTuplePlus2.tuple, newTuple);
						if(evaluateTuple(newTuple, toPostfix(whereClause)))						
							appendTupleToRelation(new_relation_reference, mem, 9, newTuple);
						heap2.delete(1);
						insertToHeap(newTuplePlus2, relation_reference2, heap2);
					}
				}  
		  }
		 //new relation has been store back with where and natural join done;
		 firstPassSort(new_relation_reference, outputNameArray);
		 Relation tempRelation = relationDuplicateElimation(new_relation_reference, outputNameArray);
		 firstPassSort(tempRelation, orderNameArray);
		 relationSortOutput(tempRelation, orderNameArray, outputNameArray, false);
		 schema_manager.deleteRelation("newrelation");
		 schema_manager.deleteRelation("tempRelation");		
	}
	private Relation relationDuplicateElimation(Relation relation_reference, String[] sortNameArray) 
	{
		//second pass of two passes
    	Heap2 heap = new Heap2(sortNameArray);
    	int relationBlockNum = relation_reference.getNumOfBlocks();
    	int[] pointer = null;
    	if(relationBlockNum%10 == 0)
    		pointer = new int[relationBlockNum/10];
    	else
    		pointer = new int[relationBlockNum/10+1];
    	for(int i=0; i<pointer.length; i++)
    	{
    		pointer[i] = 0;
	    	relation_reference.getBlock(i*10, i);
    	}
    	
    	for(int j=0; j<pointer.length; j++)
    	{
    		Block block = mem.getBlock(j);
			Tuple tuple = block.getTuple(0);
			TuplePosition tuplePlus = new TuplePosition(tuple,j,0);
			heap.insert(tuplePlus);
    	}
    	
    	Relation tempRelation = schema_manager.createRelation("tempRelation", relation_reference.getSchema());
		TuplePosition outputTuplePlus = heap.minTuple();
		appendTupleToRelation(tempRelation, mem, 9, outputTuplePlus.tuple);
		while(heap.length()>1)//second pass of 2 pass
		{
	    	TuplePosition tp = heap.minTuple();
			heap.delete(1);
			if(heap.compareTo(tp, outputTuplePlus) != 0)
			{
				appendTupleToRelation(tempRelation, mem, 9, tp.tuple);
				outputTuplePlus = tp;
			}
		    if(tp.tupleInBlock<mem.getBlock(tp.memBlockPosition).getNumTuples()-1)
		    {
		    	Tuple tuple = mem.getBlock(tp.memBlockPosition).getTuple(tp.tupleInBlock+1);
				heap.insert(new TuplePosition(tuple,tp.memBlockPosition,tp.tupleInBlock+1));
		    }
		    else if(pointer[tp.memBlockPosition]<9 && tp.memBlockPosition*10+pointer[tp.memBlockPosition]<relationBlockNum-1){//sublist not exhaust
		    	pointer[tp.memBlockPosition] += 1;
		    	relation_reference.getBlock(tp.memBlockPosition*10+pointer[tp.memBlockPosition], tp.memBlockPosition);
		    	heap.insert(new TuplePosition(mem.getBlock(tp.memBlockPosition).getTuple(0),tp.memBlockPosition,0));
		    }
		}
		return tempRelation;
	}
	private void selectNatrualJoin(String tableName1, String tableName2, String joinFieldName, String whereClause) 
	{
		Relation relation_reference1 = schema_manager.getRelation(tableName1);
		Relation relation_reference2 = schema_manager.getRelation(tableName2);
		String[] sortNameArray = new String[1];
	    sortNameArray[0] = joinFieldName; 
	    firstPassSort(relation_reference1, sortNameArray);//sort and write back to disk
	    firstPassSort(relation_reference2, sortNameArray);//sort and write back to disk
		natrualJoinSelection(relation_reference1, relation_reference2, sortNameArray, whereClause);
	}
	private void natrualJoinSelection(Relation relation_reference1, Relation relation_reference2, String[] sortNameArray, String whereClause) 
	{
		ArrayList<String> fieldNames1= relation_reference1.getSchema().getFieldNames();
    	for(int i=0; i<fieldNames1.size(); i++){
    		String newName = relation_reference1.getRelationName() + "." + fieldNames1.get(i);
    		fieldNames1.set(i, newName);
    	}
    	ArrayList<FieldType> fieldTypes1= relation_reference1.getSchema().getFieldTypes();
    	ArrayList<String> fieldNames2= relation_reference2.getSchema().getFieldNames();
    	for(int i=0; i<fieldNames2.size(); i++){
    		String newName = relation_reference2.getRelationName() + "." + fieldNames2.get(i);
    		fieldNames2.set(i, newName);
    	}
    	ArrayList<FieldType> fieldTypes2= relation_reference2.getSchema().getFieldTypes();
    	fieldNames1.addAll(fieldNames2);
    	fieldTypes1.addAll(fieldTypes2);
    	Schema newSchema=new Schema(fieldNames1,fieldTypes1);

	    Relation new_relation_reference=schema_manager.createRelation("newrelation",newSchema);
    	//write all fieldNames to output
		writeOutput(newSchema.fieldNamesToString());
		System.out.println(newSchema.fieldNamesToString());
		//second pass of two passes
    	Heap2 heap1 = new Heap2(sortNameArray);
    	Heap2 heap2 = new Heap2(sortNameArray);
    	int relationBlockNum1 = relation_reference1.getNumOfBlocks();
    	int relationBlockNum2 = relation_reference2.getNumOfBlocks();
    	int sublistNum1 = (relationBlockNum1+9)/10;
    	int sublistNum2 = (relationBlockNum2+9)/10;
    	
    	for(int i=0; i<sublistNum1; i++){//put the first block of each sublist of relation1 into memory, starting from 0
	    	relation_reference1.getBlock(i*10, i);
	    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i).getTuple(0),i,0,i*10);
			heap1.insert(tuplePlus);
    	}
    	for(int i=0; i<sublistNum2; i++){//put the first block of each sublist of relation2 into memory, starting from sublistNum1
	    	relation_reference2.getBlock(i*10, i+sublistNum1);
	    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i+sublistNum1).getTuple(0),i+sublistNum1,0,i*10);
			heap2.insert(tuplePlus);
    	}
    	    	
		 while(heap1.length()>1 && heap2.length()>1){
		    	TuplePosition tuplePlus1 = heap1.minTuple();
				TuplePosition tuplePlus2 = heap2.minTuple();
				
				if(tuplePlus1.tuple.getField(sortNameArray[0]).integer>tuplePlus2.tuple.getField(sortNameArray[0]).integer){
					heap2.delete(1);
					insertToHeap(tuplePlus2, relation_reference2, heap2);
				}
				else if(tuplePlus1.tuple.getField(sortNameArray[0]).integer<tuplePlus2.tuple.getField(sortNameArray[0]).integer){
					heap1.delete(1);
					insertToHeap(tuplePlus1, relation_reference1, heap1);
				}
				else{
					tuplePlus1.tuple.getNumOfFields();
					//process tuplePlus1,tuplePlus2;
					Tuple newTuple = new_relation_reference.createTuple();
					newTuple = joinTwoTuples(tuplePlus1.tuple, tuplePlus2.tuple, newTuple);
					if(evaluateTuple(newTuple, toPostfix(whereClause)))
					{						
						writeOutput(newTuple.toString());
						System.out.println(newTuple.toString());
					}
					heap1.delete(1);
					insertToHeap(tuplePlus1, relation_reference1, heap1);
					heap2.delete(1);
					insertToHeap(tuplePlus2, relation_reference2, heap2);
					
					while(heap1.length()>1 && heap1.compareTo(tuplePlus1, heap1.minTuple())==0){
						TuplePosition newTuplePlus1 = heap1.minTuple();
						//process newTuplePlus1,tuplePlus2;
						newTuple = new_relation_reference.createTuple();
						newTuple = joinTwoTuples(newTuplePlus1.tuple, tuplePlus2.tuple, newTuple);
						if(evaluateTuple(newTuple, toPostfix(whereClause)))
						{						
							writeOutput(newTuple.toString());
							System.out.println(newTuple.toString());
						}
						
						heap1.delete(1);
						insertToHeap(newTuplePlus1, relation_reference1, heap1);
					}
					while(heap2.length()>1 && heap2.compareTo(tuplePlus2, heap2.minTuple())==0){
						TuplePosition newTuplePlus2 = heap2.minTuple();
						//process tuplePlus1,newTuplePlus2;
						newTuple = new_relation_reference.createTuple();
						newTuple = joinTwoTuples(tuplePlus1.tuple, newTuplePlus2.tuple, newTuple);
						if(evaluateTuple(newTuple, toPostfix(whereClause)))
						{						
							this.writeOutput(newTuple.toString());
							System.out.println(newTuple.toString());
						}
						
						heap2.delete(1);
						insertToHeap(newTuplePlus2, relation_reference2, heap2);
					}
				}  
		  }
		 schema_manager.deleteRelation("newrelation");
	}
	private ArrayList<String> toPostfix(String whereClause)
	{
    	String[] expressionArray = whereClause.split(" ");
    	Stack<String> stack = new Stack<String>();
    	ArrayList<String> postfixExp = new ArrayList<String>();
    	Hashtable<String, Integer> operatorP = new Hashtable<String, Integer>();
    	operatorP.put("OR", 0);
    	operatorP.put("AND", 1);
    	operatorP.put("NOT", 2);
    	operatorP.put("<", 3);
    	operatorP.put(">", 3);
    	operatorP.put("=", 3);
    	operatorP.put("+", 4);
    	operatorP.put("-", 4);
    	operatorP.put("*", 5);
    	operatorP.put("/", 5);
    	operatorP.put("(", -1);
    	operatorP.put("[", -1);
    	for(int i=0;i<expressionArray.length; i++){
    		if(expressionArray[i].equals("(") || expressionArray[i].equals("[")){
    			stack.push(expressionArray[i]);
    		}
    		else if(operatorP.containsKey(expressionArray[i])){
    			while(stack.size()>0 && operatorP.get(stack.peek())>=operatorP.get(expressionArray[i])){
    				postfixExp.add(stack.pop());
    			}
    			stack.push(expressionArray[i]);
    		} 
    		else if(expressionArray[i].equals(")") || expressionArray[i].equals("]")){
    			while(!(stack.peek().equals("(") || stack.peek().equals("["))){
    				postfixExp.add(stack.pop());
    			}
    			stack.pop();//remove "(" or "["
    		}
    		else{
    			postfixExp.add(expressionArray[i]);
    		}
    	}
    	while(stack.size()>0){
    		postfixExp.add(stack.pop());
    	}
    	return postfixExp;
    	
    }
	private boolean evaluateTuple(Tuple tuple, ArrayList<String> postfix){
    	Stack<String> stack = new Stack<String>();
    	Boolean operand2 = null;
		Boolean operand1 = null;
		Boolean result = null;
		Integer intOperand2 = null;
		Integer intOperand1 = null;
		Integer intResult = null;
    	for(int i=0; i<postfix.size(); i++){
    		switch(postfix.get(i)){
    			case "OR":
    				operand2 = new Boolean(stack.pop());
    				operand1 = new Boolean(stack.pop());
    				result = operand1 || operand2;
    				stack.push(result.toString());break;
    			case "AND":
    				operand2 = new Boolean(stack.pop());
    				operand1 = new Boolean(stack.pop());
    				result = operand1 && operand2;
    				stack.push(result.toString());break;
    			case "NOT":
    				operand1 = new Boolean(stack.pop());
    				result = !operand1;
    				stack.push(result.toString());break;
    			case "+":
    				intOperand2 = new Integer(stack.pop());
    				intOperand1 = new Integer(stack.pop());
    				intResult = intOperand1 + intOperand2;
    				stack.push(intResult.toString());break;
    			case "-":
    				intOperand2 = new Integer(stack.pop());
    				intOperand1 = new Integer(stack.pop());
    				intResult = intOperand1 - intOperand2;
    				stack.push(intResult.toString());break;
    			case "*":
    				intOperand2 = new Integer(stack.pop());
    				intOperand1 = new Integer(stack.pop());
    				intResult = intOperand1 * intOperand2;
    				stack.push(intResult.toString());break;
    			case "/":
    				intOperand2 = new Integer(stack.pop());
    				intOperand1 = new Integer(stack.pop());
    				intResult = intOperand1 / intOperand2;
    				stack.push(intResult.toString());break;
    			case ">":
    				intOperand2 = new Integer(stack.pop());
    				intOperand1 = new Integer(stack.pop());
    				result = intOperand1 > intOperand2;
    				stack.push(result.toString());break;
    			case "<":
    				intOperand2 = new Integer(stack.pop());
    				intOperand1 = new Integer(stack.pop());
    				result = intOperand1 < intOperand2;
    				stack.push(result.toString());break;
    			case "=":
    				result = (stack.pop().compareTo(stack.pop()) == 0);
    				stack.push(result.toString());break;
    			default:
    				if(tuple.getSchema().getFieldNames().contains(postfix.get(i))){
    					stack.push(tuple.getField(postfix.get(i)).toString());
    				}
    				else if(postfix.get(i).startsWith("\"")){
    					stack.push(postfix.get(i).replace("\"", ""));
    				}
    				else{
    					stack.push(postfix.get(i));
    				}
    		}
    	}
    	return new Boolean(stack.pop());
    }
	private void selectNatrualJoinWithOrder(String tableName1, String tableName2, String joinFieldName, String orderFieldName) 
	{
		Relation relation_reference1 = schema_manager.getRelation(tableName1);
		Relation relation_reference2 = schema_manager.getRelation(tableName2);
		String[] sortNameArray = new String[1];
	    sortNameArray[0] = joinFieldName; 
	    firstPassSort(relation_reference1, sortNameArray);//sort and write back to disk
	    firstPassSort(relation_reference2, sortNameArray);//sort and write back to disk
	    String[] orderNameArray = new String[1];
	    orderNameArray[0] = orderFieldName; 
		natrualJoinSelectionWithOrder(relation_reference1, relation_reference2, sortNameArray, orderNameArray);
	}
	private void natrualJoinSelectionWithOrder(Relation relation_reference1,
			Relation relation_reference2, String[] sortNameArray,
			String[] orderNameArray) 
	{
		ArrayList<String> fieldNames1= relation_reference1.getSchema().getFieldNames();
    	for(int i = 0; i < fieldNames1.size(); i++)
    	{
    		String newName = relation_reference1.getRelationName() + "." + fieldNames1.get(i);
    		fieldNames1.set(i, newName);
    	}
    	ArrayList<FieldType> fieldTypes1= relation_reference1.getSchema().getFieldTypes();
    	ArrayList<String> fieldNames2= relation_reference2.getSchema().getFieldNames();
    	for(int i = 0; i < fieldNames2.size(); i++)
    	{
    		String newName = relation_reference2.getRelationName() + "." + fieldNames2.get(i);
    		fieldNames2.set(i, newName);
    	}
    	ArrayList<FieldType> fieldTypes2 = relation_reference2.getSchema().getFieldTypes();
    	fieldNames1.addAll(fieldNames2);
    	fieldTypes1.addAll(fieldTypes2);
    	Schema newSchema=new Schema(fieldNames1,fieldTypes1);

	    Relation new_relation_reference = schema_manager.createRelation("newrelation", newSchema);
    	Heap2 heap1 = new Heap2(sortNameArray);
    	Heap2 heap2 = new Heap2(sortNameArray);
    	int relationBlockNum1 = relation_reference1.getNumOfBlocks();
    	int relationBlockNum2 = relation_reference2.getNumOfBlocks();
    	int sublistNum1 = (relationBlockNum1 + 9) / 10;
    	int sublistNum2 = (relationBlockNum2 + 9) / 10;
    	
    	for(int i = 0; i < sublistNum1; i++)
    	{//put the first block of each sublist of relation1 into memory, starting from 0
	    	relation_reference1.getBlock(i*10, i);
	    	TuplePosition tp = new TuplePosition(mem.getBlock(i).getTuple(0), i, 0, i*10);
			heap1.insert(tp);
    	}
    	for(int i = 0; i < sublistNum2; i++)
    	{//put the first block of each sublist of relation2 into memory, starting from sublistNum1
	    	relation_reference2.getBlock(i*10, i + sublistNum1);
	    	TuplePosition tp = new TuplePosition(mem.getBlock(i + sublistNum1).getTuple(0),i+sublistNum1,0,i*10);
			heap2.insert(tp);
    	}
    	    	
		 while(heap1.length() > 1 && heap2.length() > 1)
		 {
	    	TuplePosition tp1 = heap1.minTuple();
			TuplePosition tp2 = heap2.minTuple();
			
			if(tp1.tuple.getField(sortNameArray[0]).integer > tp2.tuple.getField(sortNameArray[0]).integer)
			{
				heap2.delete(1);
				insertToHeap(tp2, relation_reference2, heap2);
			}
			else if(tp1.tuple.getField(sortNameArray[0]).integer < tp2.tuple.getField(sortNameArray[0]).integer)
			{
				heap1.delete(1);
				insertToHeap(tp1, relation_reference1, heap1);
			}
			else
			{
				tp1.tuple.getNumOfFields();
				Tuple newTuple = new_relation_reference.createTuple();
				newTuple = joinTwoTuples(tp1.tuple, tp2.tuple, newTuple);
				appendTupleToRelation(new_relation_reference, mem, 9, newTuple);//write the natural join result back to disk 
				heap1.delete(1);
				insertToHeap(tp1, relation_reference1, heap1);
				heap2.delete(1);
				insertToHeap(tp2, relation_reference2, heap2);
				
				while(heap1.length()>1 && heap1.compareTo(tp1, heap1.minTuple())==0)
				{
					TuplePosition newTP1 = heap1.minTuple();
					newTuple = new_relation_reference.createTuple();
					newTuple = joinTwoTuples(newTP1.tuple, tp2.tuple, newTuple);
					appendTupleToRelation(new_relation_reference, mem, 9, newTuple);//write the natural join result back to disk
					
					heap1.delete(1);
					insertToHeap(newTP1, relation_reference1, heap1);
				}
				while(heap2.length()>1 && heap2.compareTo(tp2, heap2.minTuple())==0){
					TuplePosition newTP2 = heap2.minTuple();
					newTuple = new_relation_reference.createTuple();
					newTuple = joinTwoTuples(tp1.tuple, newTP2.tuple, newTuple);
					appendTupleToRelation(new_relation_reference, mem, 9, newTuple);//write the natural join result back to disk
					
					heap2.delete(1);
					insertToHeap(newTP2, relation_reference2, heap2);
				}
			}
		 }
		ArrayList<Tuple> tupleList = new ArrayList<Tuple>(); 
		for (int i = 0; i < new_relation_reference.getNumOfBlocks(); i++)
		{
			new_relation_reference.getBlock(i, 8);
			tupleList.add(mem.getBlock(8).getTuple(0));
		}
		Heap.sort(tupleList, new_relation_reference.getSchema(), "course.exam");
		for (Tuple t : tupleList)
			System.out.println(t.toString());
		schema_manager.deleteRelation("newrelation");//delete temporary relation		
	}
	private void projectDistinctNatrualJoin(String projectNameList, String tableName1, String tableName2, String joinFieldName)
	{
		Relation relation_reference1 = schema_manager.getRelation(tableName1);
		Relation relation_reference2 = schema_manager.getRelation(tableName2);
		String[] sortNameArray = new String[1];
	    sortNameArray[0] = joinFieldName; 
	   firstPassSort(relation_reference1, sortNameArray);//sort and write back to disk
	   firstPassSort(relation_reference2, sortNameArray);//sort and write back to disk
	    String[] outputNameArray = projectNameList.split(", ");
		natrualJoinProjectionDistinct(relation_reference1, relation_reference2, sortNameArray, outputNameArray);
	}
	private void natrualJoinProjectionDistinct(Relation relation_reference1, Relation relation_reference2, 
			String[] sortNameArray, String[] outputNameArray) 
	{
		ArrayList<String> fieldNames1= relation_reference1.getSchema().getFieldNames();
    	for(int i=0; i<fieldNames1.size(); i++)
    	{
    		String newName = relation_reference1.getRelationName() + "." + fieldNames1.get(i);
    		fieldNames1.set(i, newName);
    	}
    	ArrayList<FieldType> fieldTypes1= relation_reference1.getSchema().getFieldTypes();
    	ArrayList<String> fieldNames2= relation_reference2.getSchema().getFieldNames();
    	for(int i=0; i<fieldNames2.size(); i++)
    	{
    		String newName = relation_reference2.getRelationName() + "." + fieldNames2.get(i);
    		fieldNames2.set(i, newName);
    	}
    	ArrayList<FieldType> fieldTypes2= relation_reference2.getSchema().getFieldTypes();
    	fieldNames1.addAll(fieldNames2);
    	fieldTypes1.addAll(fieldTypes2);
    	Schema newSchema=new Schema(fieldNames1,fieldTypes1);

	    Relation new_relation_reference=schema_manager.createRelation("newrelation",newSchema);
    	//write nameArray to output
    	
		//second pass of two passes
    	Heap2 heap1 = new Heap2(sortNameArray);
    	Heap2 heap2 = new Heap2(sortNameArray);
    	int relationBlockNum1 = relation_reference1.getNumOfBlocks();
    	int relationBlockNum2 = relation_reference2.getNumOfBlocks();
    	int sublistNum1 = (relationBlockNum1+9)/10;
    	int sublistNum2 = (relationBlockNum2+9)/10;
    	
    	for(int i=0; i<sublistNum1; i++)
    	{//put the first block of each sublist of relation1 into memory, starting from 0
	    	relation_reference1.getBlock(i*10, i);
	    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i).getTuple(0),i,0,i*10);
			heap1.insert(tuplePlus);
    	}
    	for(int i=0; i<sublistNum2; i++){//put the first block of each sublist of relation2 into memory, starting from sublistNum1
	    	relation_reference2.getBlock(i*10, i+sublistNum1);
	    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i+sublistNum1).getTuple(0),i+sublistNum1,0,i*10);
			heap2.insert(tuplePlus);
    	}
    	    	
		 while(heap1.length()>1 && heap2.length()>1){
	    	TuplePosition tuplePlus1 = heap1.minTuple();
			TuplePosition tuplePlus2 = heap2.minTuple();
			
			if(tuplePlus1.tuple.getField(sortNameArray[0]).integer>tuplePlus2.tuple.getField(sortNameArray[0]).integer){
				heap2.delete(1);
				insertToHeap(tuplePlus2, relation_reference2, heap2);
			}
			else if(tuplePlus1.tuple.getField(sortNameArray[0]).integer<tuplePlus2.tuple.getField(sortNameArray[0]).integer){
				heap1.delete(1);
				insertToHeap(tuplePlus1, relation_reference1, heap1);
			}
			else{
				tuplePlus1.tuple.getNumOfFields();
				//process tuplePlus1,tuplePlus2;
				Tuple newTuple = new_relation_reference.createTuple();
				newTuple = joinTwoTuples(tuplePlus1.tuple, tuplePlus2.tuple, newTuple);
				appendTupleToRelation(new_relation_reference, mem, 9, newTuple);//write the natural join result back to disk 
				
				heap1.delete(1);
				insertToHeap(tuplePlus1, relation_reference1, heap1);
				heap2.delete(1);
				insertToHeap(tuplePlus2, relation_reference2, heap2);
				
				while(heap1.length()>1 && heap1.compareTo(tuplePlus1, heap1.minTuple())==0){
					TuplePosition newTuplePlus1 = heap1.minTuple();
					//process newTuplePlus1,tuplePlus2;
					newTuple = new_relation_reference.createTuple();
					newTuple = joinTwoTuples(newTuplePlus1.tuple, tuplePlus2.tuple, newTuple);
					appendTupleToRelation(new_relation_reference, mem, 9, newTuple);//write the natural join result back to disk
					
					heap1.delete(1);
					insertToHeap(newTuplePlus1, relation_reference1, heap1);
				}
				while(heap2.length()>1 && heap2.compareTo(tuplePlus2, heap2.minTuple())==0){
					TuplePosition newTuplePlus2 = heap2.minTuple();
					//process tuplePlus1,newTuplePlus2;
					newTuple = new_relation_reference.createTuple();
					newTuple = joinTwoTuples(tuplePlus1.tuple, newTuplePlus2.tuple, newTuple);
					appendTupleToRelation(new_relation_reference, mem, 9, newTuple);//write the natural join result back to disk
					
					heap2.delete(1);
					insertToHeap(newTuplePlus2, relation_reference2, heap2);
				}
			}							    
		 }
		 firstPassSort(new_relation_reference, outputNameArray);
		 relationSortOutput(new_relation_reference, outputNameArray, outputNameArray, true);
		 schema_manager.deleteRelation("newrelation");//delete temporary relation
		
	}
	private void projectNatrualJoin(String projectNameList, String tableName1, String tableName2, String joinFieldName)
	{
		Relation relation_reference1 = schema_manager.getRelation(tableName1);
		Relation relation_reference2 = schema_manager.getRelation(tableName2);
		String[] sortNameArray = new String[1];
	    sortNameArray[0] = joinFieldName; 
	    firstPassSort(relation_reference1, sortNameArray); 
	    firstPassSort(relation_reference2, sortNameArray); 
	    String[] outputNameArray = projectNameList.split(", ");
		natrualJoinProjection(relation_reference1, relation_reference2, sortNameArray, outputNameArray);
	}
	private void natrualJoinProjection(Relation relation_reference1, Relation relation_reference2, String[] sortNameArray, String[] outputNameArray){
	    	//create new relation using relation1 and relation2
	    	ArrayList<String> fieldNames1= relation_reference1.getSchema().getFieldNames();
	    	for(int i=0; i < fieldNames1.size(); i++)
	    	{
	    		String newName = relation_reference1.getRelationName() + "." + fieldNames1.get(i);
	    		fieldNames1.set(i, newName);
	    	}
	    	ArrayList<FieldType> fieldTypes1= relation_reference1.getSchema().getFieldTypes();
	    	ArrayList<String> fieldNames2= relation_reference2.getSchema().getFieldNames();
	    	for(int i = 0; i < fieldNames2.size(); i++)
	    	{
	    		String newName = relation_reference2.getRelationName() + "." + fieldNames2.get(i);
	    		fieldNames2.set(i, newName);
	    	}
	    	ArrayList<FieldType> fieldTypes2= relation_reference2.getSchema().getFieldTypes();
	    	fieldNames1.addAll(fieldNames2);
	    	fieldTypes1.addAll(fieldTypes2);
	    	Schema newSchema=new Schema(fieldNames1,fieldTypes1);

		    Relation new_relation_reference=schema_manager.createRelation("newrelation",newSchema);
	    	//write nameArray to output
	    	String nameArrayOutput = "";
	    	for(int i = 0; i < outputNameArray.length; i++)
	    	{
	    		nameArrayOutput += outputNameArray[i]+"\t";
	    		System.out.print(outputNameArray[i]+"\t");
	    	}
	    	System.out.println();
			writeOutput(nameArrayOutput);
			//second pass of two pass
	    	Heap2 heap1 = new Heap2(sortNameArray);
	    	Heap2 heap2 = new Heap2(sortNameArray);
	    	int relationBlockNum1 = relation_reference1.getNumOfBlocks();
	    	int relationBlockNum2 = relation_reference2.getNumOfBlocks();
	    	int sublistNum1 = (relationBlockNum1+9)/10;
	    	int sublistNum2 = (relationBlockNum2+9)/10;
	    	
	    	for(int i=0; i<sublistNum1; i++)
	    	{
		    	relation_reference1.getBlock(i*10, i);
		    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i).getTuple(0),i,0,i*10);
				heap1.insert(tuplePlus);
	    	}
	    	for(int i=0; i<sublistNum2; i++)
	    	{//put the first block of each sublist of relation2 into memory, starting from sublistNum1
		    	relation_reference2.getBlock(i*10, i+sublistNum1);
		    	TuplePosition tuplePlus = new TuplePosition(mem.getBlock(i+sublistNum1).getTuple(0),i+sublistNum1,0,i*10);
				heap2.insert(tuplePlus);
	    	}
	    	    	
			 while(heap1.length()>1 && heap2.length()>1)
			 {
			    	TuplePosition tuplePlus1 = heap1.minTuple();
					TuplePosition tuplePlus2 = heap2.minTuple();
					
					if(tuplePlus1.tuple.getField(sortNameArray[0]).integer>tuplePlus2.tuple.getField(sortNameArray[0]).integer){
						heap2.delete(1);
						insertToHeap(tuplePlus2, relation_reference2, heap2);
					}
					else if(tuplePlus1.tuple.getField(sortNameArray[0]).integer<tuplePlus2.tuple.getField(sortNameArray[0]).integer){
						heap1.delete(1);
						insertToHeap(tuplePlus1, relation_reference1, heap1);
					}
					else{
						tuplePlus1.tuple.getNumOfFields();
						//process tuplePlus1,tuplePlus2;
						Tuple newTuple = new_relation_reference.createTuple();
						newTuple = joinTwoTuples(tuplePlus1.tuple, tuplePlus2.tuple, newTuple);
						writeToOutput(newTuple, outputNameArray);
						
						heap1.delete(1);
						insertToHeap(tuplePlus1, relation_reference1, heap1);
						heap2.delete(1);
						insertToHeap(tuplePlus2, relation_reference2, heap2);
						
						while(heap1.length()>1 && heap1.compareTo(tuplePlus1, heap1.minTuple())==0){
							TuplePosition newTuplePlus1 = heap1.minTuple();
							//process newTuplePlus1,tuplePlus2;
							newTuple = new_relation_reference.createTuple();
							newTuple = joinTwoTuples(newTuplePlus1.tuple, tuplePlus2.tuple, newTuple);
							writeToOutput(newTuple, outputNameArray);
							
							heap1.delete(1);
							insertToHeap(newTuplePlus1, relation_reference1, heap1);
						}
						while(heap2.length()>1 && heap2.compareTo(tuplePlus2, heap2.minTuple())==0){
							TuplePosition newTuplePlus2 = heap2.minTuple();
							//process tuplePlus1,newTuplePlus2;
							newTuple = new_relation_reference.createTuple();
							newTuple = joinTwoTuples(tuplePlus1.tuple, newTuplePlus2.tuple, newTuple);
							writeToOutput(newTuple, outputNameArray);
							
							heap2.delete(1);
							insertToHeap(newTuplePlus2, relation_reference2, heap2);
						}
					}							    
			  }
			 schema_manager.deleteRelation("newrelation");//delete temporary relation
	   	       	
	    }
	
	private void firstPassSort(Relation relation_reference, String[] nameArray)
	 {//first pass of two pass
		 Heap2 heap = new Heap2(nameArray);
    	int relationBlockNum = relation_reference.getNumOfBlocks();
    	int i=0;
    	for(i=0; i < relationBlockNum / 10; i++)
    	{
	    	relation_reference.getBlocks(i*10, 0, 10);
	    	for(int j=0; j<10; j++)
	    	{
	    		Block block = mem.getBlock(j);
	    		for(int k=0; k<block.getNumTuples(); k++)
	    		{
	    			Tuple tuple = block.getTuple(k);
	    			TuplePosition tp = new TuplePosition(tuple,j,k);
	    			heap.insert(tp);
	    		}
	    	}
	    	for(int j=0; j<10; j++)
	    	{
	    		Block block = mem.getBlock(j);
	    		block.clear();
	    		while(!block.isFull()&&heap.length()>1)
	    		{
	    			TuplePosition tuplePlus = heap.minTuple();
	    			heap.delete(1);
	    			block.appendTuple(tuplePlus.tuple);
	    		}
	    	}
	    	relation_reference.setBlocks(i*10, 0, 10);
    	}
    	
    	int remainBlockNum = relationBlockNum-i*10;
    	if(remainBlockNum>0)
    	{
	    	relation_reference.getBlocks(i*10, 0, remainBlockNum);
	    	for(int j=0; j<remainBlockNum; j++){
	    		Block block = mem.getBlock(j);
	    		for(int k=0; k<block.getNumTuples(); k++)
	    		{
	    			Tuple tuple = block.getTuple(k);
	    			TuplePosition tuplePlus = new TuplePosition(tuple,j,k);
	    			heap.insert(tuplePlus);
	    		}
	    	}
	    	for(int j=0; j<remainBlockNum; j++)
	    	{
	    		Block block = mem.getBlock(j);
	    		block.clear();
	    		while(!block.isFull()&&heap.length()>1){
	    			TuplePosition tuplePlus = heap.minTuple();
	    			heap.delete(1);
	    			block.appendTuple(tuplePlus.tuple);
	    		}
	    	}
	    	relation_reference.setBlocks(i*10, 0, remainBlockNum);
    	}
    	
    }
	private boolean insertToHeap(TuplePosition tp,Relation relation, Heap2 h)
	{
    	if(tp.tupleInBlock<mem.getBlock(tp.memBlockPosition).getNumTuples() - 1)
    	{
	    	Tuple tuple = mem.getBlock(tp.memBlockPosition).getTuple(tp.tupleInBlock+1);
			h.insert(new TuplePosition(tuple,tp.memBlockPosition,tp.tupleInBlock+1,tp.diskBlockPosition));
			return true;
	    }
	    else if(tp.diskBlockPosition%10<9 && tp.diskBlockPosition<relation.getNumOfBlocks()-1){//sublist not exhaust
	    	relation.getBlock(tp.diskBlockPosition+1, tp.memBlockPosition);
	    	h.insert(new TuplePosition(mem.getBlock(tp.memBlockPosition).getTuple(0),tp.memBlockPosition,0,tp.diskBlockPosition+1));
	    	return true;
	    }
    	return false;
    }
	private void relationSortOutput(Relation relation_reference, String[] sortNameArray, 
			String[] outputNameArray, boolean distinct)
	{
    	String nameArrayOutput = "";
    	for(int i = 0; i < outputNameArray.length; i++)
    	{
    		nameArrayOutput += outputNameArray[i] + "\t";
    		System.out.print(outputNameArray[i] + "\t");
    	}
    	System.out.println();
		writeOutput(nameArrayOutput);
		//second pass of 2 passes
    	Heap2 heap = new Heap2(sortNameArray);
    	int relationBlockNum = relation_reference.getNumOfBlocks();
    	int[] pointer = null;
    	if(relationBlockNum%10 == 0)
    		pointer = new int[relationBlockNum / 10];
    	else
    		pointer = new int[relationBlockNum / 10 + 1];
    	for(int i = 0; i < pointer.length; i++)
    	{
    		pointer[i] = 0;
	    	relation_reference.getBlock(i * 10, i);
    	}
    	
    	for(int j = 0; j < pointer.length; j++)
    	{
    		Block block = mem.getBlock(j);
			Tuple tuple = block.getTuple(0);
			TuplePosition tp = new TuplePosition(tuple,j,0);
			heap.insert(tp);
    	}
    	
    	if(distinct)
    	{
			TuplePosition outputTP = heap.minTuple();
			this.writeToOutput(outputTP.tuple, outputNameArray);
			while(heap.length() > 1)
			{
		    	TuplePosition tp = heap.minTuple();
				heap.delete(1);
				if(heap.compareTo(tp, outputTP) != 0)
				{//only output distinct tuple
					this.writeToOutput(tp.tuple, outputNameArray);
					outputTP = tp;
				}
			    if(tp.tupleInBlock < mem.getBlock(tp.memBlockPosition).getNumTuples()-1){//block not exhaust
			    	Tuple tuple = mem.getBlock(tp.memBlockPosition).getTuple(tp.tupleInBlock + 1);
					heap.insert(new TuplePosition(tuple, tp.memBlockPosition, tp.tupleInBlock+1));
			    }
			    else if(pointer[tp.memBlockPosition] < 9 && tp.memBlockPosition*10+pointer[tp.memBlockPosition]<relationBlockNum-1){//sublist not exhaust
			    	pointer[tp.memBlockPosition] += 1;
			    	relation_reference.getBlock(tp.memBlockPosition*10+pointer[tp.memBlockPosition], tp.memBlockPosition);
			    	heap.insert(new TuplePosition(mem.getBlock(tp.memBlockPosition).getTuple(0),tp.memBlockPosition,0));
			    }
		  }
		}
    	else
    	{
    		 while(heap.length()>1)
    		 {
    		    	TuplePosition tuplePlus = heap.minTuple();
    				heap.delete(1);   				
    				this.writeToOutput(tuplePlus.tuple, outputNameArray);
    				//this.writeToOutput(tuplePlus.tuple.toString());//should call different output method to deal with distinct or order
    			    if(tuplePlus.tupleInBlock<mem.getBlock(tuplePlus.memBlockPosition).getNumTuples()-1){//block not exhaust
    			    	Tuple tuple = mem.getBlock(tuplePlus.memBlockPosition).getTuple(tuplePlus.tupleInBlock+1);
    					heap.insert(new TuplePosition(tuple,tuplePlus.memBlockPosition,tuplePlus.tupleInBlock+1));
    			    }
    			    else if(pointer[tuplePlus.memBlockPosition]<9 && tuplePlus.memBlockPosition*10+pointer[tuplePlus.memBlockPosition]<relationBlockNum-1){//sublist not exhaust
    			    	pointer[tuplePlus.memBlockPosition] += 1;
    			    	relation_reference.getBlock(tuplePlus.memBlockPosition*10+pointer[tuplePlus.memBlockPosition], tuplePlus.memBlockPosition);
    			    	heap.insert(new TuplePosition(mem.getBlock(tuplePlus.memBlockPosition).getTuple(0),tuplePlus.memBlockPosition,0));
    			    }
    		  }
    	}	       	
    }
	private void writeOutput(Tuple tuple, String[] outputName) 
	{
		try(PrintWriter out = new PrintWriter(new FileWriter(outputFile, true)))
		{
    		for(int i=0; i<outputName.length; i++)
    		{
        		out.print(tuple.getField(outputName[i])+"\t");
        		System.out.print(tuple.getField(outputName[i])+"\t");
    		}
    		System.out.println();
    		out.println();
	    	out.close();
	    } catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
	}
	private Tuple joinTwoTuples(Tuple t1, Tuple t2, Tuple newTuple)
	{
		for (int i = 0; i < t1.getNumOfFields(); i++)
		{
			if (t1.getField(i).type == FieldType.INT)
				newTuple.setField(i, t1.getField(i).integer);
			else
				newTuple.setField(i, t1.getField(i).str);
		}
		for (int i = 0; i < t2.getNumOfFields(); i++)
		{
			if(t2.getField(i).type == FieldType.INT)
				newTuple.setField(i+t1.getNumOfFields(),t2.getField(i).integer);
			else
				newTuple.setField(i+t1.getNumOfFields(),t2.getField(i).str);	
		}
		return newTuple;
	}
	public void writeOutput(String l)
	{
		try(PrintWriter out = new PrintWriter(new FileWriter(outputFile, true)))
		{
			out.println(l);
			out.close();
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} catch (IOException e1) 
		{
			e1.printStackTrace();
		}
	}
	public void writeToOutput(Tuple tuple, String[] fieldNameArray)
	{
    	try(PrintWriter out = new PrintWriter(new FileWriter(outputFile, true)))
    	{
    		for(int i=0; i<fieldNameArray.length; i++)
    		{
        		out.print(tuple.getField(fieldNameArray[i])+"\t");
        		System.out.print(tuple.getField(fieldNameArray[i])+"\t");
        	}
    		System.out.println();
    		out.println();
	    	out.close();
	    } catch (FileNotFoundException e) 
    	{
			e.printStackTrace();
		} catch (IOException e1) 
    	{
			e1.printStackTrace();
		}
    	
    }
	private boolean insertToList(TuplePosition tp1, Relation relation_reference,
			ArrayList<TuplePosition> list) 
	{
		if (tp1.tupleInBlock < mem.getBlock(tp1.memBlockPosition).getNumTuples() - 1)
		{//take care of block
			Tuple tuple = mem.getBlock(tp1.memBlockPosition).getTuple(tp1.tupleInBlock + 1);
			TuplePosition t = new TuplePosition(tuple, tp1.memBlockPosition, tp1.tupleInBlock + 1, tp1.diskBlockPosition); 
			list.add(t);
			return true;
		}
		else if (tp1.diskBlockPosition % 10 < 9 && tp1.diskBlockPosition < (relation_reference.getNumOfBlocks() - 1))
		{//take care of sublist
			relation_reference.getBlock(tp1.diskBlockPosition + 1, tp1.memBlockPosition);
			Tuple tuple = mem.getBlock(tp1.memBlockPosition).getTuple(0);
			TuplePosition t = new TuplePosition(tuple, tp1.memBlockPosition, 0, tp1.diskBlockPosition + 1);
			list.add(t);
			return true;
		}
		return false;
	}

	private void insertTuple(String patternInsert, String str)
	{
		Pattern p = Pattern.compile(patternInsert);
		Matcher m = p.matcher(str);
		m.find();//m.group(1) is table name
		String table_name = m.group(1);
		String fieldName = m.group(2);
		String[] fName = fieldName.split(", | ");//field name
		//field_names = new ArrayList<String>(Arrays.asList(fName));//convert string array to array list
		Relation relation_reference1 = schema_manager.getRelation(m.group(1));
		Tuple tuple = relation_reference1.createTuple();
		Schema tuple_schema = tuple.getSchema();
		
		String fieldValue = m.group(3);
		String[] fValue = fieldValue.split(", | ");
		//System.out.println(str + ": ");
		for (int i = 0; i < fValue.length; i++)
		{//remove quotes
			fValue[i] = fValue[i].replace("\"", "");
		}
		if (relation_reference1.getNumOfTuples() == 0)
		{//for the first time insertion, we could insert directly
			//System.out.println("!!!!First time");
			for (int i = 0; i < tuple.getNumOfFields(); i++)
			{
				if (tuple_schema.getFieldType(i) == FieldType.INT)
				//convert String to INT when field type is INT
					tuple.setField(i, Integer.parseInt(fValue[i]));
				else
					tuple.setField(i, fValue[i]);
			}
		}
		else
		{//For not first time insertion, we have to confirm the field name first, then insert
			for (int i = 0; i < tuple.getNumOfFields(); i++)
			{
				if (tuple_schema.getFieldType(fName[i]) == FieldType.INT)
				//Get field type via field name
					tuple.setField(fName[i], Integer.parseInt(fValue[i]));
				else
					tuple.setField(fName[i], fValue[i]);
			}
		}
		
		appendTupleToRelation(relation_reference1, mem, 2, tuple);//append tuple from mem to disk
		System.out.println(str + ": " + "\n" + relation_reference1);
	}
	private  ArrayList<String> infixToPostfix(ArrayList<String> infixExpression) {
		ArrayList<String>expression = infixExpression;
        ArrayList postfixString = new ArrayList();
        Stack<String> stack = new Stack<String>();
        HashMap<String, Integer> prec = new HashMap<String, Integer>();
        prec.put("*", 5);
        prec.put("/", 5);
        prec.put("+", 4);
        prec.put("-", 4);
        prec.put(">", 3);
        prec.put("<", 3);
        prec.put("=", 3);
        prec.put("NOT", 2);
        prec.put("AND", 1);
        prec.put("OR", 0);
        prec.put("(", -1);
        prec.put(")", -1);
        prec.put("[", -1);
        prec.put("]", -1);
        for (int index = 0; index < expression.size(); index++) 
        {
        	 String value = expression.get(index);
             if (value.equals("(")) 
                 stack.push(value); 
             else if (value.equals(")")) 
             {
                 String oper = stack.pop();
                 while (!(oper.equals("("))) 
                 {
 	                postfixString.add(oper);
 	                oper = stack.pop();
                 }
             } 
             else if (value.equals("[")) 
                 stack.push(value); 
             else if (value.equals("]")) 
             {
                 String oper = stack.pop();
                 while (!(oper.equals("["))) 
                 {
 	                postfixString.add(oper);
 	                oper = stack.pop();
                 }
             } 
             else if ((!value.equals("+")) && (!value.equals("-")) && (!value.equals("*"))
             		&& (!value.equals("/") && (!value.equals("=")) && (!value.equals("OR"))
                    && (!value.equals("AND"))) && (!value.equals("NOT"))&& (!value.equals(">")) 
                    && (!value.equals("<")))
             	postfixString.add(value);
             else
             {
             	while ((!stack.isEmpty()) && (prec.get(stack.peek()) >= (prec.get(value))))
             	{
             		String oper = stack.pop();
         			postfixString.add(oper);
             	}
             	stack.push(value);
             }
        }
        while (!stack.isEmpty()) 
            postfixString.add(stack.pop());
        return postfixString;
    }
	
	public String postfixCompute(Tuple tuple, ArrayList<String> list)
	{
		Stack<String> stack = new Stack<String>();
		for (String s: list)
		{
			if (s.equals("*"))
			{
				stack.push(Integer.toString(Integer.parseInt(stack.pop()) * Integer.parseInt(stack.pop())));
			}
			else if (s.equals("/"))
			{
				int t1 = Integer.parseInt(stack.pop());
				int t2 = Integer.parseInt(stack.pop());
				stack.push(Integer.toString(t2 / t1));
			}
			else if (s.equals("+")) 
			{
				stack.push(Integer.toString(Integer.parseInt(stack.pop()) + Integer.parseInt(stack.pop())));
			}
			else if (s.equals("-"))
			{
				int t1 = Integer.parseInt(stack.pop());
				int t2 = Integer.parseInt(stack.pop());
				stack.push(Integer.toString(t2 - t1));
			}
			else if (s.equals("="))
			{
				String t1 = stack.pop();//100
				String t2 = stack.pop();//field value
				stack.push(Boolean.toString(t1.equals(t2)));
			}
			else if (s.equals(">"))
			{
				int t1 = Integer.parseInt(stack.pop());
				int t2 = Integer.parseInt(stack.pop());
				stack.push(Boolean.toString(t2 > t1));
			}
			else if (s.equals("<"))
			{
				int t1 = Integer.parseInt(stack.pop());
				int t2 = Integer.parseInt(stack.pop());
				stack.push(Boolean.toString(t2 < t1));
			}
			else if (s.equals("AND"))
			{
				Boolean t1 = Boolean.parseBoolean(stack.pop());
				Boolean t2 = Boolean.parseBoolean(stack.pop());
				stack.push(Boolean.toString(t2 && t1));
			}
			else if (s.equals("OR"))
			{
				Boolean t1 = Boolean.parseBoolean(stack.pop());
				Boolean t2 = Boolean.parseBoolean(stack.pop());
				stack.push(Boolean.toString(t2 || t1));
			}
			else if (s.equals("NOT"))
			{
				Boolean t1 = Boolean.parseBoolean(stack.pop());
				stack.push(Boolean.toString(!t1));
			}
			else if (tuple.getSchema().fieldNameExists(s))//field name 
				stack.push(tuple.getField(s).toString());
			else stack.push(s);//integer value
		}
		return stack.pop();
	}
	private void createTable(String tableName, String[] NameAndType)
	{
		ArrayList<String> field_names = new ArrayList<String>();
		ArrayList<FieldType> field_types=new ArrayList<FieldType>();
		for(int i = 0; i < NameAndType.length-1; i++)
		{
			 field_names.add(NameAndType[i]);
			 i++;
			 if (NameAndType[i].equalsIgnoreCase("STR20"))
				 field_types.add(FieldType.STR20);
			 else if (NameAndType[i].equalsIgnoreCase("INT"))
				 field_types.add(FieldType.INT);
			 else
				 System.out.println("Error in FieldType");
		}
		Schema schema=new Schema(field_names, field_types);
		Relation relation_reference = schema_manager.createRelation(tableName, schema);
		System.out.println("We created a table: '" + tableName + "' with following schema: "+ "\n" + schema);
	}
	//appendTupleToRelation is copied from TestStorageManager to save tuple from mem to disk
	private static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) 
	{
		    Block block_reference;
		    if (relation_reference.getNumOfBlocks()==0) {
		      //System.out.print("The relation is empty" + "\n");
		      //System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
		      block_reference=mem.getBlock(memory_block_index);
		      block_reference.clear(); //clear the block
		      block_reference.appendTuple(tuple); // append the tuple
		      //System.out.print("Write to the first block of the relation" + "\n");
		      relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
		    } else {
		      //System.out.print("Read the last block of the relation into memory block 5:" + "\n");
		      relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
		      block_reference=mem.getBlock(memory_block_index);

		      if (block_reference.isFull()) {
		        //System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
		        block_reference.clear(); //clear the block
		        block_reference.appendTuple(tuple); // append the tuple
		        //System.out.print("Write to a new block at the end of the relation" + "\n");
		        relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
		      } else {
		        //System.out.print("(The block is not full: Append it directly)" + "\n");
		        block_reference.appendTuple(tuple); // append the tuple
		        //System.out.print("Write to the last block of the relation" + "\n");
		        relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
		      }
		    }
		  }
	
	
public static void main(String[] args)  
{
    long start = System.currentTimeMillis(); 
    TinyParser tp = new TinyParser();
	tp.disk.resetDiskIOs();
	tp.disk.resetDiskTimer();
		
    
    /*String str = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
	tp.parser(str);
	str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, \"A\")";
    tp.parser(str);
    str = "SELECT * FROM course";
    tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (2, 0, 100, 100, \"E\")";
    tp.parser(str);
    str = "SELECT * FROM course";
    tp.parser(str);
    str = "INSERT INTO course (sid, grade, exam, project, homework) VALUES (3, \"E\", 100, 100, 100)";
    tp.parser(str);
    str = "SELECT * FROM course";
    tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course";
    tp.parser(str);
    str = "SELECT * FROM course";
    tp.parser(str);
    str = "DELETE FROM course WHERE grade = \"E\"";
    tp.parser(str);
    //str = "SELECT * FROM course";
   // tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (4, 99, 100, 100, \"B\")";
    tp.parser(str);
   // str = "SELECT * FROM course";
   // tp.parser(str);
    str = "DELETE FROM course";
    tp.parser(str);
    //str = "SELECT * FROM course";
   // tp.parser(str);
    str = "DROP TABLE course";
    tp.parser(str);*/
    String str = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
    tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, \"A\")";
    tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 100, 99, \"C\")";
    tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (3, 100, 100, 100, \"C\")";
    tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (16, 0, 0, 1, \"E\")";
    tp.parser(str);tp.parser(str);tp.parser(str);tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (16, 0, 0, 1, \"A\")";
    //tp.parser(str);tp.parser(str);tp.parser(str);tp.parser(str);
    //str = "SELECT sid, grade FROM course";
    //tp.parser(str);
    
    /*str = "SELECT * FROM course ORDER BY exam";
    tp.parser(str);*/
    str = "SELECT * FROM course";
    tp.parser(str);
    str = "CREATE TABLE course2 (sid INT, exam INT, grade STR20)";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (1, 100, \"A\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (16, 25, \"E\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (17, 0, \"A\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (2, 99, \"B\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (3, 98, \"C\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (4, 97, \"D\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (5, 66, \"A\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (6, 65, \"B\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (18, 0, \"A\")";
    tp.parser(str);
    //str ="SELECT * FROM course, course2";
    //tp.parser(str);
    /*Relation r1 = tp.schema_manager.getRelation("course");
    ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
   r1.getBlocks(0, 0, r1.getNumOfBlocks());
   tupleList.addAll(tp.mem.getTuples(0, r1.getNumOfBlocks()));
    tp.firstPassSort(r1, "exam");
    System.out.println("After sort: ");
    
    
	System.out.println(tp.mem);*/
   str = "SELECT * FROM course, course2 WHERE course.sid = course2.sid ORDER BY course.exam";
  // str = "SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid";
    tp.parser(str);
    //str = "SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid AND [ "
	//		+ "course.exam > course2.exam OR course.grade = \"A\" AND course2.grade = \"A\" ] ORDER BY course.exam";
   // tp.parser(str);
   /*str = "SELECT * FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam = 100 OR course2.exam = 100 ]";
    tp.parser(str);
   str ="SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam > course2.exam";
    tp.parser(str);
    str ="SELECT * FROM course, course2 WHERE course.sid = course2.sid AND course.exam > course2.exam AND course.homework = 100";
    tp.parser(str);
    str = "SELECT * FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam > course2.exam OR course.homework = 100 ]";
    tp.parser(str);
    str = "SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid AND [ "
				+ "course.exam > course2.exam OR course.grade = \"A\" AND course2.grade = \"A\" ] ORDER BY course.exam";*/
   //s tp.parser(str);
    /*str ="SELECT * FROM course WHERE exam > 70";
    tp.parser(str);
    str = "SELECT * FROM course WHERE exam = 100 OR homework = 100 AND project = 100";
    tp.parser(str);
    str ="SELECT * FROM course WHERE exam + homework = 200";
    tp.parser(str);
    str = "SELECT * FROM course WHERE [ exam = 100 OR homework = 100 ] AND project = 100";
    tp.parser(str);
    str = "SELECT * FROM course WHERE ( exam * 30 + homework * 20 + project * 50 ) / 100 = 100";
    tp.parser(str);
    str = "SELECT * FROM course WHERE grade = \"C\" AND [ exam > 70 OR project > 70 ] AND NOT ( exam * 30 + homework * 20 + project * 50 ) / 100 < 60";
    tp.parser(str);
    str = "CREATE TABLE course2 (sid INT, exam INT, grade STR20)";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (1, 100, \"A\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (16, 25, \"E\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (17, 0, \"A\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (2, 99, \"B\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (3, 98, \"C\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (4, 97, \"D\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (5, 66, \"A\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (6, 65, \"B\")";
    tp.parser(str);
    str = "INSERT INTO course2 (sid, exam, grade) VALUES (18, 0, \"A\")";
    /*for (int i = 0; i < 15; i++)
    	tp.parser(str);
    str = "SELECT * FROM course2 ORDER BY exam";
    tp.parser(str);
    /*str = "SELECT * FROM course, course2";
    tp.parser(str);*/
    
    long elapsedTimeMillis = System.currentTimeMillis()-start; 
    /*System.out.print("Computer elapse time = " + elapsedTimeMillis + " ms" + "\n");
    System.out.print("Calculated elapse time = " + tp.disk.getDiskTimer() + " ms" + "\n");
    System.out.print("Calculated Disk I/Os = " + tp.disk.getDiskIOs() + "\n");
   */
    }
}
