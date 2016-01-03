
import java.util.ArrayList;

import storageManager.Field;
import storageManager.FieldType;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;


//Heap sort: the minimum is root.
public class Heap
{
	private Heap(){}
	public static void sort(ArrayList<Tuple> pq, Schema schema, String fieldName)
	{//sort by one field
		int N = pq.size();
		for (int k = N/2; k >= 1; k--)
		{
			sink(pq, schema, fieldName, k, N);
		}
		 while (N > 1) 
		 {
	        exch(pq, 1, N--);
	        sink(pq, schema, fieldName, 1, N);
	     }
	}
	
	public static void sortP(ArrayList<TuplePosition> pq, Schema schema, String fieldName)
	{//sort by one field
		int N = pq.size();
		for (int k = N/2; k >= 1; k--)
		{
			sinkP(pq, schema, fieldName, k, N);
		}
		 while (N > 1) 
		 {
	        exchP(pq, 1, N--);
	        sinkP(pq, schema, fieldName, 1, N);
	     }
	}
	private static void sink(ArrayList<Tuple> pq, Schema schema, String fieldName, int k, int N)
	{
		while (k <= N/2)
		{
			int j = 2*k; 
			if (j < N && less(pq, schema,fieldName, j, j+1))//select the larger child
				j++;
			if (!less(pq, schema, fieldName, k, j)) 
				break; 
			else
				exch(pq, k, j);
			k = j;
		}
	}
	private static void sinkP(ArrayList<TuplePosition> pq, Schema schema, String fieldName, int k, int N)
	{
		while (k <= N/2)
		{
			int j = 2*k; 
			if (j < N && lessP(pq, schema,fieldName, j, j+1))//select the larger child
				j++;
			if (!lessP(pq, schema, fieldName, k, j)) 
				break; 
			else
				exchP(pq, k, j);
			k = j;
		}
	}
	/*private static boolean less(ArrayList<Tuple> pq, Schema schema, ArrayList<String> fieldNameList, int i, int j)
	{
		return false;
	}*/
	private static boolean less(ArrayList<Tuple> pq, Schema schema, String fieldName, int i, int j)
	{
		String strType = "STR20";
		ArrayList<FieldType> fieldTypeList = schema.getFieldTypes();
		ArrayList<String> fieldList = schema.getFieldNames();
		if (!(fieldName == null))
		{//specific field
			FieldType fieldType = schema.getFieldType(fieldName);
			if (fieldType.toString().compareToIgnoreCase(strType) == 0)//string type
			{
				return pq.get(i-1).getField(fieldName).toString().compareTo(pq.get(j-1).getField(fieldName).toString()) < 0;
			}
				else//int type
			{
				int fieldValuei = Integer.parseInt(pq.get(i-1).getField(fieldName).toString()); 
				int fieldValuej = Integer.parseInt(pq.get(j-1).getField(fieldName).toString());
				return fieldValuei < fieldValuej;
			}
		}
		else
		{//copmare all the fields
			for (String name : fieldList)
			{
				String f = pq.get(0).getField(name).type.toString();
				if (f.equals("STR20"))//string type
				{
					if (pq.get(i-1).getField(name).toString().compareTo(pq.get(j-1).getField(name).toString()) < 0)
						return true;
					else if (pq.get(i-1).getField(name).toString().compareTo(pq.get(j-1).getField(name).toString()) == 0)
						continue;
					else return false;
				}
				else//int type
				{
					int fieldValuei = Integer.parseInt(pq.get(i-1).getField(name).toString()); 
					//System.out.println("fieldValuei: " +fieldValuei);
					int fieldValuej = Integer.parseInt(pq.get(j-1).getField(name).toString());
					//System.out.println("fieldValuej: " +fieldValuej);
					if (fieldValuei < fieldValuej) return true;
					else if (fieldValuei == fieldValuej) continue;
					else return false;
				}
			
			}
			return false;
		}
	}
	private static boolean lessP(ArrayList<TuplePosition> pq, Schema schema, String fieldName, int i, int j)
	{
		String strType = "STR20";
		ArrayList<FieldType> fieldTypeList = schema.getFieldTypes();
		ArrayList<String> fieldList = schema.getFieldNames();
		if (!(fieldName == null))
		{//specific field
			FieldType fieldType = schema.getFieldType(fieldName);
			if (fieldType.toString().compareToIgnoreCase(strType) == 0)//string type
			{
				return pq.get(i-1).tuple.getField(fieldName).toString().compareTo(pq.get(j-1).tuple.getField(fieldName).toString()) < 0;
			}
				else//int type
			{
				int fieldValuei = Integer.parseInt(pq.get(i-1).tuple.getField(fieldName).toString()); 
				int fieldValuej = Integer.parseInt(pq.get(j-1).tuple.getField(fieldName).toString());
				return fieldValuei < fieldValuej;
			}
		}
		else
		{//copmare all the fields
			for (String name : fieldList)
			{
				String f = pq.get(0).tuple.getField(name).type.toString();
				if (f.equals("STR20"))//string type
				{
					if (pq.get(i-1).tuple.getField(name).toString().compareTo(pq.get(j-1).tuple.getField(name).toString()) < 0)
						return true;
					else if (pq.get(i-1).tuple.getField(name).toString().compareTo(pq.get(j-1).tuple.getField(name).toString()) == 0)
						continue;
					else return false;
				}
				else//int type
				{
					int fieldValuei = Integer.parseInt(pq.get(i-1).tuple.getField(name).toString()); 
					//System.out.println("fieldValuei: " +fieldValuei);
					int fieldValuej = Integer.parseInt(pq.get(j-1).tuple.getField(name).toString());
					//System.out.println("fieldValuej: " +fieldValuej);
					if (fieldValuei < fieldValuej) return true;
					else if (fieldValuei == fieldValuej) continue;
					else return false;
				}
			
			}
			return false;
		}
	}
	private static void exch(ArrayList<Tuple> pq, int i, int j)
	{
		Tuple temp = pq.get(i-1);
		Tuple ex = pq.get(j-1);
		pq.set(i-1, ex);
		pq.set(j-1, temp);
	}
	
	private static void exchP(ArrayList<TuplePosition> pq, int i, int j)
	{
		TuplePosition temp = pq.get(i-1);
		TuplePosition ex = pq.get(j-1);
		pq.set(i-1, ex);
		pq.set(j-1, temp);
	}
	
public static void main(String[] args)
{
	TinyParser tp = new TinyParser();
	String str = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)";
	tp.parser(str);
	str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 67, 100, \"A\")";
    tp.parser(str);
    //str = "SELECT * FROM course";
    //tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (7, 5, 60, 78, \"B\")";
    tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (2, 0, 100, 0, \"A\")";
    tp.parser(str);
    str = "INSERT INTO course (sid, grade, exam, project, homework) VALUES (3, \"E\", 101, 32, 55)";
    tp.parser(str);
    str = "INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 66, 100, \"B\")";
    tp.parser(str);
    Relation relation = tp.schema_manager.getRelation("course");
    for (int i = 0; i < relation.getNumOfBlocks(); i++)
    	relation.getBlock(i, i);
    ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
    for (int i = 0; i < relation.getNumOfBlocks(); i++)
    	tupleList.add(tp.mem.getBlock(i).getTuple(0));
	ArrayList<TuplePosition> list = new ArrayList<TuplePosition>();
	String nulls = null;
	for (Tuple t : tupleList)
	{
		list.add(new TuplePosition(t, 5, 0, 9));
	}
	
	System.out.println(lessP(list, list.get(0).tuple.getSchema(), nulls, 2, 4));
	for (TuplePosition t : list)
		System.out.println(t.tuple.toString());
	
	Heap.sortP(list, relation.getSchema(), nulls);
	System.out.println("After sort:");
	for (TuplePosition t : list)
	{
		System.out.println(t.tuple.toString());
	}
	
}
}