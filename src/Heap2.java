

import java.util.ArrayList;

import storageManager.FieldType;

public class Heap2 {
	private ArrayList<TuplePosition> tupleList;
	private String[] fieldNameArray;
	
	
	public Heap2(String[] nameArray)
	{
		tupleList = new ArrayList<TuplePosition>();
		fieldNameArray = nameArray;
	}
	
	
	public int length()
	{
		return tupleList.size();
	}
	
	public void insert(TuplePosition tuple)
	{
		if(tupleList.size()==0)
		{
			tupleList.add(tuple);
			tupleList.add(tuple);
		}
		else
		{
			tupleList.add(tuple);
			this.heapify(tupleList.size()-1);
		}
	}
	
	public int compareTo(TuplePosition t1, TuplePosition t2)
	{
		for(int i=0; i<fieldNameArray.length; i++)
		{
			if(t1.tuple.getField(fieldNameArray[i]).type == FieldType.STR20)
			{			
				if(t1.tuple.getField(fieldNameArray[i]).toString().compareTo
						(t2.tuple.getField(fieldNameArray[i]).toString())<0)
					return -1;
				else if(t1.tuple.getField(fieldNameArray[i]).toString().compareTo
						(t2.tuple.getField(fieldNameArray[i]).toString())>0)
					return 1;
			}
			else
			{
				if(t1.tuple.getField(fieldNameArray[i]).integer < 
						t2.tuple.getField(fieldNameArray[i]).integer)
					return -1;
				else if(t1.tuple.getField(fieldNameArray[i]).integer > 
					t2.tuple.getField(fieldNameArray[i]).integer)
					return 1;
			}
		}
		return 0;
	}
	
	private void heapify(int index)
	{
		if(index > 1 && compareTo(tupleList.get(index),tupleList.get(index/2)) < 0)
		{
			TuplePosition t = tupleList.get(index);
			tupleList.set(index, tupleList.get(index/2));
			tupleList.set(index/2, t);
			this.heapify(index/2);			
		}
		else if(index*2+1 <= tupleList.size()-1)
		{
			if(compareTo(tupleList.get(index*2),tupleList.get(index*2+1)) < 0)
			{
				if(compareTo(tupleList.get(index*2),tupleList.get(index)) < 0)
				{
					TuplePosition t = tupleList.get(index);
					tupleList.set(index, tupleList.get(index*2));
					tupleList.set(index*2, t);
					this.heapify(index*2);
				}
				else return;
			}
			else
			{
				if(compareTo(tupleList.get(index*2+1),tupleList.get(index)) < 0)
				{
					TuplePosition t = tupleList.get(index);
					tupleList.set(index, tupleList.get(index*2+1));
					tupleList.set(index*2+1, t);
					this.heapify(index*2+1);
				}
				else return;
			}
		}
		
		else if(index*2 == tupleList.size() - 1 && compareTo(tupleList.get(index*2),tupleList.get(index)) < 0)
		{
			TuplePosition t = tupleList.get(index);
			tupleList.set(index, tupleList.get(index*2));
			tupleList.set(index*2, t);
			this.heapify(index*2);
		}
		else return;
	}
	
	public TuplePosition minTuple()
	{
		return tupleList.get(1);
	}
	
	public void delete(int index)
	{
		if(index == tupleList.size() - 1)
			tupleList.remove(index);
		else
		{
			tupleList.set(index,tupleList.get(tupleList.size() - 1));
			tupleList.remove(tupleList.size()-1);
			this.heapify(index);
		}
	}
}
