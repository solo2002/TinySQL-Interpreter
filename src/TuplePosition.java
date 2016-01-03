import storageManager.Tuple;

//a class contain tuple and its position of blocks and offset in each block
public class TuplePosition {
	public Tuple tuple;
	public int memBlockPosition;
	public int diskBlockPosition;
	public int tupleInBlock;
	
	public TuplePosition (Tuple t, int mp, int off, int dp)
	{
		tuple = t;
		diskBlockPosition = dp;
		memBlockPosition = mp;
		tupleInBlock = off;
	}
	public TuplePosition (Tuple t, int mp, int off)
	{
		tuple = t;
		memBlockPosition = mp;
		tupleInBlock = off;
	}
}
