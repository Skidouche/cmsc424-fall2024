package edu.umd.cs424.database.index;

import java.io.Closeable;
import java.io.IOException;
import java.io.FileWriter;
import java.io.File;
import java.util.*;
import java.util.function.Function;

import edu.umd.cs424.database.BaseTransaction;
import edu.umd.cs424.database.common.Buffer;
import edu.umd.cs424.database.common.Pair;
import edu.umd.cs424.database.concurrency.LockContext;
import edu.umd.cs424.database.databox.DataBox;
import edu.umd.cs424.database.databox.IntDataBox;
import edu.umd.cs424.database.databox.Type;
import edu.umd.cs424.database.io.Page;
import edu.umd.cs424.database.io.PageAllocator;
import edu.umd.cs424.database.table.RecordId;

import javax.xml.crypto.Data;

/**
 * A persistent B+ tree.
 *
 * // Create an order 2, integer-valued B+ tree that is persisted in tree.txt.
 * var tree = new BPlusTree("tree.txt", Type.intType(), 2, transaction);
 *
 * // Insert some values into the tree.
 * tree.put(new IntDataBox(0), new RecordId(0, (short) 0));
 * tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
 * tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
 *
 * // Get some values out of the tree.
 * tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 * tree.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 * tree.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 * tree.get(new IntDataBox(3)); // Optional.empty();
 *
 * // Iterate over the record ids in the tree.
 * tree.scanEqual(new IntDataBox(2)); // [(2, 2)]
 * tree.scanAll(); // [(0, 0), (1, 1), (2, 2)]
 *
 * // Remove some elements from the tree.
 * tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 * tree.remove(new IntDataBox(0));
 * tree.get(new IntDataBox(0)); // Optional.empty()
 *
 * // Load the tree from disk.
 * var fromDisk = new BPlusTree("tree.txt");
 *
 * // All the values are still there.
 * fromDisk.get(new IntDataBox(0)); // Optional.empty()
 * fromDisk.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 * fromDisk.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 */
public class BPlusTree implements Closeable {
	public static final String FILENAME_PREFIX = "db";
	public static final String FILENAME_EXTENSION = ".index";

	private final PageAllocator allocator;
	private final BPlusTreeMetadata metadata;
	private final Page headerPage;
	private final LockContext lockContext;
	private BPlusNode root;

	// Constructors ////////////////////////////////////////////////////////////
	/**
	 * Construct a new B+ tree which is serialized into the file `filename`, stores
	 * keys of type `keySchema`, and has order `order`. For example, `new
	 * BPlusTree("tree.txt", Type.intType(), 2)` constructs a B+ tree that is
	 * serialized to "tree.txt", that maps integers to record ids, and that has
	 * order 2.
	 *
	 * If the specified order is so large that a single node cannot fit on a single
	 * page, then a BPlusTree exception is thrown. If you want to have maximally
	 * full B+ tree nodes, then use the BPlusTree.maxOrder function to get the
	 * appropriate order.
	 *
	 * We reserve the first page (i.e. page number 0) of the file for a header page
	 * which contains:
	 *
	 * - the key schema of the tree, - the order of the tree, and - the page number
	 * of the root of the tree.
	 *
	 * All other pages are serializations of inner and leaf nodes. See writeHeader
	 * for details.
	 */
	public BPlusTree(String filename, Type keySchema, int order, LockContext lockContext, BaseTransaction transaction)
			throws BPlusTreeException {
		// Sanity checks.
		if (order < 0) {
			String msg = String.format("You cannot construct a B+ tree with negative order %d.", order);
			throw new BPlusTreeException(msg);
		}

		int maxOrder = BPlusTree.maxOrder(Page.pageSize, keySchema);
		if (order > maxOrder) {
			String msg = String.format(
					"You cannot construct a B+ tree with order %d greater than the " + "max order %d.", order,
					maxOrder);
			throw new BPlusTreeException(msg);
		}

		this.lockContext = lockContext;

		// Initialize the page allocator.
		this.allocator = new PageAllocator(this.lockContext, filename, true, transaction);
		this.metadata = new BPlusTreeMetadata(allocator, keySchema, order);

		// Allocate the header page.
		int headerPageNum = allocator.allocPage(transaction);
		assert (headerPageNum == 0);
		this.headerPage = allocator.fetchPage(transaction, headerPageNum);

		// Construct the root.
		var keys = new ArrayList<DataBox>();
		var rids = new ArrayList<RecordId>();
		Optional<Integer> rightSibling = Optional.empty();
		this.root = new LeafNode(this.metadata, keys, rids, rightSibling, transaction);

		// Initialize the header page.
		writeHeader(transaction, headerPage);
	}

	/** Read a B+ tree that was previously serialized to filename. */
	public BPlusTree(String filename, LockContext lockContext, BaseTransaction transaction) {
		this.lockContext = lockContext;

		// Initialize the page allocator and fetch the header page.
		this.allocator = new PageAllocator(this.lockContext, filename, false, transaction);
		Page headerPage = allocator.fetchPage(transaction, 0);
		Buffer buf = headerPage.getBuffer(transaction);

		// Read the contents of the header page. See writeHeader for information
		// on exactly what is written to the header page.
		Type keySchema = Type.fromBytes(buf);
		int order = buf.getInt();
		int rootPageNum = buf.getInt();

		// Initialize members.
		this.metadata = new BPlusTreeMetadata(allocator, keySchema, order);
		this.headerPage = allocator.fetchPage(transaction, 0);
		this.root = BPlusNode.fromBytes(transaction, this.metadata, rootPageNum);
	}

	public void close() {
		this.allocator.close();
	}

	// Core API ////////////////////////////////////////////////////////////////
	/**
	 * Returns the value associated with `key`.
	 *
	 * // Create a B+ tree and insert a single value into it.
	 * var tree = new BPlusTree("t.txt", Type.intType(), 4);
	 * DataBox key = new IntDataBox(42);
	 * var rid = new RecordId(0, (short) 0);
	 * tree.put(key, rid);
	 *
	 * // Get the value we put and also try to get a value we never put.
	 * tree.get(key); // Optional.of(rid)
	 * tree.get(new IntDataBox(100)); // Optional.empty()
	 */
	public Optional<RecordId> get(BaseTransaction transaction, DataBox key) {

		//Get desired result
		LeafNode target = root.get(transaction, key);
		if ((target.getKeys()).contains(key)){

			int i = (target.getKeys()).indexOf(key);
			return Optional.of(target.getRids().get(i));

		}else{
			return Optional.empty();
		}
	}

	/**
	 * scanEqual(k) is equivalent to get(k) except that it returns an iterator
	 * instead of an Optional. That is, if get(k) returns Optional.empty(), then
	 * scanEqual(k) returns an empty iterator. If get(k) returns Optional.of(rid)
	 * for some rid, then scanEqual(k) returns an iterator over rid.
	 */
	public Iterator<RecordId> scanEqual(BaseTransaction transaction, DataBox key) {
		typecheck(key);
		Optional<RecordId> rid = get(transaction, key);
		if (rid.isPresent()) {
			var l = new ArrayList<RecordId>();
			l.add(rid.get());
			return l.iterator();
		} else {
			return new ArrayList<RecordId>().iterator();
		}
	}

	/**
	 * Returns an iterator over all the RecordIds stored in the B+ tree in ascending
	 * order of their corresponding keys.
	 *
	 * // Create a B+ tree and insert some values into it.
	 * var tree = new BPlusTree("t.txt", Type.intType(), 4);
	 * tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
	 * tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
	 * tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
	 * tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
	 * tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
	 *
	 * Iterator<RecordId> iter = tree.scanAll();
	 * iter.next(); // RecordId(1, 1)
	 * iter.next(); // RecordId(2, 2)
	 * iter.next(); // RecordId(3, 3)
	 * iter.next(); // RecordId(4, 4)
	 * iter.next(); // RecordId(5, 5)
	 * iter.next(); // NoSuchElementException
	 *
	 * Note that you CAN NOT materialize all record ids in memory and then return an
	 * iterator over them. Your iterator must lazily scan over the leaves of the B+
	 * tree. Solutions that materialize all record ids in memory will receive 0
	 * points.
	 */
	public Iterator<RecordId> scanAll(BaseTransaction transaction) {
		return new BPlusTreeIterator(transaction);
	}

	/**
	 * Returns an iterator over all the RecordIds stored in the B+ tree that are
	 * greater than or equal to `key`. RecordIds are returned in ascending of their
	 * corresponding keys.
	 *
	 * // Create a B+ tree and insert some values into it.
	 * var tree = new BPlusTree("t.txt", Type.intType(), 4);
	 * tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
	 * tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
	 * tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
	 * tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
	 * tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
	 *
	 * Iterator<RecordId> iter = tree.scanGreaterEqual(new IntDataBox(3));
	 * iter.next(); // RecordId(3, 3)
	 * iter.next(); // RecordId(4, 4)
	 * iter.next(); // RecordId(5, 5)
	 * iter.next(); // NoSuchElementException
	 *
	 * Note that you CAN NOT materialize all record ids in memory and then return an
	 * iterator over them. Your iterator must lazily scan over the leaves of the B+
	 * tree. Solutions that materialize all record ids in memory will receive 0
	 * points.
	 */
	public Iterator<RecordId> scanGreaterEqual(BaseTransaction transaction, DataBox key) {
		typecheck(key);
		return new BPlusTreeIterator(transaction, key, new IntDataBox(Integer.MAX_VALUE));
	}

	public Iterator<RecordId> scanEnhanced(BaseTransaction transaction, DataBox lowerBound, DataBox upperBound) {
		typecheck(lowerBound);
		typecheck(upperBound);
		return new BPlusTreeIterator(transaction, lowerBound, upperBound);
	}

	public Iterator<RecordId> scanEnhanced(BaseTransaction transaction, DataBox lowerBound, DataBox upperBound,
										   int limit, Function<DataBox, Boolean> filter) {
		typecheck(lowerBound);
		typecheck(upperBound);
		return new BPlusTreeIterator(transaction, lowerBound, upperBound, limit, filter);
	}

	/**
	 * Inserts a (key, rid) pair into a B+ tree. If the key already exists in the B+
	 * tree, then the pair is not inserted and an exception is raised.
	 *
	 * var tree = new BPlusTree("t.txt", Type.intType(), 4);
	 * DataBox key = new IntDataBox(42);
	 * var rid = new RecordId(42, (short) 42);
	 * tree.put(key, rid); // Sucess :)
	 * tree.put(key, rid); // BPlusTreeException :(
	 */
	public void put(BaseTransaction transaction, DataBox key, RecordId rid) throws BPlusTreeException {

		//duplicate detection
		if(this.get(transaction, key).isPresent()){
			throw new BPlusTreeException("Duplicate key insertion");
		}

		Optional<Pair<DataBox, Integer>> rootSplit = root.put(transaction, key, rid);

		//On split, need to write new root
		if(rootSplit.isPresent()){

			// Key construction
			List newKeys = new ArrayList<DataBox>();
			newKeys.add((rootSplit.get()).getFirst());

			// children construction
			List newKids = new ArrayList<Integer>();
			newKids.add((root.getPage()).getPageNum());
			newKids.add(((rootSplit.get()).getSecond()));

			// Node construction
			root = new InnerNode(metadata, newKeys, newKids, transaction);

		}

		root.sync(transaction);

	}

	/**
	 * Bulk loads data into the B+ tree. Tree should be empty and the data iterator
	 * should be in sorted order (by the DataBox key field) and contain no
	 * duplicates (no error checking is done for this).
	 *
	 * fillFactor specifies the fill factor for leaves only; inner nodes should be
	 * filled up to full and split in half exactly like in put.
	 *
	 * This method should raise an exception if the tree is not empty at time of
	 * bulk loading. If data does not meet the preconditions (contains duplicates or
	 * not in order), the resulting behavior is undefined.
	 *
	 * The behavior of this method should be similar to that of InnerNode's bulkLoad
	 * (see comments in BPlusNode.bulkLoad).
	 */
	public void bulkLoad(BaseTransaction transaction, Iterator<Pair<DataBox, RecordId>> data, float fillFactor)
			throws BPlusTreeException {

		// Test for empty
		if (root instanceof LeafNode && ((LeafNode) root).getKeys().size() != 0){
			throw new BPlusTreeException("Tree Not Empty");
		}

		// Loop through data to load
		while(data.hasNext()){

			//recursive bulkLoad
			Optional<Pair<DataBox, Integer>> rootSplit = root.bulkLoad(transaction, data, fillFactor);

			// Check for split
			if (rootSplit.isPresent()){

				// Key construction
				List newKeys = new ArrayList<DataBox>();
				newKeys.add((rootSplit.get()).getFirst());

				// children construction
				List newKids = new ArrayList<Integer>();
				newKids.add((root.getPage()).getPageNum());
				newKids.add(((rootSplit.get()).getSecond()));

				// Node construction
				root = new InnerNode(metadata, newKeys, newKids, transaction);

			}

			root.sync(transaction);

		}

	}

	/**
	 * Deletes a (key, rid) pair from a B+ tree.
	 *
	 * var tree = new BPlusTree("t.txt", Type.intType(), 4);
	 * DataBox key = new IntDataBox(42);
	 * var rid = new RecordId(42, (short) 42);
	 *
	 * tree.put(key, rid);
	 * tree.get(key); // Optional.of(rid)
	 * tree.remove(key);
	 * tree.get(key); // Optional.empty()
	 */
	public void remove(BaseTransaction transaction, DataBox key) {
		root.remove(transaction, key);
	}

	// Helpers /////////////////////////////////////////////////////////////////
	/**
	 * Returns a sexp representation of this tree. See BPlusNode.toSexp for more
	 * information.
	 */
	public String toSexp(BaseTransaction transaction) {
		return root.toSexp(transaction);
	}

	/**
	 * Debugging large B+ trees is hard. To make it a bit easier, we can print out a
	 * B+ tree as a DOT file which we can then convert into a nice picture of the B+
	 * tree. tree.toDot() returns the contents of DOT file which illustrates the B+
	 * tree. The details of the file itself is not at all important, just know that
	 * if you call tree.toDot() and save the output to a file called tree.dot, then
	 * you can run this command
	 *
	 * dot -T pdf tree.dot -o tree.pdf
	 *
	 * to create a PDF of the tree.
	 */
	public String toDot(BaseTransaction transaction) {
		var strings = new ArrayList<String>();
		strings.add("digraph g {");
		strings.add("  node [shape=record, height=0.1];");
		strings.add(root.toDot(transaction));
		strings.add("}");
		return String.join("\n", strings);
	}

	/**
	 * This function is very similar to toDot() except that we write the dot
	 * representation of the B+ tree to a dot file and then convert that to a PDF
	 * that will be stored in the src directory. Pass in a string with the ".pdf"
	 * extension included at the end (ex "tree.pdf").
	 */
	public void toDotPDFFile(BaseTransaction transaction, String filename) {
		String tree_string = toDot(transaction);

		// Writing to intermediate dot file
		try {
			var file = new File("tree.dot");
			var fileWriter = new FileWriter(file);
			fileWriter.write(tree_string);
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Running command to convert dot file to PDF
		try {
			Runtime.getRuntime().exec("dot -T pdf tree.dot -o " + filename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Returns the largest number d such that the serialization of a LeafNode with
	 * 2d entries and an InnerNode with 2d keys will fit on a single page of size
	 * `pageSizeInBytes`.
	 */
	public static int maxOrder(int pageSizeInBytes, Type keySchema) {
		int leafOrder = LeafNode.maxOrder(pageSizeInBytes, keySchema);
		int innerOrder = InnerNode.maxOrder(pageSizeInBytes, keySchema);
		return Math.min(leafOrder, innerOrder);
	}

	/** Returns the number of pages used to serialize the tree. */
	public int getNumPages() {
		return metadata.getAllocator().getNumPages();
	}

	/** Serializes the header page to page. */
	private void writeHeader(BaseTransaction transaction, Page page) {
		byte[] keySchema = metadata.getKeySchema().toBytes();
		Buffer buf = page.getBuffer(transaction);
		buf.put(keySchema);
		buf.putInt(metadata.getOrder());
		buf.putInt(root.getPage().getPageNum());
	}

	private void typecheck(DataBox key) {
		Type t = metadata.getKeySchema();
		if (!key.type().equals(t)) {
			String msg = String.format("DataBox %s is not of type %s", key, t);
			throw new IllegalArgumentException(msg);
		}
	}

	// Iterator ////////////////////////////////////////////////////////////////
	private class BPlusTreeIterator implements Iterator<RecordId> {
		// these are instance variables used by the existing iterator implementation
		// you are welcome to add/change/remove instance variables in this class as you see fit
		private LeafNode curNode;
		private Iterator<RecordId> curIterator;
		private DataBox lowerBound;
		private DataBox upperBound;
		private Integer LIMIT;
		private Function<DataBox, Boolean> filter;
		private int counter;
		private int currInd;
		private RecordId curRec;
		boolean empty = false;



		/**
		 * This constructor simply creates an iterator that
		 * iterates through every record in the tree in ascending or of the keys.
		 * <p>
		 * The existing code is flawed, but serves as a good example on iterators are implemented.
		 * <p>
		 * For your implementation, you are recommended to use explicit constructor invocation so that
		 * you do not have to implement this method.
		 *
		 * @param transaction Ignore this parameter. It has nothing to do with project 3.
		 */
		public BPlusTreeIterator(BaseTransaction transaction) {
			this(transaction, null, null, Integer.MAX_VALUE, x -> true);
		}

		/**
		 * This constructor creates an iterator that iterates through every record in the tree
		 * whose key is greater than or equal to lowerBound and less than or equal to upperBound.
		 * <p>
		 * For example, let the following be the key/record pairs in the tree:
		 * (1, 555), (2, 1), (3, 2432), (5, 21), (7, 1298)
		 * If lowerBound.getInt() is 3 and upperBound.getInt() is 6, then the iterator should return
		 * 2432 and 21 in order.
		 * <p>
		 * For your implementation, you are recommended to use explicit constructor invocation so that
		 * you do not have to implement this method.
		 *
		 * @param transaction Ignore this parameter. It has nothing to do with project 3.
		 * @param lowerBound Lower bound (inclusive) on the keys whose record will be returned by the iterator.
		 * @param upperBound Upper bound (inclusive) on the keys whose record will be returned by the iterator.
		 */
		public BPlusTreeIterator(BaseTransaction transaction,
								 DataBox lowerBound, DataBox upperBound) {
			this(transaction, upperBound, lowerBound, Integer.MAX_VALUE, x -> true);
		}

		/**
		 * This constructor is similar with the previous one but has more criteria. The added limit parameter should
		 * be fairly easy to understand. For the filter parameter, suppose <code>box</code> is a <code>DataBox</code>
		 * object, then you can use <code>filter.apply(box)</code> to check if this key passes the filter. If it does,
		 * return it when <code>next</code> is called, otherwise ignore it.
		 * <p>
		 * Since this is the most powerful/flexible constructor among all 3, you are recommended to let
		 * other constructors call this constructor through explicit constructor invocation
		 * and put all your implementation here.
		 *
		 * @param transaction Ignore this parameter. It has nothing to do with project 3.
		 * @param lowerBound Lower bound (inclusive) on the keys whose record will be returned by the iterator.
		 * @param upperBound Upper bound (inclusive) on the keys whose record will be returned by the iterator.
		 * @param limit The maximum number of elements the iterator can return. For example, suppose limit is 5 but
		 *              there are 10 records available, then only the first 5 records should be returned
		 *              by the generator.
		 * @param filter A function which takes a DataBox as input and returns a Boolean. It determines, by performing
		 *               some check on the key, whether a record should be included in the result of the iterator.
		 */
		public BPlusTreeIterator(BaseTransaction transaction,
								 DataBox lowerBound, DataBox upperBound, int limit,
								 Function<DataBox, Boolean> filter) {
			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
			this.LIMIT = limit;
			this.filter = filter;
			counter = 0;
			currInd = 0;

			// bound processing
			if (lowerBound != null && upperBound != null){

				// start from key with lower bound value
				//this.curNode = root.get(transaction, lowerBound);
				//this.curIterator = curNode.scanAll();

				// Locate first index to iterate off of
				//while (currInd < (curNode.getKeys()).size() && (curNode.getKeys()).get(currInd).compareTo(lowerBound) > 0){
				//	currInd++;
				//	curRec = curIterator.next();
				//}
				this.curNode = root.get(transaction, lowerBound);
				this.curIterator = curNode.scanAll();
				currInd = (curNode.getKeys()).indexOf(lowerBound);

				if(currInd == -1){
					empty = true;
				} else {

					for(int i = 0; i < currInd; i++){
						curIterator.next();
					}
					curRec = (curNode.getRids()).get(currInd);
				}

			}else{
				// No bounds

				this.curNode = root.getLeftmostLeaf(transaction);
				this.curIterator = curNode.scanAll();

			}

		}

		/**
		 * Checks whether there are more elements the iterator can return.
		 *
		 * @return True if calling hasNext will yield the next element. False if there are no more elements available.
		 */

		//for every leaf node - check if current leaf is null
			//for every key
				//do something
			//current node = current node right sibling
			//curNode = curNode right sibling - read from page number

		@Override
		public boolean hasNext() {
			// Check if the limit has been hit
			// check if the next key is under upper bound
			// if so, check if the key passes the filter
			// if not, check the next keys until one of them passes the filter
			// When peeking into next, check if you break node index. If so, move to the next node and repeat.

			if (counter < LIMIT && !empty) {
				// Can still read more entries

				while (curNode != null){
					//while this is still a valid leaf node

					while(curIterator.hasNext()){
						//while this node still has keys

						DataBox key = (curNode.getKeys()).get(currInd);

						//upper bound check
						if ( upperBound != null && (!(key.compareTo(upperBound) <= 0) || !(key.compareTo(lowerBound) >= 0))){
							//System.out.println(lowerBound.getInt() + " " + key.getInt() + " " + upperBound.getInt() + " " + curNode.getRids().get(currInd) + " out of bounds");
							return false;
						}

						// check filter
						if (filter.apply(key)){
							if ( upperBound != null){
								//System.out.println(lowerBound.getInt() + " " + key.getInt() + " " + upperBound.getInt() + " " + curNode.getRids().get(currInd));
							}
							return true;
						}//else{
							//System.out.println(lowerBound.getInt() + " " + key.getInt() + " " + upperBound.getInt() + " " + curNode.getRids().get(currInd) + " failed key check");
						//}

						//move to next key and record
						curRec = curIterator.next();
						currInd++;

					}

					//reached end of leaf or leaves
					// Because there might be empty leaves we need to loop through until we find a
					// non-empty one
					while (curNode.getRightSibling(null).isPresent() && !curIterator.hasNext()) {
						curNode = curNode.getRightSibling(null).get();

						//Reset key and record index on new leaf
						currInd = 0;
						curIterator = curNode.scanAll();
					}

					if(!curIterator.hasNext()){
						return false;
					}


				}

				return false;

				/**
				if (!curIterator.hasNext()) {
					// Because there might be empty leaves we need to loop through until we find a
					// non-empty one
					while (curNode.getRightSibling(null).isPresent() && !curIterator.hasNext()) {
						curNode = curNode.getRightSibling(null).get();

						//Reset key and record index on new leaf
						currInd = 0;
						curIterator = curNode.scanAll();
					}

				}

				if (curIterator.hasNext()){
					boolean EOR = false;

					while (!EOR){

						DataBox key = (curNode.getKeys()).get(currInd);

						//upper bound check
						if ( upperBound != null && !(key.compareTo(upperBound) <= 0)){

							return false;
						}

						// check filter
						if (filter.apply(key)){
							return true;
						}else{

							// move on to the next entry to check validity
							if (curIterator.hasNext()){
								//next record id and key
								curRec = curIterator.next();
								currInd++;

							}else{

								//check fr
								while (curNode.getRightSibling(null).isPresent() && !curIterator.hasNext()) {
									curNode = curNode.getRightSibling(null).get();
									//Reset key and record index on new leaf
									currInd = 0;
									curIterator = curNode.scanAll();
								}

								if(!curIterator.hasNext()){
									EOR = true;
								}

							}

						}

					}
					return false;

				}else{

					//reached end of records
					return false;

				}
			**/
			}else {
				// limit has been hit
				return false;

			}

		}

		/**
		 * Look for the next available record in the BTree and return it. The returned record should have
		 * the lowest key among all possible records that can be returned. In other words, the iterator should
		 * go through the key/record pairs in the tree in ascending order of the key, matching the order in which
		 * the BPlusTree stores them.
		 *
		 * @return The next record.
		 * @throws NoSuchElementException When no more elements are available.
		 */
		@Override
		public RecordId next() {

			//reads current entry and tests validity- else moves along all entries until valid one is reached.
			if (this.hasNext()) {

				//moves to next key index
				currInd ++;
				//counts up one read record
				counter++;
				//reads current record and moves up to next record index
				curRec = curIterator.next();
				return curRec;
			}else{

				throw new NoSuchElementException("No valid next element");

			}
		}
	}
}
