package edu.umd.cs424.database.index;

import java.nio.ByteBuffer;
import java.util.*;

import edu.umd.cs424.database.BaseTransaction;
import edu.umd.cs424.database.common.Buffer;
import edu.umd.cs424.database.common.Pair;
import edu.umd.cs424.database.databox.DataBox;
import edu.umd.cs424.database.databox.Type;
import edu.umd.cs424.database.io.Page;
import edu.umd.cs424.database.table.RecordId;

/**
 * A leaf of a B+ tree. Every leaf in a B+ tree of order d stores between d and
 * 2d (key, record id) pairs and a pointer to its right sibling (i.e. the page
 * number of its right sibling). Moreover, every leaf node is serialized and
 * persisted on a single page; see toBytes and fromBytes for details on how a
 * leaf is serialized. For example, here is an illustration of two order 2
 * leafs connected together:
 *
 *   leaf 1 (stored on some page)          leaf 2 (stored on some other page)
 *   +-------+-------+-------+-------+     +-------+-------+-------+-------+
 *   | k0:r0 | k1:r1 | k2:r2 |       | --> | k3:r3 | k4:r4 |       |       |
 *   +-------+-------+-------+-------+     +-------+-------+-------+-------+
 */
class LeafNode extends BPlusNode {
    // Metadatta about the B+ tree hat this node belongs to.
    private final BPlusTreeMetadata metadata;

    // The page on which this leaf is serialized.
    private final Page page;

    // The keys and record ids of this leaf. `keys` is always sorted in ascending
    // order. The record id at index i corresponds to the key at index i. For
    // example, the keys [a, b, c] and the rids [1, 2, 3] represent the pairing
    // [a:1, b:2, c:3].
    //
    // Note the following subtlety. keys and rids are in-memory caches of the
    // keys and record ids stored on disk. Thus, consider what happens when you
    // create two LeafNode objects that point to the same page:
    //
    //   BPlusTreeMetadata meta = ...;
    //   int pageNum = ...;
    //   Page page = allocator.fetchPage(pageNum);
    //   ByteBuffer buf = page.getByteBuffer();
    //
    //   LeafNode leaf0 = LeafNode.fromBytes(buf, meta, pageNum);
    //   LeafNode leaf1 = LeafNode.fromBytes(buf, meta, pageNum);
    //
    // This scenario looks like this:
    //
    //   HEAP                        | DISK
    //   ===============================================================
    //   leaf0                       | page 42
    //   +-------------------------+ | +-------+-------+-------+-------+
    //   | keys = [k0, k1, k2]     | | | k0:r0 | k1:r1 | k2:r2 |       |
    //   | rids = [r0, r1, r2]     | | +-------+-------+-------+-------+
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //   leaf1                       |
    //   +-------------------------+ |
    //   | keys = [k0, k1, k2]     | |
    //   | rids = [r0, r1, r2]     | |
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //
    // Now imagine we perform on operation on leaf0 like leaf0.put(k3, r3). The
    // in-memory values of leaf0 will be updated and they will be synced to disk.
    // But, the in-memory values of leaf1 will not be updated. That will look
    // like this:
    //
    //   HEAP                        | DISK
    //   ===============================================================
    //   leaf0                       | page 42
    //   +-------------------------+ | +-------+-------+-------+-------+
    //   | keys = [k0, k1, k2, k3] | | | k0:r0 | k1:r1 | k2:r2 | k3:r3 |
    //   | rids = [r0, r1, r2, r3] | | +-------+-------+-------+-------+
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //   leaf1                       |
    //   +-------------------------+ |
    //   | keys = [k0, k1, k2]     | |
    //   | rids = [r0, r1, r2]     | |
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //
    // Make sure your code (or your tests) doesn't use stale in-memory cached
    // values of keys and rids.
    private List<DataBox> keys;
    private List<RecordId> rids;

    // If this leaf is the rightmost leaf, then rightSibling is Optional.empty().
    // Otherwise, rightSibling is Optional.of(n) where n is the page number of
    // this leaf's right sibling.
    private Optional<Integer> rightSibling;

    // Constructors //////////////////////////////////////////////////////////////
    /**
     * Construct a brand new leaf node. The leaf will be persisted on a brand new
     * page allocated by metadata.getAllocator().
     */
    public LeafNode(BPlusTreeMetadata metadata, List<DataBox> keys,
                    List<RecordId> rids, Optional<Integer> rightSibling, BaseTransaction transaction) {
        this(metadata, metadata.getAllocator().allocPage(transaction), keys, rids,
             rightSibling, transaction);
    }

    /**
     * Construct a leaf node that is persisted to page `pageNum` allocated by
     * metadata.getAllocator().
     */
    private LeafNode(BPlusTreeMetadata metadata, int pageNum, List<DataBox> keys,
                     List<RecordId> rids, Optional<Integer> rightSibling, BaseTransaction transaction) {
        assert(keys.size() == rids.size());

        this.metadata = metadata;
        this.page = metadata.getAllocator().fetchPage(transaction, pageNum);
        this.keys = keys;
        this.rids = rids;
        this.rightSibling = rightSibling;
        sync(transaction);
    }

    // Core API //////////////////////////////////////////////////////////////////
    // See BPlusNode.get.
    @Override
    public LeafNode get(BaseTransaction transaction, DataBox key) {

        return this;

    }

    // See BPlusNode.getLeftmostLeaf.
    @Override
    public LeafNode getLeftmostLeaf(BaseTransaction transaction) {

        return this;

    }

    // See BPlusNode.put.
    @Override
    public Optional<Pair<DataBox, Integer>> put(BaseTransaction transaction, DataBox key, RecordId rid)
    throws BPlusTreeException {
        /**
         * node.put(k, r) inserts the pair (k, r) into the subtree rooted by this node. There
         * are two cases to consider:
         *
         *   Case 1: If inserting the pair (k, r) does NOT cause the node to overflow, then
         *           Optional.empty() is returned.
         *   Case 2: If inserting the pair (k, r) does cause the node to overflow,
         *           then the node is split into a left and right node (described more
         *           below) and a pair (split_key, right_node_page_num) is returned
         *           where right_node_page_num is the page number of the newly
         *           created right node, and the value of split_key depends on
         *           whether the node is an inner node or a leaf node (described more below).
         *
         * Now we explain how to split nodes and which split keys to return. Let's
         * take a look at an example. Consider inserting the key 4 into the example
         * tree above. No nodes overflow (i.e. we always hit case 1). The tree then
         * looks like this:
         *
         *                               inner
         *                               +----+----+----+----+
         *                               | 10 | 20 |    |    |
         *                               +----+----+----+----+
         *                              /     |     \
         *                         ____/      |      \____
         *                        /           |           \
         *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
         *   |  1 |  2 |  3 |  4 |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
         *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
         *   leaf0                  leaf1                  leaf2
         *
         * Now let's insert key 5 into the tree. Now, leaf0 overflows and creates a
         * new right sibling leaf3. d entries remain in the left node; d + 1 entries
         * are moved to the right node. DO NOT REDISTRIBUTE ENTRIES ANY OTHER WAY. In
         * our example, leaf0 and leaf3 would look like this:
         *
         *   +----+----+----+----+  +----+----+----+----+
         *   |  1 |  2 |    |    |->|  3 |  4 |  5 |    |
         *   +----+----+----+----+  +----+----+----+----+
         *   leaf0                  leaf3
         *
         * When a leaf splits, it returns the first entry in the right node as the
         * split key. In this example, 3 is the split key. After leaf0 splits, inner
         * inserts the new key and child pointer into itself and hits case 0 (i.e. it
         * does not overflow). The tree looks like this:
         *
         *                          inner
         *                          +--+--+--+--+
         *                          | 3|10|20|  |
         *                          +--+--+--+--+
         *                         /   |  |   \
         *                 _______/    |  |    \_________
         *                /            |   \             \
         *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
         *   | 1| 2|  |  |->| 3| 4| 5| 6|->|11|12|13|  |->|21|22|23|  |
         *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
         *   leaf0          leaf3          leaf1          leaf2
         *
         * When an inner node splits, the first d entries are kept in the left node
         * and the last d entries are moved to the right node. The middle entry is
         * moved (not copied) up as the split key. For example, we would split the
         * following order 2 inner node
         *
         *   +---+---+---+---+
         *   | 1 | 2 | 3 | 4 | 5
         *   +---+---+---+---+
         *
         * into the following two inner nodes
         *
         *   +---+---+---+---+  +---+---+---+---+
         *   | 1 | 2 |   |   |  | 4 | 5 |   |   |
         *   +---+---+---+---+  +---+---+---+---+
         *
         * with a split key of 3.
         *
         * DO NOT redistribute entries in any other way besides what we have
         * described. For example, do not move entries between nodes to avoid
         * splitting.
         *
         * Our B+ trees do not support duplicate entries with the same key. If a
         * duplicate key is inserted, the tree is left unchanged and an exception is
         * raised.
         */

        // records index to insert into, push everything else to the right inclusive of current element at i
        int i = 0;
        while (i < keys.size() && key.getInt() > (keys.get(i)).getInt()){
            i++;
        }

        //check for duplicate key
        if (i < keys.size() && (keys.get(i)).getInt() == key.getInt()){
            throw new BPlusTreeException("Tried to insert duplicate");
        }else{
            // no duplicates

            //insert new key and rid
            keys.add(i, key);
            rids.add(i, rid);
            sync(transaction);

            // check if split is needed
            if (keys.size() > (2 * metadata.getOrder())){
                // split required

                // Create new right node and split accordingly
                DataBox splitKey = keys.get(metadata.getOrder());
                List rightKeys = keys.subList(metadata.getOrder(), keys.size());
                List rightRIDs = rids.subList(metadata.getOrder(), rids.size());
                this.keys = keys.subList(0, metadata.getOrder());
                this.rids = rids.subList(0, metadata.getOrder());

                // Reassign relevant pointers

                LeafNode newRight = new LeafNode(metadata, rightKeys, rightRIDs, this.rightSibling, transaction);
                Integer rightPageNum = (newRight.getPage()).getPageNum();
                this.rightSibling = Optional.of(rightPageNum);
                sync(transaction);

                return Optional.of(new Pair<>(splitKey, rightPageNum));
                // return pair of split key and right node page number

            }else{
                // no split needed
                return Optional.empty();
            }

        }
    }

    // See BPlusNode.bulkLoad.
    @Override
    public Optional<Pair<DataBox, Integer>> bulkLoad(BaseTransaction transaction,
            Iterator<Pair<DataBox, RecordId>> data,
            float fillFactor)
    throws BPlusTreeException {

        int fill = (int)(Math.ceil(2 * metadata.getOrder() * fillFactor));

        //population
        while (data.hasNext()){
            Pair<DataBox, RecordId> insert = data.next();
            keys.add(insert.getFirst());
            rids.add(insert.getSecond());

            if (keys.size() > fill){
                //split condition

                // New leaf component construction
                List<DataBox> rightKeys = keys.subList(fill, keys.size());
                List rightRID = rids.subList(fill, rids.size());
                //redistribute prev keys and ids
                keys = keys.subList(0, fill);
                rids = rids.subList(0, fill);
                // leaf construction and synch
                LeafNode rightSib = new LeafNode(metadata, rightKeys, rightRID, this.rightSibling, transaction);
                int rpn = (rightSib.getPage()).getPageNum();
                this.rightSibling = Optional.of(rpn);
                sync(transaction);

                return Optional.of(new Pair<DataBox, Integer>(rightKeys.get(0), rpn));


            }
        }

        sync(transaction);
        return Optional.empty();

    }

    // See BPlusNode.remove.
    @Override
    public void remove(BaseTransaction transaction, DataBox key) {

        //locate existing key, remove it and corresponding rid
        if (keys.contains(key)) {
            int i = keys.indexOf(key);
            keys.remove(key);
            rids.remove(i);
            sync(transaction);
        }
    }

    // Iterators /////////////////////////////////////////////////////////////////
    /** Return the record id associated with `key`. */
    public Optional<RecordId> getKey(DataBox key) {
        int index = keys.indexOf(key);
        return index == -1 ? Optional.empty() : Optional.of(rids.get(index));
    }

    /**
     * Returns an iterator over all the keys present in this node
     */
    public Iterator<DataBox> scanKeys() {
        return keys.iterator();
    }

    /**
     * Returns an iterator over the record ids of this leaf in ascending order of
     * their corresponding keys.
     */
    public Iterator<RecordId> scanAll() {
        return rids.iterator();
    }

    // Helpers ///////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    /** Returns the right sibling of this leaf, if it has one. */
    public Optional<LeafNode> getRightSibling(BaseTransaction transaction) {
        return rightSibling.flatMap(pageNum -> Optional.of(LeafNode.fromBytes(transaction, metadata, pageNum)));
    }

    /**
     * Returns the largest number d such that the serialization of a LeafNode
     * with 2d entries will fit on a single page of size `pageSizeInBytes`.
     */
    public static int maxOrder(int pageSizeInBytes, Type keySchema) {
        // A leaf node with k entries takes up the following number of bytes:
        //
        //   1 + 4 + 4 + k * (keySize + ridSize)
        //
        // where
        //
        //   - 1 is the number of bytes used to store isLeaf,
        //   - 4 is the number of bytes used to store a sibling pointer,
        //   - 4 is the number of bytes used to store k,
        //   - keySize is the number of bytes used to store a DataBox of type
        //     keySchema, and
        //   - ridSize is the number of bytes of a RecordId.
        //
        // Solving the following equation
        //
        //   k * (keySize + ridSize) + 9 <= pageSizeInBytes
        //
        // we get
        //
        //   k = (pageSizeInBytes - 9) / (keySize + ridSize)
        //
        // The order d is half of k.
        int keySize = keySchema.getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int k = (pageSizeInBytes - 9) / (keySize + ridSize);
        return k / 2;
    }

    // For testing only.
    List<DataBox> getKeys() {
        return keys;
    }

    // For testing only.
    List<RecordId> getRids() {
        return rids;
    }

    // Pretty Printing ///////////////////////////////////////////////////////////
    @Override
    public String toString() {
        return String.format("LeafNode(pageNum=%s, keys=%s, rids=%s)",
                             page.getPageNum(), keys, rids);
    }

    @Override
    public String toSexp(BaseTransaction transaction) {
        var ss = new ArrayList<String>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i).toString();
            String rid = rids.get(i).toSexp();
            ss.add(String.format("(%s %s)", key, rid));
        }
        return String.format("(%s)", String.join(" ", ss));
    }

    /**
     * Given a leaf with page number 1 and three (key, rid) pairs (0, (0, 0)),
     * (1, (1, 1)), and (2, (2, 2)), the corresponding dot fragment is:
     * <p>
     *   node1[label = "{0: (0 0)|1: (1 1)|2: (2 2)}"];
     */
    @Override
    public String toDot(BaseTransaction transaction) {
        var ss = new ArrayList<String>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("%s: %s", keys.get(i), rids.get(i).toSexp()));
        }
        int pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        return String.format("  node%d[label = \"{%s}\"];", pageNum, s);
    }

    // Serialization /////////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // When we serialize a leaf node, we write:
        //
        //   a. the literal value 1 (1 byte) which indicates that this node is a
        //      leaf node,
        //   b. the value 1 or 0 (1 byte) which indicates whether this node has a right sibling
        //   c. the page id (4 bytes) of our right sibling, this field is ignored if the
        //      right sibling indicator is 0.
        //   d. the number (4 bytes) of keys (K) this leaf node contains (key, rid) pairs this leaf node contains.
        //   e. the K keys of this node
        //   f. the K rids of this node
        //
        // For example, the following bytes:
        //
        //   +----+----+-------------+-------------+----+----+-------------------+-------------------+
        //   | 01 | 01 | 00 00 00 04 | 00 00 00 02 | 05 | 07 | 00 00 00 03 00 01 | 00 00 00 04 00 06 |
        //   +----+----+-------------+-------------+----+----+-------------------+-------------------+
        //    \__/ \__/ \___________/ \___________/ \_______/ \_____________________________________/
        //     a     b         c            d           e                        f
        //
        // represent a leaf node having a sibling on page 4, two keys [5, 7] and two corresponding records [(3, 1), (4, 6)]

        // All sizes are in bytes.
        int isLeafSize = 1;
        int hasSiblingSize = 1;
        int siblingSize = Integer.BYTES;
        int lenSize = Integer.BYTES;
        int keySize = metadata.getKeySchema().getSizeInBytes() * keys.size();
        int ridSize = RecordId.getSizeInBytes() * keys.size();
        int size = isLeafSize + hasSiblingSize + siblingSize + lenSize + keySize + ridSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 1);
        buf.put((byte) (rightSibling.isPresent() ? 1 : 0));
        buf.putInt(rightSibling.orElse(0));
        buf.putInt(keys.size());
        for (DataBox key : keys) {
            buf.put(key.toBytes());
        }
        for (int i = 0; i < keys.size(); ++i) {
            buf.put(rids.get(i).toBytes());
        }
        return buf.array();
    }
    public static LeafNode fromBytes(BaseTransaction transaction, BPlusTreeMetadata metadata, int pageNum) {
        // Fetch the page and its ByteBuffer
        Page page = metadata.getAllocator().fetchPage(transaction, pageNum);
        Buffer buf = page.getBuffer(transaction);
        //read first byte, 1 = leaf, 0 = not leaf
        assert(buf.get() == (byte) 1);
        //read second byte
        boolean hasRightSibling = buf.get() == (byte) 1;
        //read next 4 bytes for the right sibling page number
        int rSibPageNum = buf.getInt();
        Optional<Integer> rightSibling = Optional.empty();
        if(hasRightSibling) {
            rightSibling = Optional.of(rSibPageNum);
        }

        //read next 4 bytes for number of keys
        int numKeys = buf.getInt();
        //read the keys based on number of keys
        List<DataBox> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; ++i) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
        }
        //now read the number of records corresponding to the keys
        List<RecordId> rids = new ArrayList<>();
        for (int i = 0; i < numKeys; ++i) {
            rids.add(RecordId.fromBytes(buf));
        }
        //create a new leafnode from the bytes
        return new LeafNode(metadata, pageNum, keys, rids, rightSibling, transaction);
    }

    /**
     * LeafNode.fromBytes(m, p) loads a LeafNode from page p of
     * meta.getAllocator().
     */
    public static LeafNode ofromBytes(BaseTransaction transaction, BPlusTreeMetadata metadata,
                                     int pageNum) {
        // When we serialize a leaf node, we write:
        //
        //   a. the literal value 1 (1 byte) which indicates that this node is a
        //      leaf node,
        //   b. the value 1 or 0 (1 byte) which indicates whether this node has a right sibling
        //   c. the page id (4 bytes) of our right sibling, this field is ignored if the
        //      right sibling indicator is 0.
        //   d. the number (4 bytes) of keys (K) this leaf node contains (key, rid) pairs this leaf node contains.
        //   e. the K keys of this node
        //   f. the K rids of this node
        //
        // For example, the following bytes:
        //
        //   +----+----+-------------+-------------+----+----+-------------------+-------------------+
        //   | 01 | 01 | 00 00 00 04 | 00 00 00 02 | 05 | 07 | 00 00 00 03 00 01 | 00 00 00 04 00 06 |
        //   +----+----+-------------+-------------+----+----+-------------------+-------------------+
        //    \__/ \__/ \___________/ \___________/ \_______/ \_____________________________________/
        //     a     b         c            d           e                        f
        //
        // represent a leaf node having a sibling on page 4, two keys [5, 7] and two corresponding records [(3, 1), (4, 6)]

        //Write function to read leaf node from page

        Page page = metadata.getAllocator().fetchPage(transaction, pageNum);
        Buffer buf = page.getBuffer(transaction);

        // Assert the node is a leaf
        assert(buf.get() == (byte) 1);

        // Has right sibling
        int hasSib = buf.get();
        int rpid = buf.getInt();
        Optional<Integer> rsib = Optional.empty();
        if(hasSib == (byte) 1 ) {
            rsib = Optional.of(rpid);
        }

        int keyQuant = buf.getInt();
        var keys = new ArrayList<DataBox>();
        var rids = new ArrayList<RecordId>();

        for (int i = 0 ; i < keyQuant ; i++){
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
        }
        for (int i = 0 ; i < keyQuant ; i++){
            rids.add(RecordId.fromBytes(buf));
        }

        if (hasSib == (byte) 0) {
            return new LeafNode(metadata, pageNum, keys, rids, Optional.of(0), transaction);
        }
        else{
            return new LeafNode(metadata, pageNum, keys, rids, Optional.of(rpid), transaction);
        }

    }

    // Builtins //////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LeafNode node)) {
            return false;
        }
        return page.getPageNum() == node.page.getPageNum() &&
               keys.equals(node.keys) &&
               rids.equals(node.rids) &&
               rightSibling.equals(node.rightSibling);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, rids, rightSibling);
    }
}
