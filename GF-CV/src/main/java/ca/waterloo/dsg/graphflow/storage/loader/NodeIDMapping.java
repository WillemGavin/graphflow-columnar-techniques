package ca.waterloo.dsg.graphflow.storage.loader;

import lombok.Getter;

public class NodeIDMapping {

    @Getter private int[][] types;
    @Getter private long[][] offsets;

    NodeIDMapping(long numNodes) {
        offsets = create2DLongArray(numNodes + 1);
        types = create2DIntegerArray(numNodes + 1);
    }

    public void setNode(long nodeID, int type, long offset) {
        types[(int) nodeID / Integer.MAX_VALUE][(int) nodeID % Integer.MAX_VALUE]
            = type;
        offsets[(int) nodeID / Integer.MAX_VALUE][(int) nodeID % Integer.MAX_VALUE]
            = offset;
    }

    public int getNodeType(long nodeID) {
        return types[(int) nodeID / Integer.MAX_VALUE][(int) nodeID % Integer.MAX_VALUE];
    }

    public long getNodeOffset(long nodeID) {
        return offsets[(int) nodeID / Integer.MAX_VALUE][(int) nodeID % Integer.MAX_VALUE];
    }

    /**
     * 建立二维Long型矩阵
     * @param capacity
     * @return
     */
    public long[][] create2DLongArray(long capacity) {
        // 计算桶的数量
        int buckets = (int) (capacity / Integer.MAX_VALUE) + 1;
        var array = new long[buckets][];
        for (var i = 0;i < buckets - 1;i++) {
            array[i] = new long[Integer.MAX_VALUE];
        }
        // 设置最后一个桶的大小
        array[buckets - 1] = new long[(int) capacity % Integer.MAX_VALUE];
        return array;
    }

    /**
     * 建立二维Integer矩阵
     * @param capacity
     * @return
     */
    public int[][] create2DIntegerArray(long capacity) {
        int buckets = (int) (capacity / Integer.MAX_VALUE) + 1;
        var array = new int[buckets][];
        for (var i = 0;i < buckets - 1;i++) {
            array[i] = new int[Integer.MAX_VALUE];
        }
        array[buckets - 1] = new int[(int) capacity % Integer.MAX_VALUE];
        return array;
    }
}
