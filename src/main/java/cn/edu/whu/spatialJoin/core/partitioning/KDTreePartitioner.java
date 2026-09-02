package cn.edu.whu.spatialJoin.core.partitioning;


import cn.edu.whu.spatialJoin.common.SpatialJoinConstants;
import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.common.Summary;
import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeature;
import cn.edu.whu.spatialJoin.enums.GeometryType;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper;
import cn.edu.whu.spatialJoin.synopses.AbstractHistogram;
import cn.edu.whu.spatialJoin.utils.IntArray;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * A partitioner that partitions the space using a KD-tree structure.
 *
 * <p>The historical implementation in this project builds a fixed-count complete binary tree.
 * The class now also supports two adaptive refinement modes:
 * <ul>
 *   <li>{@code adaptive-load}: keep splitting while one node's estimated memory load exceeds a target byte threshold</li>
 *   <li>{@code adaptive-count}: keep splitting while one node's estimated feature/member count exceeds a derived threshold</li>
 * </ul>
 *
 * <p>To preserve backwards compatibility, the default remains {@code fixed-count}.
 */
@SpatialPartitioner.Metadata(
    disjointSupported = true,
    extension = "kdtree",
    description = "Recursively partitions the space in half (based on the median) alternating between the dimensions " +
        "until either a requested number of partitions is reached or an adaptive count/load threshold is satisfied.")
public class KDTreePartitioner extends SpatialPartitioner {
  /** Whether disjoint partitions are desired. */
  protected boolean disjoint;

  /**
   * Fixed-count KD-tree split coordinates. These fields preserve the original complete-binary-tree implementation
   * when {@code kdtree.refine.strategy=fixed-count}.
   */
  protected double[] splitCoords = new double[0];

  /** Cached axis of every fixed-count split. */
  protected int[] splitAxis = new int[0];

  /** Number of dimensions for this tree. */
  protected int numDimensions;

  /** The MBR of the input space. */
  protected final EnvelopeNDLite envelope = new EnvelopeNDLite();

  /** Recursive refinement strategy for the KD-tree. */
  private String refinementStrategy = SpatialJoinConstants.KDTree.DEFAULT_REFINE_STRATEGY;
  /** Target per-partition memory-load threshold used by adaptive-load refinement. */
  private long targetPartitionSizeBytes = SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES;
  /** Derived per-partition feature-count threshold used by adaptive-count refinement. */
  private long targetPartitionFeatureCount = Long.MAX_VALUE;
  /** Maximum adaptive recursion depth. */
  private int maxRefinementDepth = SpatialJoinConstants.KDTree.DEFAULT_MAX_REFINEMENT_DEPTH;
  /** Dataset-summary totals reused when deriving the count threshold. */
  private long configuredLeftSizeBytes = -1L;
  private long configuredRightSizeBytes = -1L;
  private long configuredLeftFeatureCount = -1L;
  private long configuredRightFeatureCount = -1L;
  /** Debug/analysis counters for adaptive refinement. */
  private int adaptiveSplitCount = 0;
  private int thresholdSatisfiedLeafCount = 0;
  private int splitLimitLeafCount = 0;
  private int maxObservedRefinementDepth = 0;

  /**
   * Explicit adaptive KD-tree node list. The historical fixed-count complete tree uses {@link #splitCoords} and
   * {@link #splitAxis}; adaptive modes store an irregular binary tree here instead.
   */
  private AdaptiveNode[] adaptiveNodes = new AdaptiveNode[0];
  /** One leaf envelope per adaptive KD partition. */
  private EnvelopeNDLite[] adaptiveLeafEnvelopes = new EnvelopeNDLite[0];

  @Override
  public void setup(SpatialJoinOptions context, boolean disjoint) {
    this.disjoint = disjoint;
    this.refinementStrategy = normalizeRefinementStrategy(
        context == null
            ? SpatialJoinConstants.KDTree.DEFAULT_REFINE_STRATEGY
            : context.getString(SpatialJoinConstants.KDTree.REFINE_STRATEGY,
                SpatialJoinConstants.KDTree.DEFAULT_REFINE_STRATEGY));
    this.targetPartitionSizeBytes = resolveTargetPartitionBytes(context);
    this.maxRefinementDepth = resolveMaxRefinementDepth(context);
    this.configuredLeftSizeBytes = readOptionalLong(context, SpatialJoinConstants.LAHilbert.LEFT_SUMMARY_SIZE);
    this.configuredRightSizeBytes = readOptionalLong(context, SpatialJoinConstants.LAHilbert.RIGHT_SUMMARY_SIZE);
    this.configuredLeftFeatureCount = readOptionalLong(context, SpatialJoinConstants.LAHilbert.LEFT_SUMMARY_FEATURES);
    this.configuredRightFeatureCount = readOptionalLong(context, SpatialJoinConstants.LAHilbert.RIGHT_SUMMARY_FEATURES);
    this.targetPartitionFeatureCount = resolveTargetPartitionFeatureCount();
    this.adaptiveSplitCount = 0;
    this.thresholdSatisfiedLeafCount = 0;
    this.splitLimitLeafCount = 0;
    this.maxObservedRefinementDepth = 0;
  }

  @Override
  public void construct(Summary summary, @Required double[][] sample, AbstractHistogram histogram, int numPartitions) {
    if (!usesAdaptiveRefinement()) {
      constructFixedCount(summary, sample, numPartitions);
      adaptiveNodes = new AdaptiveNode[0];
      adaptiveLeafEnvelopes = new EnvelopeNDLite[0];
      return;
    }
    construct(summary, toSyntheticFeatures(sample), histogram, numPartitions);
  }

  @Override
  public void construct(Summary summary, AbstractFeature[] features, AbstractHistogram histogram, int numPartitions) {
    if (!usesAdaptiveRefinement()) {
      constructFixedCount(summary, toFeaturePoints(features), numPartitions);
      adaptiveNodes = new AdaptiveNode[0];
      adaptiveLeafEnvelopes = new EnvelopeNDLite[0];
      return;
    }
    constructAdaptive(summary, features);
  }

  @Override
  public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    matchedPartitions.clear();
    if (usesAdaptiveRefinement()) {
      overlapAdaptivePartitions(mbr, matchedPartitions);
      return;
    }
    IntArray searchStack = new IntArray();
    searchStack.add(0); // Root split node
    while (!searchStack.isEmpty()) {
      int nodeID = searchStack.pop();
      if (nodeID >= splitCoords.length) {
        matchedPartitions.add(nodeID - splitCoords.length);
        continue;
      }

      int axis = splitAxis[nodeID];
      if (mbr.getMaxCoord(axis) < splitCoords[nodeID]) {
        searchStack.add(nodeID * 2 + 1);
      } else if (mbr.getMinCoord(axis) > splitCoords[nodeID]) {
        searchStack.add(nodeID * 2 + 2);
      } else {
        searchStack.add(nodeID * 2 + 2);
        searchStack.add(nodeID * 2 + 1);
      }
    }
  }

  @Override
  public int overlapPartition(EnvelopeNDLite mbr) {
    if (usesAdaptiveRefinement())
      return overlapAdaptivePartition(mbr);
    int partition = 0; // Start at the root
    while (partition < splitCoords.length) {
      int axis = splitAxis[partition];
      double center = (mbr.getMinCoord(axis) + mbr.getMaxCoord(axis)) / 2.0;
      if (center < splitCoords[partition])
        partition = partition * 2 + 1;
      else
        partition = partition * 2 + 2;
    }
    return partition - splitCoords.length;
  }

  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    if (usesAdaptiveRefinement()) {
      if (partitionID < 0 || partitionID >= adaptiveLeafEnvelopes.length)
        throw new IllegalArgumentException("Invalid partition id: " + partitionID);
      mbr.set(adaptiveLeafEnvelopes[partitionID]);
      return;
    }
    partitionID += splitCoords.length;
    mbr.set(envelope);
    while (partitionID > 0) {
      int parentPartitionID = (partitionID - 1) / 2;
      int axis = splitAxis[parentPartitionID];
      if ((partitionID & 1) == 1) {
        mbr.setMaxCoord(axis, Math.min(mbr.getMaxCoord(axis), splitCoords[parentPartitionID]));
      } else {
        mbr.setMinCoord(axis, Math.max(mbr.getMinCoord(axis), splitCoords[parentPartitionID]));
      }
      partitionID = parentPartitionID;
    }
  }

  @Override
  public int numPartitions() {
    return usesAdaptiveRefinement() ? adaptiveLeafEnvelopes.length : splitCoords.length + 1;
  }

  @Override
  public boolean isDisjoint() {
    return disjoint;
  }

  @Override
  public int getCoordinateDimension() {
    return numDimensions;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(this.envelope, out);
    out.writeBoolean(disjoint);
    out.writeInt(numDimensions);
    out.writeUTF(refinementStrategy == null ? SpatialJoinConstants.KDTree.DEFAULT_REFINE_STRATEGY : refinementStrategy);
    out.writeLong(targetPartitionSizeBytes);
    out.writeLong(targetPartitionFeatureCount);
    out.writeInt(maxRefinementDepth);
    out.writeLong(configuredLeftSizeBytes);
    out.writeLong(configuredRightSizeBytes);
    out.writeLong(configuredLeftFeatureCount);
    out.writeLong(configuredRightFeatureCount);
    boolean adaptive = usesAdaptiveRefinement();
    out.writeBoolean(adaptive);
    if (adaptive) {
      out.writeInt(adaptiveNodes.length);
      for (AdaptiveNode node : adaptiveNodes) {
        out.writeBoolean(node.leaf);
        out.writeInt(node.splitAxis);
        out.writeDouble(node.splitCoord);
        out.writeInt(node.leftChild);
        out.writeInt(node.rightChild);
        out.writeInt(node.partitionId);
      }
      out.writeInt(adaptiveLeafEnvelopes.length);
      for (EnvelopeNDLite leafEnvelope : adaptiveLeafEnvelopes)
        GeometryHelper.writeIEnvelope(leafEnvelope, out);
    } else {
      out.writeInt(splitCoords.length);
      for (int i = 0; i < splitCoords.length; i++) {
        out.writeDouble(splitCoords[i]);
        out.writeInt(splitAxis[i]);
      }
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(this.envelope, in);
    this.disjoint = in.readBoolean();
    numDimensions = in.readInt();
    this.refinementStrategy = normalizeRefinementStrategy(in.readUTF());
    this.targetPartitionSizeBytes = in.readLong();
    this.targetPartitionFeatureCount = in.readLong();
    this.maxRefinementDepth = in.readInt();
    this.configuredLeftSizeBytes = in.readLong();
    this.configuredRightSizeBytes = in.readLong();
    this.configuredLeftFeatureCount = in.readLong();
    this.configuredRightFeatureCount = in.readLong();
    boolean adaptive = in.readBoolean();
    if (adaptive) {
      int nodeCount = in.readInt();
      adaptiveNodes = new AdaptiveNode[nodeCount];
      for (int i = 0; i < nodeCount; i++) {
        adaptiveNodes[i] = new AdaptiveNode(
            in.readBoolean(),
            in.readInt(),
            in.readDouble(),
            in.readInt(),
            in.readInt(),
            in.readInt());
      }
      int leafCount = in.readInt();
      adaptiveLeafEnvelopes = new EnvelopeNDLite[leafCount];
      for (int i = 0; i < leafCount; i++) {
        EnvelopeNDLite leafEnvelope = new EnvelopeNDLite();
        GeometryHelper.readIEnvelope(leafEnvelope, in);
        adaptiveLeafEnvelopes[i] = leafEnvelope;
      }
      splitCoords = new double[0];
      splitAxis = new int[0];
    } else {
      int numSplits = in.readInt();
      this.splitCoords = new double[numSplits];
      this.splitAxis = new int[numSplits];
      for (int i = 0; i < splitCoords.length; i++) {
        splitCoords[i] = in.readDouble();
        splitAxis[i] = in.readInt();
      }
      adaptiveNodes = new AdaptiveNode[0];
      adaptiveLeafEnvelopes = new EnvelopeNDLite[0];
    }
  }

  @Override
  public EnvelopeNDLite getEnvelope() {
    return envelope;
  }

  private void constructFixedCount(Summary summary, double[][] sample, int numPartitions) {
    this.envelope.set(summary);
    numDimensions = sample == null ? summary.getCoordinateDimension() : sample.length;
    final int sampleCount = sample == null || sample.length == 0 ? 0 : sample[0].length;
    splitCoords = new double[Math.max(0, numPartitions - 1)];
    splitAxis = new int[Math.max(0, numPartitions - 1)];
    if (numPartitions <= 1 || sampleCount <= 1 || sample == null || sample.length == 0)
      return;
    if (numPartitions > sampleCount)
      numPartitions = sampleCount;

    class SplitRange {
      int start, end;
      int splitAxis;
      int splitID;

      SplitRange(int splitID, int s, int e, int x) {
        this.splitID = splitID;
        this.start = s;
        this.end = e;
        this.splitAxis = x;
      }
    }

    class AxisSorter implements IndexedSortable {
      int axis;

      @Override
      public int compare(int i, int j) {
        return Double.compare(sample[axis][i], sample[axis][j]);
      }

      @Override
      public void swap(int i, int j) {
        double t;
        for (int d = 0; d < numDimensions; d++) {
          t = sample[d][i];
          sample[d][i] = sample[d][j];
          sample[d][j] = t;
        }
      }
    }

    AxisSorter sorter = new AxisSorter();
    QuickSort quickSort = new QuickSort();
    Queue<SplitRange> toSplit = new ArrayDeque<>();
    toSplit.add(new SplitRange(0, 0, sampleCount, 0));
    while (!toSplit.isEmpty()) {
      SplitRange rangeToSplit = toSplit.remove();
      sorter.axis = rangeToSplit.splitAxis;
      quickSort.sort(sorter, rangeToSplit.start, rangeToSplit.end);
      int m = (rangeToSplit.start + rangeToSplit.end + 1) / 2;
      splitCoords[rangeToSplit.splitID] =
          (sample[rangeToSplit.splitAxis][m - 1] + sample[rangeToSplit.splitAxis][m]) / 2.0;
      splitAxis[rangeToSplit.splitID] = rangeToSplit.splitAxis;
      int lowerSplitID = rangeToSplit.splitID * 2 + 1;
      int upperSplitID = lowerSplitID + 1;
      if (upperSplitID < splitCoords.length)
        toSplit.add(new SplitRange(upperSplitID, m, rangeToSplit.end, (rangeToSplit.splitAxis + 1) % numDimensions));
      if (lowerSplitID < splitCoords.length) {
        rangeToSplit.end = m;
        rangeToSplit.splitID = lowerSplitID;
        rangeToSplit.splitAxis = (rangeToSplit.splitAxis + 1) % numDimensions;
        toSplit.add(rangeToSplit);
      }
    }
  }

  private void constructAdaptive(Summary summary, AbstractFeature[] features) {
    this.envelope.set(summary);
    this.numDimensions = resolveCoordinateDimension(summary, features);
    this.splitCoords = new double[0];
    this.splitAxis = new int[0];

    if (features == null || features.length == 0 || numDimensions <= 0) {
      adaptiveNodes = new AdaptiveNode[]{new AdaptiveNode(true, -1, 0.0d, -1, -1, 0)};
      adaptiveLeafEnvelopes = new EnvelopeNDLite[]{new EnvelopeNDLite(envelope)};
      return;
    }

    int[] rootIndexes = new int[features.length];
    for (int i = 0; i < features.length; i++)
      rootIndexes[i] = i;
    EnvelopeNDLite rootEnvelope = new EnvelopeNDLite(envelope);
    if (rootEnvelope.isEmpty())
      rootEnvelope = computeEnvelope(features, rootIndexes, null);

    ArrayList<AdaptiveNode> nodes = new ArrayList<>();
    ArrayList<EnvelopeNDLite> leaves = new ArrayList<>();
    buildAdaptiveNode(
        features,
        rootIndexes,
        0,
        0,
        rootEnvelope,
        sumLoadWeight(features, rootIndexes),
        sumFeatureCount(features, rootIndexes),
        nodes,
        leaves);
    adaptiveNodes = nodes.toArray(new AdaptiveNode[0]);
    adaptiveLeafEnvelopes = leaves.toArray(new EnvelopeNDLite[0]);
  }

  private int buildAdaptiveNode(AbstractFeature[] features,
                                int[] indexes,
                                int depth,
                                int preferredAxis,
                                EnvelopeNDLite nodeEnvelope,
                                long estimatedBytes,
                                long featureCount,
                                List<AdaptiveNode> nodes,
                                List<EnvelopeNDLite> leaves) {
    maxObservedRefinementDepth = Math.max(maxObservedRefinementDepth, depth);
    AdaptiveStopReason stopReason =
        classifyAdaptiveStopReason(depth, estimatedBytes, featureCount, indexes == null ? 0 : indexes.length);
    if (stopReason != AdaptiveStopReason.CONTINUE) {
      recordAdaptiveLeafStop(stopReason);
      int partitionId = leaves.size();
      leaves.add(new EnvelopeNDLite(nodeEnvelope));
      int nodeId = nodes.size();
      nodes.add(new AdaptiveNode(true, -1, 0.0d, -1, -1, partitionId));
      return nodeId;
    }

    int axis = chooseAdaptiveSplitAxis(features, indexes, preferredAxis);
    if (axis < 0) {
      int partitionId = leaves.size();
      leaves.add(new EnvelopeNDLite(nodeEnvelope));
      int nodeId = nodes.size();
      nodes.add(new AdaptiveNode(true, -1, 0.0d, -1, -1, partitionId));
      return nodeId;
    }

    int[] sortedIndexes = indexes.clone();
    sortIndexesByAxis(sortedIndexes, features, axis);
    int mid = sortedIndexes.length / 2;
    if (mid <= 0 || mid >= sortedIndexes.length) {
      int partitionId = leaves.size();
      leaves.add(new EnvelopeNDLite(nodeEnvelope));
      int nodeId = nodes.size();
      nodes.add(new AdaptiveNode(true, -1, 0.0d, -1, -1, partitionId));
      return nodeId;
    }
    double lowerCoord = features[sortedIndexes[mid - 1]].getCenter(axis);
    double upperCoord = features[sortedIndexes[mid]].getCenter(axis);
    double splitCoord = (lowerCoord + upperCoord) * 0.5d;
    if (!(splitCoord > nodeEnvelope.getMinCoord(axis) && splitCoord < nodeEnvelope.getMaxCoord(axis))) {
      splitCoord = nodeEnvelope.getMinCoord(axis) + nodeEnvelope.getSideLength(axis) * 0.5d;
      if (!(splitCoord > nodeEnvelope.getMinCoord(axis) && splitCoord < nodeEnvelope.getMaxCoord(axis))) {
        int partitionId = leaves.size();
        leaves.add(new EnvelopeNDLite(nodeEnvelope));
        int nodeId = nodes.size();
        nodes.add(new AdaptiveNode(true, -1, 0.0d, -1, -1, partitionId));
        return nodeId;
      }
    }

    int[] leftIndexes = new int[mid];
    int[] rightIndexes = new int[sortedIndexes.length - mid];
    System.arraycopy(sortedIndexes, 0, leftIndexes, 0, leftIndexes.length);
    System.arraycopy(sortedIndexes, mid, rightIndexes, 0, rightIndexes.length);
    if (leftIndexes.length == 0 || rightIndexes.length == 0) {
      int partitionId = leaves.size();
      leaves.add(new EnvelopeNDLite(nodeEnvelope));
      int nodeId = nodes.size();
      nodes.add(new AdaptiveNode(true, -1, 0.0d, -1, -1, partitionId));
      return nodeId;
    }

    EnvelopeNDLite leftEnvelope = new EnvelopeNDLite(nodeEnvelope);
    leftEnvelope.setMaxCoord(axis, splitCoord);
    EnvelopeNDLite rightEnvelope = new EnvelopeNDLite(nodeEnvelope);
    rightEnvelope.setMinCoord(axis, splitCoord);

    adaptiveSplitCount++;
    int nodeId = nodes.size();
    nodes.add(new AdaptiveNode(false, axis, splitCoord, -1, -1, -1));
    int nextPreferredAxis = (axis + 1) % Math.max(1, numDimensions);
    int leftChild = buildAdaptiveNode(
        features, leftIndexes, depth + 1, nextPreferredAxis, leftEnvelope,
        sumLoadWeight(features, leftIndexes), sumFeatureCount(features, leftIndexes), nodes, leaves);
    int rightChild = buildAdaptiveNode(
        features, rightIndexes, depth + 1, nextPreferredAxis, rightEnvelope,
        sumLoadWeight(features, rightIndexes), sumFeatureCount(features, rightIndexes), nodes, leaves);
    nodes.set(nodeId, new AdaptiveNode(false, axis, splitCoord, leftChild, rightChild, -1));
    return nodeId;
  }

  private void overlapAdaptivePartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    if (adaptiveNodes == null || adaptiveNodes.length == 0) {
      if (adaptiveLeafEnvelopes != null && adaptiveLeafEnvelopes.length > 0)
        matchedPartitions.add(0);
      return;
    }
    IntArray searchStack = new IntArray();
    searchStack.add(0);
    while (!searchStack.isEmpty()) {
      int nodeId = searchStack.pop();
      AdaptiveNode node = adaptiveNodes[nodeId];
      if (node.leaf) {
        matchedPartitions.add(node.partitionId);
        continue;
      }
      int axis = node.splitAxis;
      if (mbr.getMaxCoord(axis) < node.splitCoord) {
        searchStack.add(node.leftChild);
      } else if (mbr.getMinCoord(axis) > node.splitCoord) {
        searchStack.add(node.rightChild);
      } else {
        searchStack.add(node.rightChild);
        searchStack.add(node.leftChild);
      }
    }
  }

  private int overlapAdaptivePartition(EnvelopeNDLite mbr) {
    if (adaptiveNodes == null || adaptiveNodes.length == 0)
      return 0;
    int nodeId = 0;
    while (true) {
      AdaptiveNode node = adaptiveNodes[nodeId];
      if (node.leaf)
        return node.partitionId;
      double center = (mbr.getMinCoord(node.splitAxis) + mbr.getMaxCoord(node.splitAxis)) * 0.5d;
      nodeId = center < node.splitCoord ? node.leftChild : node.rightChild;
    }
  }

  private boolean shouldAdaptiveRefine(int depth, long estimatedBytes, long featureCount, int rawFeatureCount) {
    return classifyAdaptiveStopReason(depth, estimatedBytes, featureCount, rawFeatureCount) == AdaptiveStopReason.CONTINUE;
  }

  private AdaptiveStopReason classifyAdaptiveStopReason(int depth,
                                                        long estimatedBytes,
                                                        long featureCount,
                                                        int rawFeatureCount) {
    if (rawFeatureCount < 2)
      return AdaptiveStopReason.TERMINAL_SMALL;
    if (maxRefinementDepth >= 0 && depth >= maxRefinementDepth)
      return AdaptiveStopReason.SPLIT_LIMIT;
    if (SpatialJoinConstants.KDTree.REFINE_STRATEGY_ADAPTIVE_COUNT.equals(refinementStrategy))
      return featureCount > targetPartitionFeatureCount
          ? AdaptiveStopReason.CONTINUE
          : AdaptiveStopReason.THRESHOLD_SATISFIED;
    return estimatedBytes > targetPartitionSizeBytes
        ? AdaptiveStopReason.CONTINUE
        : AdaptiveStopReason.THRESHOLD_SATISFIED;
  }

  private void recordAdaptiveLeafStop(AdaptiveStopReason stopReason) {
    if (stopReason == AdaptiveStopReason.THRESHOLD_SATISFIED)
      thresholdSatisfiedLeafCount++;
    else if (stopReason == AdaptiveStopReason.SPLIT_LIMIT)
      splitLimitLeafCount++;
  }

  private boolean usesAdaptiveRefinement() {
    return SpatialJoinConstants.KDTree.REFINE_STRATEGY_ADAPTIVE_COUNT.equals(refinementStrategy) ||
        SpatialJoinConstants.KDTree.REFINE_STRATEGY_ADAPTIVE_LOAD.equals(refinementStrategy);
  }

  private String normalizeRefinementStrategy(String configuredStrategy) {
    if (SpatialJoinConstants.KDTree.REFINE_STRATEGY_ADAPTIVE_COUNT.equals(configuredStrategy))
      return SpatialJoinConstants.KDTree.REFINE_STRATEGY_ADAPTIVE_COUNT;
    if (SpatialJoinConstants.KDTree.REFINE_STRATEGY_ADAPTIVE_LOAD.equals(configuredStrategy))
      return SpatialJoinConstants.KDTree.REFINE_STRATEGY_ADAPTIVE_LOAD;
    return SpatialJoinConstants.KDTree.REFINE_STRATEGY_FIXED_COUNT;
  }

  private long resolveTargetPartitionBytes(SpatialJoinOptions conf) {
    if (conf == null)
      return SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES;
    if (conf.contains(SpatialJoinConstants.KDTree.PARTITION_BYTES))
      return conf.getSizeAsBytes(
          SpatialJoinConstants.KDTree.PARTITION_BYTES,
          SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES);
    if (conf.contains(SpatialJoinConstants.LAHilbert.PARTITION_BYTES))
      return conf.getSizeAsBytes(
          SpatialJoinConstants.LAHilbert.PARTITION_BYTES,
          SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES);

    long executorMemoryBytes = conf.getSizeAsBytes(
        SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY_KEY,
        SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY);
    int executorCores = Math.max(1, conf.getInt(
        SpatialJoinConstants.SparkDefaults.EXECUTOR_CORES_KEY,
        SpatialJoinConstants.SparkDefaults.EXECUTOR_CORES));
    double memoryFraction = conf.getDouble(
        SpatialJoinConstants.SparkDefaults.MEMORY_FRACTION_KEY,
        SpatialJoinConstants.SparkDefaults.MEMORY_FRACTION);
    double storageFraction = conf.getDouble(
        SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION_KEY,
        SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION);
    double safetyFraction = conf.getDouble(
        SpatialJoinConstants.LAHilbert.SAFETY_FRACTION,
        SpatialJoinConstants.LAHilbert.DEFAULT_SAFETY_FRACTION);
    long derivedBytes = (long) (executorMemoryBytes * memoryFraction * (1.0d - storageFraction) * safetyFraction / executorCores);
    return Math.max(1L, derivedBytes);
  }

  private int resolveMaxRefinementDepth(SpatialJoinOptions conf) {
    if (conf == null)
      return SpatialJoinConstants.KDTree.DEFAULT_MAX_REFINEMENT_DEPTH;
    return conf.getInt(
        SpatialJoinConstants.KDTree.MAX_REFINEMENT_DEPTH,
        SpatialJoinConstants.KDTree.DEFAULT_MAX_REFINEMENT_DEPTH);
  }

  private long resolveTargetPartitionFeatureCount() {
    long totalSizeBytes = 0L;
    long totalFeatureCount = 0L;
    if (configuredLeftSizeBytes > 0L)
      totalSizeBytes += configuredLeftSizeBytes;
    if (configuredRightSizeBytes > 0L)
      totalSizeBytes += configuredRightSizeBytes;
    if (configuredLeftFeatureCount > 0L)
      totalFeatureCount += configuredLeftFeatureCount;
    if (configuredRightFeatureCount > 0L)
      totalFeatureCount += configuredRightFeatureCount;
    if (totalSizeBytes <= 0L || totalFeatureCount <= 0L || targetPartitionSizeBytes <= 0L)
      return Long.MAX_VALUE;
    double averageBytesPerFeature = Math.max(1.0d, totalSizeBytes / (double) totalFeatureCount);
    long derivedThreshold = (long) Math.floor(targetPartitionSizeBytes / averageBytesPerFeature);
    return Math.max(2L, Math.max(1L, derivedThreshold));
  }

  private int readOptionalInt(SpatialJoinOptions context, String key, int defaultValue) {
    if (context == null || !context.contains(key))
      return defaultValue;
    return context.getInt(key, defaultValue);
  }

  private long readOptionalLong(SpatialJoinOptions context, String key) {
    if (context == null || !context.contains(key))
      return -1L;
    return context.getLong(key, -1L);
  }

  private int resolveCoordinateDimension(Summary summary, AbstractFeature[] features) {
    if (summary != null && summary.getCoordinateDimension() > 0)
      return summary.getCoordinateDimension();
    if (features != null) {
      for (AbstractFeature feature : features) {
        if (feature != null && feature.getCoordinateDimension() > 0)
          return feature.getCoordinateDimension();
      }
    }
    return 0;
  }

  private AbstractFeature[] toSyntheticFeatures(double[][] points) {
    if (points == null || points.length == 0 || points[0].length == 0)
      return new AbstractFeature[0];
    int dims = points.length;
    int count = points[0].length;
    AbstractFeature[] features = new AbstractFeature[count];
    for (int i = 0; i < count; i++) {
      EnvelopeNDLite pointEnvelope = new EnvelopeNDLite(dims);
      for (int d = 0; d < dims; d++) {
        pointEnvelope.setMinCoord(d, points[d][i]);
        pointEnvelope.setMaxCoord(d, points[d][i]);
      }
      features[i] = new AbstractFeature(
          pointEnvelope,
          i,
          1,
          1L,
          1L,
          0.0d,
          0.0d,
          1L,
          0,
          GeometryType.POINT,
          null);
    }
    return features;
  }

  private int chooseAdaptiveSplitAxis(AbstractFeature[] features, int[] indexes, int preferredAxis) {
    if (numDimensions <= 0)
      return -1;
    for (int offset = 0; offset < numDimensions; offset++) {
      int axis = (preferredAxis + offset) % numDimensions;
      double min = Double.POSITIVE_INFINITY;
      double max = Double.NEGATIVE_INFINITY;
      for (int index : indexes) {
        double center = features[index].getCenter(axis);
        if (center < min)
          min = center;
        if (center > max)
          max = center;
      }
      if (max > min)
        return axis;
    }
    return -1;
  }

  private void sortIndexesByAxis(final int[] indexes, final AbstractFeature[] features, final int axis) {
    if (indexes == null || indexes.length <= 1)
      return;
    IndexedSortable sortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        return Double.compare(features[indexes[i]].getCenter(axis), features[indexes[j]].getCenter(axis));
      }

      @Override
      public void swap(int i, int j) {
        int temp = indexes[i];
        indexes[i] = indexes[j];
        indexes[j] = temp;
      }
    };
    new QuickSort().sort(sortable, 0, indexes.length);
  }

  private long sumLoadWeight(AbstractFeature[] features, int[] indexes) {
    long total = 0L;
    if (features == null || indexes == null)
      return total;
    for (int index : indexes) {
      if (index >= 0 && index < features.length && features[index] != null)
        total += Math.max(0L, features[index].getLoadWeight());
    }
    return total;
  }

  private long sumFeatureCount(AbstractFeature[] features, int[] indexes) {
    long total = 0L;
    if (features == null || indexes == null)
      return total;
    for (int index : indexes) {
      if (index >= 0 && index < features.length && features[index] != null)
        total += Math.max(1L, features[index].getMemberCount());
    }
    return total;
  }

  private EnvelopeNDLite computeEnvelope(AbstractFeature[] features, int[] indexes, EnvelopeNDLite fallback) {
    EnvelopeNDLite result = new EnvelopeNDLite(numDimensions);
    boolean found = false;
    if (features != null && indexes != null) {
      for (int index : indexes) {
        if (index >= 0 && index < features.length && features[index] != null) {
          if (!found) {
            result.set(features[index]);
            found = true;
          } else {
            result.merge(features[index]);
          }
        }
      }
    }
    if (!found && fallback != null)
      result.set(fallback);
    return result;
  }

  private static class AdaptiveNode {
    private final boolean leaf;
    private final int splitAxis;
    private final double splitCoord;
    private final int leftChild;
    private final int rightChild;
    private final int partitionId;

    private AdaptiveNode(boolean leaf,
                         int splitAxis,
                         double splitCoord,
                         int leftChild,
                         int rightChild,
                         int partitionId) {
      this.leaf = leaf;
      this.splitAxis = splitAxis;
      this.splitCoord = splitCoord;
      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.partitionId = partitionId;
    }
  }

  public long getTargetPartitionSizeBytes() {
    return targetPartitionSizeBytes;
  }

  public String getRefinementStrategy() {
    return refinementStrategy;
  }

  public long getTargetPartitionFeatureCount() {
    return targetPartitionFeatureCount;
  }

  public int getAdaptiveSplitCount() {
    return adaptiveSplitCount;
  }

  public int getThresholdSatisfiedLeafCount() {
    return thresholdSatisfiedLeafCount;
  }

  public int getSplitLimitLeafCount() {
    return splitLimitLeafCount;
  }

  public int getMaxObservedRefinementDepth() {
    return maxObservedRefinementDepth;
  }

  private enum AdaptiveStopReason {
    CONTINUE,
    THRESHOLD_SATISFIED,
    SPLIT_LIMIT,
    TERMINAL_SMALL
  }
}
