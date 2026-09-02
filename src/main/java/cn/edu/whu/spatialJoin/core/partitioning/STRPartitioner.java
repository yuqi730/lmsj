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

@SpatialPartitioner.Metadata(
    disjointSupported = true,
    extension = "str",
    description = "Partitions the space using the Sort-tile-recursive (STR) algorithm for R-tree packing")
public class STRPartitioner extends SpatialPartitioner {

  private boolean disjoint;
  private int treeDegree;
  private int numTreeLevels;
  protected double[] splitCoords = new double[0];
  protected int[] nodeLevel = new int[0];
  protected int numDimensions;
  protected transient int numInternalNodes;
  protected EnvelopeNDLite envelope = new EnvelopeNDLite();

  private String refinementStrategy = SpatialJoinConstants.STRTree.DEFAULT_REFINE_STRATEGY;
  private long targetPartitionSizeBytes = SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES;
  private long targetPartitionFeatureCount = Long.MAX_VALUE;
  private int maxRefinementDepth = SpatialJoinConstants.STRTree.DEFAULT_MAX_REFINEMENT_DEPTH;
  private int maxAdaptiveChildren = SpatialJoinConstants.STRTree.DEFAULT_MAX_CHILDREN;
  private long configuredLeftSizeBytes = -1L;
  private long configuredRightSizeBytes = -1L;
  private long configuredLeftFeatureCount = -1L;
  private long configuredRightFeatureCount = -1L;
  private int adaptiveSplitCount = 0;
  private int thresholdSatisfiedLeafCount = 0;
  private int splitLimitLeafCount = 0;
  private int maxObservedRefinementDepth = 0;

  private AdaptiveNode[] adaptiveNodes = new AdaptiveNode[0];
  private EnvelopeNDLite[] adaptiveLeafEnvelopes = new EnvelopeNDLite[0];

  @Override
  public void setup(SpatialJoinOptions conf, boolean disjoint) {
    this.disjoint = disjoint;
    this.refinementStrategy = normalizeRefinementStrategy(
        conf == null
            ? SpatialJoinConstants.STRTree.DEFAULT_REFINE_STRATEGY
            : conf.getString(
                SpatialJoinConstants.STRTree.REFINE_STRATEGY,
                SpatialJoinConstants.STRTree.DEFAULT_REFINE_STRATEGY));
    this.targetPartitionSizeBytes = resolveTargetPartitionBytes(conf);
    this.maxRefinementDepth = conf == null
        ? SpatialJoinConstants.STRTree.DEFAULT_MAX_REFINEMENT_DEPTH
        : conf.getInt(
            SpatialJoinConstants.STRTree.MAX_REFINEMENT_DEPTH,
            SpatialJoinConstants.STRTree.DEFAULT_MAX_REFINEMENT_DEPTH);
    this.maxAdaptiveChildren = conf == null
        ? SpatialJoinConstants.STRTree.DEFAULT_MAX_CHILDREN
        : Math.max(2, conf.getInt(
            SpatialJoinConstants.STRTree.MAX_CHILDREN,
            SpatialJoinConstants.STRTree.DEFAULT_MAX_CHILDREN));
    this.configuredLeftSizeBytes = readOptionalLong(conf, SpatialJoinConstants.LAHilbert.LEFT_SUMMARY_SIZE);
    this.configuredRightSizeBytes = readOptionalLong(conf, SpatialJoinConstants.LAHilbert.RIGHT_SUMMARY_SIZE);
    this.configuredLeftFeatureCount = readOptionalLong(conf, SpatialJoinConstants.LAHilbert.LEFT_SUMMARY_FEATURES);
    this.configuredRightFeatureCount = readOptionalLong(conf, SpatialJoinConstants.LAHilbert.RIGHT_SUMMARY_FEATURES);
    this.targetPartitionFeatureCount = resolveTargetPartitionFeatureCount();
    this.adaptiveSplitCount = 0;
    this.thresholdSatisfiedLeafCount = 0;
    this.splitLimitLeafCount = 0;
    this.maxObservedRefinementDepth = 0;
  }

  @Override
  public void construct(Summary summary, @Required double[][] points, AbstractHistogram ignoredHistogram, int numBuckets) {
    if (!usesAdaptiveRefinement()) {
      constructFixedCount(summary, points, numBuckets);
      adaptiveNodes = new AdaptiveNode[0];
      adaptiveLeafEnvelopes = new EnvelopeNDLite[0];
      return;
    }
    construct(summary, toSyntheticFeatures(points), ignoredHistogram, numBuckets);
  }

  @Override
  public void construct(Summary summary, AbstractFeature[] features, AbstractHistogram histogram, int numBuckets) {
    if (!usesAdaptiveRefinement()) {
      constructFixedCount(summary, toFeaturePoints(features), numBuckets);
      adaptiveNodes = new AdaptiveNode[0];
      adaptiveLeafEnvelopes = new EnvelopeNDLite[0];
      return;
    }
    constructAdaptive(summary, features);
  }

  @Override
  public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    assert mbr.getCoordinateDimension() == this.getCoordinateDimension();
    matchedPartitions.clear();
    if (usesAdaptiveRefinement()) {
      overlapAdaptivePartitions(mbr, matchedPartitions);
      return;
    }
    if (treeDegree == 0)
      return;

    IntArray searchStack = new IntArray();
    searchStack.add(0);
    while (!searchStack.isEmpty()) {
      int nodeID = searchStack.pop();
      if (nodeID >= numInternalNodes) {
        matchedPartitions.add(nodeID - numInternalNodes);
        continue;
      }

      int axis = nodeLevel[nodeID];
      int s = 0;
      int e = treeDegree - 1;
      int firstSplitIndex = nodeID * (treeDegree - 1);
      while (s < e) {
        int m = (s + e) / 2;
        double diff = mbr.getMinCoord(axis) - splitCoords[firstSplitIndex + m];
        if (diff < 0)
          e = m;
        else
          s = m + 1;
      }
      while (e < (treeDegree - 1) && mbr.getMaxCoord(axis) > splitCoords[firstSplitIndex + e])
        e++;
      for (int childIndex = e; childIndex >= s; childIndex--) {
        int childNodeID = nodeID * treeDegree + childIndex + 1;
        searchStack.add(childNodeID);
      }
    }
  }

  @Override
  public int overlapPartition(EnvelopeNDLite mbr) {
    if (usesAdaptiveRefinement())
      return overlapAdaptivePartition(mbr);
    if (treeDegree == 0)
      return 0;
    int nodeID = 0;
    for (int d = 0; d < this.getCoordinateDimension(); d++) {
      double mbrCenter = (mbr.getMinCoord(d) + mbr.getMaxCoord(d)) / 2.0;
      int s = 0;
      int e = treeDegree - 1;
      int firstSplitIndex = nodeID * (treeDegree - 1);
      while (s < e) {
        int m = (s + e) / 2;
        double diff = mbrCenter - splitCoords[firstSplitIndex + m];
        if (diff < 0)
          e = m;
        else
          s = m + 1;
      }
      nodeID = nodeID * treeDegree + s + 1;
    }
    return nodeID - numInternalNodes;
  }

  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    if (usesAdaptiveRefinement()) {
      if (partitionID < 0 || partitionID >= adaptiveLeafEnvelopes.length)
        throw new IllegalArgumentException("Invalid partition id: " + partitionID);
      mbr.set(adaptiveLeafEnvelopes[partitionID]);
      return;
    }
    partitionID += numInternalNodes;
    if (treeDegree == 0) {
      mbr.set(envelope);
      return;
    }
    mbr.set(envelope);
    while (partitionID > 0) {
      int parentNodeID = (partitionID - 1) / treeDegree;
      int childIndex = partitionID % treeDegree - 1;
      if (childIndex == -1)
        childIndex += treeDegree;
      int axis = nodeLevel[parentNodeID];
      if (childIndex > 0)
        mbr.setMinCoord(axis, Math.max(mbr.getMinCoord(axis), splitCoords[parentNodeID * (treeDegree - 1) + childIndex - 1]));
      if (childIndex < (treeDegree - 1))
        mbr.setMaxCoord(axis, Math.min(mbr.getMaxCoord(axis), splitCoords[parentNodeID * (treeDegree - 1) + childIndex]));
      partitionID = parentNodeID;
    }
  }

  @Override
  public int numPartitions() {
    if (usesAdaptiveRefinement())
      return adaptiveLeafEnvelopes.length;
    int numPartitions = 1;
    for (int l = 0; l < numTreeLevels; l++)
      numPartitions *= treeDegree;
    return numPartitions;
  }

  @Override
  public boolean isDisjoint() {
    return disjoint;
  }

  @Override
  public int getCoordinateDimension() {
    return this.numDimensions;
  }

  @Override
  public EnvelopeNDLite getEnvelope() {
    return envelope;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(envelope, out);
    out.writeBoolean(disjoint);
    out.writeInt(numDimensions);
    out.writeUTF(refinementStrategy == null ? SpatialJoinConstants.STRTree.DEFAULT_REFINE_STRATEGY : refinementStrategy);
    out.writeLong(targetPartitionSizeBytes);
    out.writeLong(targetPartitionFeatureCount);
    out.writeInt(maxRefinementDepth);
    out.writeInt(maxAdaptiveChildren);
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
        out.writeInt(node.partitionId);
        GeometryHelper.writeIEnvelope(node.envelope, out);
        IntArray.writeIntArray(node.children, out);
      }
      out.writeInt(adaptiveLeafEnvelopes.length);
      for (EnvelopeNDLite leafEnvelope : adaptiveLeafEnvelopes)
        GeometryHelper.writeIEnvelope(leafEnvelope, out);
    } else {
      out.writeInt(treeDegree);
      out.writeInt(numTreeLevels);
      if (treeDegree > 0) {
        IntArray.writeIntArray(nodeLevel, out);
        for (double x : splitCoords)
          out.writeDouble(x);
      }
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(envelope, in);
    this.disjoint = in.readBoolean();
    this.numDimensions = in.readInt();
    this.refinementStrategy = normalizeRefinementStrategy(in.readUTF());
    this.targetPartitionSizeBytes = in.readLong();
    this.targetPartitionFeatureCount = in.readLong();
    this.maxRefinementDepth = in.readInt();
    this.maxAdaptiveChildren = in.readInt();
    this.configuredLeftSizeBytes = in.readLong();
    this.configuredRightSizeBytes = in.readLong();
    this.configuredLeftFeatureCount = in.readLong();
    this.configuredRightFeatureCount = in.readLong();
    boolean adaptive = in.readBoolean();
    if (adaptive) {
      int nodeCount = in.readInt();
      adaptiveNodes = new AdaptiveNode[nodeCount];
      for (int i = 0; i < nodeCount; i++) {
        boolean leaf = in.readBoolean();
        int partitionId = in.readInt();
        EnvelopeNDLite nodeEnvelope = new EnvelopeNDLite();
        GeometryHelper.readIEnvelope(nodeEnvelope, in);
        int[] children = IntArray.readIntArray(null, in);
        adaptiveNodes[i] = new AdaptiveNode(leaf, partitionId, nodeEnvelope, children);
      }
      int leafCount = in.readInt();
      adaptiveLeafEnvelopes = new EnvelopeNDLite[leafCount];
      for (int i = 0; i < leafCount; i++) {
        EnvelopeNDLite leafEnvelope = new EnvelopeNDLite();
        GeometryHelper.readIEnvelope(leafEnvelope, in);
        adaptiveLeafEnvelopes[i] = leafEnvelope;
      }
      treeDegree = 0;
      numTreeLevels = 0;
      numInternalNodes = 0;
      nodeLevel = new int[0];
      splitCoords = new double[0];
    } else {
      this.treeDegree = in.readInt();
      this.numTreeLevels = in.readInt();
      if (treeDegree > 0) {
        nodeLevel = IntArray.readIntArray(nodeLevel, in);
        splitCoords = new double[nodeLevel.length * (treeDegree - 1)];
        for (int i = 0; i < splitCoords.length; i++)
          splitCoords[i] = in.readDouble();
      } else {
        nodeLevel = new int[0];
        splitCoords = new double[0];
      }
      adaptiveNodes = new AdaptiveNode[0];
      adaptiveLeafEnvelopes = new EnvelopeNDLite[0];
      numInternalNodes = treeDegree <= 1 ? 0 : (int) ((Math.pow(treeDegree, numDimensions) - 1) / (treeDegree - 1));
    }
  }

  @Override
  public String toString() {
    return "STRPartitioner{" + numPartitions() + "," + (disjoint ? "disjoint" : "non-disjoint") + "," + refinementStrategy + '}';
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

  private void constructFixedCount(Summary summary, double[][] points, int numBuckets) {
    this.envelope.set(summary);
    this.numDimensions = points == null ? summary.getCoordinateDimension() : points.length;
    final int pointCount = points == null || points.length == 0 ? 0 : points[0].length;
    if (numBuckets <= 1 || pointCount <= 1 || points == null || points.length == 0) {
      treeDegree = 0;
      numTreeLevels = 0;
      numInternalNodes = 0;
      nodeLevel = new int[0];
      splitCoords = new double[0];
      return;
    }
    this.treeDegree = (int) Math.ceil(Math.pow(numBuckets, 1.0 / Math.max(1, numDimensions)));
    this.numTreeLevels = Math.max(1, numDimensions);
    numInternalNodes = treeDegree <= 1 ? 0 : (int) ((Math.pow(treeDegree, numDimensions) - 1) / (treeDegree - 1));
    nodeLevel = new int[numInternalNodes];
    int totalNumberOfSplits = numInternalNodes * Math.max(0, treeDegree - 1);
    splitCoords = new double[totalNumberOfSplits];
    if (totalNumberOfSplits == 0)
      return;

    class AxisSorter implements IndexedSortable {
      int axis;
      @Override
      public int compare(int i, int j) {
        return Double.compare(points[axis][i], points[axis][j]);
      }
      @Override
      public void swap(int i, int j) {
        double t;
        for (int d = 0; d < numDimensions; d++) {
          t = points[d][i];
          points[d][i] = points[d][j];
          points[d][j] = t;
        }
      }
    }
    AxisSorter sorter = new AxisSorter();
    QuickSort quickSort = new QuickSort();

    class SplitRange {
      int start, end;
      int splitAxis;
      int partitionID;
      SplitRange(int partitionID, int s, int e, int x) {
        this.partitionID = partitionID;
        this.start = s;
        this.end = e;
        this.splitAxis = x;
      }
    }

    Queue<SplitRange> toSplit = new ArrayDeque<>();
    toSplit.add(new SplitRange(0, 0, pointCount, 0));
    while (!toSplit.isEmpty()) {
      SplitRange rangeToSplit = toSplit.remove();
      sorter.axis = rangeToSplit.splitAxis;
      nodeLevel[rangeToSplit.partitionID] = rangeToSplit.splitAxis;
      quickSort.sort(sorter, rangeToSplit.start, rangeToSplit.end);
      int firstSplitIndex = rangeToSplit.partitionID * (treeDegree - 1);
      int i1 = rangeToSplit.start;
      for (int iSplit = 1; iSplit < treeDegree; iSplit++) {
        int i2 = (rangeToSplit.start * (treeDegree - iSplit) + (rangeToSplit.end + 1) * iSplit) / treeDegree;
        splitCoords[firstSplitIndex + iSplit - 1] = (points[rangeToSplit.splitAxis][i2 - 1] + points[rangeToSplit.splitAxis][i2]) / 2.0;
        if (rangeToSplit.splitAxis < (numDimensions - 1))
          toSplit.add(new SplitRange(rangeToSplit.partitionID * treeDegree + iSplit, i1, i2, rangeToSplit.splitAxis + 1));
        i1 = i2;
      }
      if (rangeToSplit.splitAxis < (numDimensions - 1))
        toSplit.add(new SplitRange(rangeToSplit.partitionID * treeDegree + treeDegree, i1, rangeToSplit.end, rangeToSplit.splitAxis + 1));
    }
  }

  private void constructAdaptive(Summary summary, AbstractFeature[] features) {
    this.envelope.set(summary);
    this.numDimensions = resolveCoordinateDimension(summary, features);
    this.treeDegree = 0;
    this.numTreeLevels = 0;
    this.numInternalNodes = 0;
    this.nodeLevel = new int[0];
    this.splitCoords = new double[0];
    if (features == null || features.length == 0 || numDimensions <= 0) {
      adaptiveNodes = new AdaptiveNode[]{new AdaptiveNode(true, 0, new EnvelopeNDLite(envelope), new int[0])};
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
    buildAdaptiveNode(features, rootIndexes, 0, rootEnvelope, sumLoadWeight(features, rootIndexes), sumFeatureCount(features, rootIndexes), nodes, leaves);
    adaptiveNodes = nodes.toArray(new AdaptiveNode[0]);
    adaptiveLeafEnvelopes = leaves.toArray(new EnvelopeNDLite[0]);
  }

  private int buildAdaptiveNode(AbstractFeature[] features,
                                int[] indexes,
                                int depth,
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
      EnvelopeNDLite leafEnvelope = new EnvelopeNDLite(nodeEnvelope);
      leaves.add(leafEnvelope);
      int nodeId = nodes.size();
      nodes.add(new AdaptiveNode(true, partitionId, leafEnvelope, new int[0]));
      return nodeId;
    }
    int desiredChildren = determineAdaptiveChildCount(estimatedBytes, featureCount, indexes.length);
    ChildGroup[] childGroups = numDimensions == 2
        ? splitAdaptiveGroups2D(features, indexes, nodeEnvelope, desiredChildren)
        : splitAdaptiveGroupsByAxis(features, indexes, nodeEnvelope, depth % Math.max(1, numDimensions), desiredChildren);
    if (childGroups == null || childGroups.length <= 1) {
      int partitionId = leaves.size();
      EnvelopeNDLite leafEnvelope = new EnvelopeNDLite(nodeEnvelope);
      leaves.add(leafEnvelope);
      int nodeId = nodes.size();
      nodes.add(new AdaptiveNode(true, partitionId, leafEnvelope, new int[0]));
      return nodeId;
    }
    adaptiveSplitCount++;
    int nodeId = nodes.size();
    nodes.add(new AdaptiveNode(false, -1, new EnvelopeNDLite(nodeEnvelope), new int[0]));
    int[] children = new int[childGroups.length];
    for (int i = 0; i < childGroups.length; i++) {
      ChildGroup childGroup = childGroups[i];
      children[i] = buildAdaptiveNode(features, childGroup.indexes, depth + 1, childGroup.envelope, childGroup.estimatedBytes, childGroup.featureCount, nodes, leaves);
    }
    nodes.set(nodeId, new AdaptiveNode(false, -1, new EnvelopeNDLite(nodeEnvelope), children));
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
      for (int i = node.children.length - 1; i >= 0; i--) {
        int childId = node.children[i];
        if (adaptiveNodes[childId].envelope.intersectsEnvelopeClosed(mbr))
          searchStack.add(childId);
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
      int containingChild = -1;
      int intersectingChild = -1;
      for (int childId : node.children) {
        EnvelopeNDLite childEnvelope = adaptiveNodes[childId].envelope;
        if (containsCenter(childEnvelope, mbr)) {
          containingChild = childId;
          break;
        }
        if (intersectingChild < 0 && childEnvelope.intersectsEnvelopeClosed(mbr))
          intersectingChild = childId;
      }
      nodeId = containingChild >= 0 ? containingChild : (intersectingChild >= 0 ? intersectingChild : node.children[0]);
    }
  }

  private boolean containsCenter(EnvelopeNDLite partitionEnvelope, EnvelopeNDLite query) {
    if (partitionEnvelope == null || query == null || partitionEnvelope.isEmpty() || query.isEmpty())
      return false;
    for (int d = 0; d < query.getCoordinateDimension(); d++) {
      double center = query.getCenter(d);
      if (center < partitionEnvelope.getMinCoord(d) || center >= partitionEnvelope.getMaxCoord(d))
        return false;
    }
    return true;
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
    if (SpatialJoinConstants.STRTree.REFINE_STRATEGY_ADAPTIVE_COUNT.equals(refinementStrategy))
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

  private int determineAdaptiveChildCount(long estimatedBytes, long featureCount, int rawFeatureCount) {
    long threshold = SpatialJoinConstants.STRTree.REFINE_STRATEGY_ADAPTIVE_COUNT.equals(refinementStrategy)
        ? Math.max(1L, targetPartitionFeatureCount)
        : Math.max(1L, targetPartitionSizeBytes);
    long signal = SpatialJoinConstants.STRTree.REFINE_STRATEGY_ADAPTIVE_COUNT.equals(refinementStrategy)
        ? Math.max(1L, featureCount)
        : Math.max(1L, estimatedBytes);
    long desired = (long) Math.ceil(signal / (double) threshold);
    int clamped = (int) Math.max(2L, Math.min((long) Math.max(2, maxAdaptiveChildren), desired));
    return Math.max(2, Math.min(rawFeatureCount, clamped));
  }

  private ChildGroup[] splitAdaptiveGroups2D(AbstractFeature[] features, int[] indexes, EnvelopeNDLite parentEnvelope, int desiredChildren) {
    if (indexes == null || indexes.length < 2 || parentEnvelope == null || parentEnvelope.isEmpty())
      return null;
    int[] xSorted = indexes.clone();
    sortIndexesByAxis(xSorted, features, 0);
    int columns = Math.max(1, Math.min(desiredChildren, (int) Math.ceil(Math.sqrt(desiredChildren))));
    int[] columnStarts = new int[columns];
    int[] columnEnds = new int[columns];
    double[] columnMinX = new double[columns];
    double[] columnMaxX = new double[columns];
    for (int c = 0; c < columns; c++) {
      columnStarts[c] = (c * xSorted.length) / columns;
      columnEnds[c] = ((c + 1) * xSorted.length) / columns;
      if (columnStarts[c] == columnEnds[c])
        continue;
      columnMinX[c] = c == 0 ? parentEnvelope.getMinCoord(0)
          : midpoint(features[xSorted[columnStarts[c] - 1]].getCenter(0), features[xSorted[columnStarts[c]]].getCenter(0));
      columnMaxX[c] = c == columns - 1 ? parentEnvelope.getMaxCoord(0)
          : midpoint(features[xSorted[columnEnds[c] - 1]].getCenter(0), features[xSorted[columnEnds[c]]].getCenter(0));
    }
    List<ChildGroup> groups = new ArrayList<>();
    int rows = Math.max(1, (int) Math.ceil(desiredChildren / (double) columns));
    for (int c = 0; c < columns; c++) {
      int start = columnStarts[c];
      int end = columnEnds[c];
      if (start >= end)
        continue;
      int[] columnIndexes = new int[end - start];
      System.arraycopy(xSorted, start, columnIndexes, 0, columnIndexes.length);
      sortIndexesByAxis(columnIndexes, features, 1);
      int actualRows = Math.max(1, Math.min(rows, columnIndexes.length));
      for (int r = 0; r < actualRows; r++) {
        int rowStart = (r * columnIndexes.length) / actualRows;
        int rowEnd = ((r + 1) * columnIndexes.length) / actualRows;
        if (rowStart >= rowEnd)
          continue;
        double minY = r == 0 ? parentEnvelope.getMinCoord(1)
            : midpoint(features[columnIndexes[rowStart - 1]].getCenter(1), features[columnIndexes[rowStart]].getCenter(1));
        double maxY = r == actualRows - 1 ? parentEnvelope.getMaxCoord(1)
            : midpoint(features[columnIndexes[rowEnd - 1]].getCenter(1), features[columnIndexes[rowEnd]].getCenter(1));
        if (!(columnMaxX[c] > columnMinX[c]) || !(maxY > minY))
          continue;
        int[] childIndexes = new int[rowEnd - rowStart];
        System.arraycopy(columnIndexes, rowStart, childIndexes, 0, childIndexes.length);
        EnvelopeNDLite childEnvelope = new EnvelopeNDLite(parentEnvelope);
        childEnvelope.setMinCoord(0, columnMinX[c]);
        childEnvelope.setMaxCoord(0, columnMaxX[c]);
        childEnvelope.setMinCoord(1, minY);
        childEnvelope.setMaxCoord(1, maxY);
        groups.add(new ChildGroup(childEnvelope, childIndexes, sumLoadWeight(features, childIndexes), sumFeatureCount(features, childIndexes)));
      }
    }
    return groups.size() <= 1 ? null : groups.toArray(new ChildGroup[0]);
  }

  private ChildGroup[] splitAdaptiveGroupsByAxis(AbstractFeature[] features, int[] indexes, EnvelopeNDLite parentEnvelope, int axis, int desiredChildren) {
    if (indexes == null || indexes.length < 2 || parentEnvelope == null || parentEnvelope.isEmpty())
      return null;
    int[] sorted = indexes.clone();
    sortIndexesByAxis(sorted, features, axis);
    int childCount = Math.max(2, Math.min(desiredChildren, sorted.length));
    List<ChildGroup> groups = new ArrayList<>(childCount);
    for (int childIndex = 0; childIndex < childCount; childIndex++) {
      int start = (childIndex * sorted.length) / childCount;
      int end = ((childIndex + 1) * sorted.length) / childCount;
      if (start >= end)
        continue;
      double minCoord = childIndex == 0 ? parentEnvelope.getMinCoord(axis)
          : midpoint(features[sorted[start - 1]].getCenter(axis), features[sorted[start]].getCenter(axis));
      double maxCoord = childIndex == childCount - 1 ? parentEnvelope.getMaxCoord(axis)
          : midpoint(features[sorted[end - 1]].getCenter(axis), features[sorted[end]].getCenter(axis));
      if (!(maxCoord > minCoord))
        continue;
      int[] childIndexes = new int[end - start];
      System.arraycopy(sorted, start, childIndexes, 0, childIndexes.length);
      EnvelopeNDLite childEnvelope = new EnvelopeNDLite(parentEnvelope);
      childEnvelope.setMinCoord(axis, minCoord);
      childEnvelope.setMaxCoord(axis, maxCoord);
      groups.add(new ChildGroup(childEnvelope, childIndexes, sumLoadWeight(features, childIndexes), sumFeatureCount(features, childIndexes)));
    }
    return groups.size() <= 1 ? null : groups.toArray(new ChildGroup[0]);
  }

  private boolean usesAdaptiveRefinement() {
    return SpatialJoinConstants.STRTree.REFINE_STRATEGY_ADAPTIVE_COUNT.equals(refinementStrategy) ||
        SpatialJoinConstants.STRTree.REFINE_STRATEGY_ADAPTIVE_LOAD.equals(refinementStrategy);
  }

  private String normalizeRefinementStrategy(String configuredStrategy) {
    if (SpatialJoinConstants.STRTree.REFINE_STRATEGY_ADAPTIVE_COUNT.equals(configuredStrategy))
      return SpatialJoinConstants.STRTree.REFINE_STRATEGY_ADAPTIVE_COUNT;
    if (SpatialJoinConstants.STRTree.REFINE_STRATEGY_ADAPTIVE_LOAD.equals(configuredStrategy))
      return SpatialJoinConstants.STRTree.REFINE_STRATEGY_ADAPTIVE_LOAD;
    return SpatialJoinConstants.STRTree.REFINE_STRATEGY_FIXED_COUNT;
  }

  private long resolveTargetPartitionBytes(SpatialJoinOptions conf) {
    if (conf == null)
      return SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES;
    if (conf.contains(SpatialJoinConstants.STRTree.PARTITION_BYTES))
      return conf.getSizeAsBytes(SpatialJoinConstants.STRTree.PARTITION_BYTES, SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES);
    if (conf.contains(SpatialJoinConstants.LAHilbert.PARTITION_BYTES))
      return conf.getSizeAsBytes(SpatialJoinConstants.LAHilbert.PARTITION_BYTES, SpatialJoinConstants.LAHilbert.DEFAULT_PARTITION_BYTES);
    long executorMemoryBytes = conf.getSizeAsBytes(SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY_KEY, SpatialJoinConstants.SparkDefaults.EXECUTOR_MEMORY);
    int executorCores = Math.max(1, conf.getInt(SpatialJoinConstants.SparkDefaults.EXECUTOR_CORES_KEY, SpatialJoinConstants.SparkDefaults.EXECUTOR_CORES));
    double memoryFraction = conf.getDouble(SpatialJoinConstants.SparkDefaults.MEMORY_FRACTION_KEY, SpatialJoinConstants.SparkDefaults.MEMORY_FRACTION);
    double storageFraction = conf.getDouble(SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION_KEY, SpatialJoinConstants.SparkDefaults.STORAGE_FRACTION);
    double safetyFraction = conf.getDouble(SpatialJoinConstants.LAHilbert.SAFETY_FRACTION, SpatialJoinConstants.LAHilbert.DEFAULT_SAFETY_FRACTION);
    long derivedBytes = (long) (executorMemoryBytes * memoryFraction * (1.0d - storageFraction) * safetyFraction / executorCores);
    return Math.max(1L, derivedBytes);
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
      features[i] = new AbstractFeature(pointEnvelope, i, 1, 1L, 1L, 0.0d, 0.0d, 1L, 0, GeometryType.POINT, null);
    }
    return features;
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

  private static double midpoint(double lower, double upper) {
    return (lower + upper) * 0.5d;
  }

  private static class ChildGroup {
    private final EnvelopeNDLite envelope;
    private final int[] indexes;
    private final long estimatedBytes;
    private final long featureCount;
    private ChildGroup(EnvelopeNDLite envelope, int[] indexes, long estimatedBytes, long featureCount) {
      this.envelope = envelope;
      this.indexes = indexes;
      this.estimatedBytes = estimatedBytes;
      this.featureCount = featureCount;
    }
  }

  private static class AdaptiveNode {
    private final boolean leaf;
    private final int partitionId;
    private final EnvelopeNDLite envelope;
    private final int[] children;
    private AdaptiveNode(boolean leaf, int partitionId, EnvelopeNDLite envelope, int[] children) {
      this.leaf = leaf;
      this.partitionId = partitionId;
      this.envelope = envelope == null ? new EnvelopeNDLite(0) : new EnvelopeNDLite(envelope);
      this.children = children == null ? new int[0] : children;
    }
  }

  private enum AdaptiveStopReason {
    CONTINUE,
    THRESHOLD_SATISFIED,
    SPLIT_LIMIT,
    TERMINAL_SMALL
  }
}
