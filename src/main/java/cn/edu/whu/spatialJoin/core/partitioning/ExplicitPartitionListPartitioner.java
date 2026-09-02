package cn.edu.whu.spatialJoin.core.partitioning;

import cn.edu.whu.spatialJoin.common.SpatialJoinOptions;
import cn.edu.whu.spatialJoin.common.Summary;
import cn.edu.whu.spatialJoin.core.partitioning.helper.AbstractFeature;
import cn.edu.whu.spatialJoin.geom.EnvelopeNDLite;
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper;
import cn.edu.whu.spatialJoin.synopses.AbstractHistogram;
import cn.edu.whu.spatialJoin.utils.IntArray;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Compact explicit partition view used after effective-partition pruning for partitioners that
 * cannot natively renumber an arbitrary retained subset into a dense id range.
 *
 * <p>The current partition-aware execution pipeline expects final partition ids to be dense
 * {@code [0, n)} integers because those ids are used directly as Spark partition keys during the
 * prepared working-set materialization stage. LAHilbert can prune and renumber its leaf set
 * internally, but fixed-layout partitioners such as Grid/STR/KD cannot express an arbitrary
 * retained subset that way. This wrapper keeps only the retained envelopes and exposes them as a
 * dense explicit partition list so the later execution stages can keep using the same logic.
 */
@SpatialPartitioner.Metadata(
    disjointSupported = true,
    extension = "explicit",
    description = "Dense explicit partition list used after effective-partition pruning")
public class ExplicitPartitionListPartitioner extends SpatialPartitioner {

  private final EnvelopeNDLite envelope = new EnvelopeNDLite();
  private EnvelopeNDLite[] partitionEnvelopes = new EnvelopeNDLite[0];
  private String[] partitionCodes = new String[0];
  private boolean disjoint;

  public ExplicitPartitionListPartitioner() {
  }

  public ExplicitPartitionListPartitioner(EnvelopeNDLite overallEnvelope,
                                         EnvelopeNDLite[] partitionEnvelopes,
                                         String[] partitionCodes,
                                         boolean disjoint) {
    if (overallEnvelope != null)
      this.envelope.set(overallEnvelope);
    this.partitionEnvelopes = copyEnvelopes(partitionEnvelopes);
    this.partitionCodes = partitionCodes == null ? new String[0] : Arrays.copyOf(partitionCodes, partitionCodes.length);
    this.disjoint = disjoint;
  }

  @Override
  public void setup(SpatialJoinOptions conf, boolean disjoint) {
    this.disjoint = disjoint;
  }

  @Override
  public void construct(Summary summary, double[][] points, AbstractHistogram histogram, int numPartitions) {
    throw new UnsupportedOperationException("ExplicitPartitionListPartitioner is an execution-only view");
  }

  @Override
  public void construct(Summary summary, AbstractFeature[] features, AbstractHistogram histogram, int numPartitions) {
    throw new UnsupportedOperationException("ExplicitPartitionListPartitioner is an execution-only view");
  }

  @Override
  public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {
    matchedPartitions.clear();
    if (mbr == null || partitionEnvelopes == null)
      return;
    for (int partitionId = 0; partitionId < partitionEnvelopes.length; partitionId++) {
      EnvelopeNDLite partitionEnvelope = partitionEnvelopes[partitionId];
      if (partitionEnvelope != null && partitionEnvelope.intersectsEnvelopeClosed(mbr))
        matchedPartitions.add(partitionId);
    }
  }

  @Override
  public int overlapPartition(EnvelopeNDLite mbr) {
    if (partitionEnvelopes == null || partitionEnvelopes.length == 0)
      return 0;
    for (int partitionId = 0; partitionId < partitionEnvelopes.length; partitionId++) {
      EnvelopeNDLite partitionEnvelope = partitionEnvelopes[partitionId];
      if (partitionEnvelope != null && containsCenter(partitionEnvelope, mbr))
        return partitionId;
    }
    for (int partitionId = 0; partitionId < partitionEnvelopes.length; partitionId++) {
      EnvelopeNDLite partitionEnvelope = partitionEnvelopes[partitionId];
      if (partitionEnvelope != null && partitionEnvelope.intersectsEnvelopeClosed(mbr))
        return partitionId;
    }
    return 0;
  }

  @Override
  public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {
    if (partitionID < 0 || partitionID >= numPartitions())
      throw new IllegalArgumentException("Invalid partition id: " + partitionID);
    mbr.set(partitionEnvelopes[partitionID]);
  }

  @Override
  public int numPartitions() {
    return partitionEnvelopes == null ? 0 : partitionEnvelopes.length;
  }

  @Override
  public boolean isDisjoint() {
    return disjoint;
  }

  @Override
  public int getCoordinateDimension() {
    if (!envelope.isEmpty())
      return envelope.getCoordinateDimension();
    if (partitionEnvelopes != null && partitionEnvelopes.length > 0 && partitionEnvelopes[0] != null)
      return partitionEnvelopes[0].getCoordinateDimension();
    return 0;
  }

  @Override
  public EnvelopeNDLite getEnvelope() {
    return new EnvelopeNDLite(envelope);
  }

  public String getPartitionCode(int partitionId) {
    if (partitionCodes == null || partitionId < 0 || partitionId >= partitionCodes.length)
      return Integer.toString(partitionId);
    return partitionCodes[partitionId];
  }

  @Override
  protected String getPartitionCodeForPartitionId(int partitionId) {
    return getPartitionCode(partitionId);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeIEnvelope(envelope, out);
    out.writeBoolean(disjoint);
    int partitionCount = numPartitions();
    out.writeInt(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      GeometryHelper.writeIEnvelope(partitionEnvelopes[i], out);
      out.writeUTF(getPartitionCode(i));
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readIEnvelope(envelope, in);
    disjoint = in.readBoolean();
    int partitionCount = in.readInt();
    partitionEnvelopes = new EnvelopeNDLite[partitionCount];
    partitionCodes = new String[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      EnvelopeNDLite partitionEnvelope = new EnvelopeNDLite();
      GeometryHelper.readIEnvelope(partitionEnvelope, in);
      partitionEnvelopes[i] = partitionEnvelope;
      partitionCodes[i] = in.readUTF();
    }
  }

  private static EnvelopeNDLite[] copyEnvelopes(EnvelopeNDLite[] source) {
    if (source == null || source.length == 0)
      return new EnvelopeNDLite[0];
    EnvelopeNDLite[] copied = new EnvelopeNDLite[source.length];
    for (int i = 0; i < source.length; i++)
      copied[i] = source[i] == null ? new EnvelopeNDLite(0) : new EnvelopeNDLite(source[i]);
    return copied;
  }

  private static boolean containsCenter(EnvelopeNDLite partitionEnvelope, EnvelopeNDLite featureEnvelope) {
    if (partitionEnvelope == null || featureEnvelope == null || partitionEnvelope.isEmpty() || featureEnvelope.isEmpty())
      return false;
    for (int d = 0; d < featureEnvelope.getCoordinateDimension(); d++) {
      double center = featureEnvelope.getCenter(d);
      if (center < partitionEnvelope.getMinCoord(d) || center >= partitionEnvelope.getMaxCoord(d))
        return false;
    }
    return true;
  }
}
