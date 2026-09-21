using Confluent.Kafka;

namespace LowAllocPipeline;

// ---------------------------------------------------------------------------
// 1. A struct-based "envelope" for in-flight messages.
//    Using a struct avoids a heap allocation per message for the wrapper
//    itself. The payload bytes come from ArrayPool, so we also avoid an
//    allocation for the byte[] on every message.
// ---------------------------------------------------------------------------
public readonly struct PooledMessage
{
    public readonly byte[] RentedBuffer;   // rented from ArrayPool<byte>.Shared
    public readonly int Length;            // actual payload length within RentedBuffer
    public readonly TopicPartitionOffset Tpo;

    public PooledMessage(byte[] rentedBuffer, int length, TopicPartitionOffset tpo)
    {
        RentedBuffer = rentedBuffer;
        Length = length;
        Tpo = tpo;
    }

    public ReadOnlySpan<byte> Span => RentedBuffer.AsSpan(0, Length);
}