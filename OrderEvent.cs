/// <summary>
/// Mutable, poolable domain type. Reused across messages — never "new"'d
/// per message on the hot path.
/// </summary>
public sealed class OrderEvent
{
    public Guid OrderId;
    public string CustomerId = string.Empty; // reused string field, overwritten not reallocated where possible
    public decimal Amount;
    public long TimestampUnixMs;
 
    public void Reset()
    {
        OrderId = default;
        CustomerId = string.Empty;
        Amount = default;
        TimestampUnixMs = default;
    }
}