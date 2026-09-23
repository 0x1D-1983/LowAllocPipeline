using Microsoft.Extensions.ObjectPool;

/// <summary>Pool policy: reset on return, so a rented instance is always clean.</summary>
public sealed class OrderEventPooledPolicy : PooledObjectPolicy<OrderEvent>
{
    public override OrderEvent Create() => new();
 
    public override bool Return(OrderEvent obj)
    {
        obj.Reset();
        return true; // keep it in the pool
    }
}