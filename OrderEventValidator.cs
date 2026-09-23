using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

/// <summary>
/// Validates + deserializes in one pass directly over the pooled byte span.
/// No intermediate JsonDocument / JsonNode tree is materialized.
/// </summary>
public sealed class OrderEventValidator
{
    private readonly ObjectPool<OrderEvent> _pool;
 
    public OrderEventValidator(ObjectPool<OrderEvent> pool) => _pool = pool;
 
    public ValidatedMessage Validate(ReadOnlySpan<byte> payload, TopicPartitionOffset tpo)
    {
        var reader = new Utf8JsonReader(payload, isFinalBlock: true, state: default);
        var target = _pool.Get();
 
        bool sawOrderId = false, sawCustomerId = false, sawAmount = false, sawTs = false;
 
        try
        {
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                return Reject(ValidationOutcome.MalformedPayload, "expected JSON object", target, tpo);
 
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                if (reader.TokenType != JsonTokenType.PropertyName)
                    return Reject(ValidationOutcome.MalformedPayload, "expected property name", target, tpo);
 
                if (reader.ValueTextEquals("orderId"))
                {
                    reader.Read();
                    if (!reader.TryGetGuid(out target.OrderId))
                        return Reject(ValidationOutcome.SchemaViolation, "orderId not a GUID", target, tpo);
                    sawOrderId = true;
                }
                else if (reader.ValueTextEquals("customerId"))
                {
                    reader.Read();
                    target.CustomerId = reader.GetString() ?? string.Empty;
                    sawCustomerId = target.CustomerId.Length > 0;
                }
                else if (reader.ValueTextEquals("amount"))
                {
                    reader.Read();
                    if (!reader.TryGetDecimal(out target.Amount) || target.Amount < 0)
                        return Reject(ValidationOutcome.SchemaViolation, "amount invalid or negative", target, tpo);
                    sawAmount = true;
                }
                else if (reader.ValueTextEquals("timestampMs"))
                {
                    reader.Read();
                    if (!reader.TryGetInt64(out target.TimestampUnixMs))
                        return Reject(ValidationOutcome.SchemaViolation, "timestampMs invalid", target, tpo);
                    sawTs = true;
                }
                else
                {
                    reader.Skip(); // unknown field — forward-compatible, ignore rather than reject
                }
            }
 
            if (!(sawOrderId && sawCustomerId && sawAmount && sawTs))
                return Reject(ValidationOutcome.SchemaViolation, "missing required field(s)", target, tpo);
 
            return new ValidatedMessage(ValidationOutcome.Valid, target, tpo, reason: null);
        }
        catch (JsonException ex)
        {
            return Reject(ValidationOutcome.MalformedPayload, ex.Message, target, tpo);
        }
    }
 
    private ValidatedMessage Reject(ValidationOutcome outcome, string reason, OrderEvent unused, TopicPartitionOffset tpo)
    {
        _pool.Return(unused); // give the (partially populated but about-to-be-reset) instance back immediately
        return new ValidatedMessage(outcome, null, tpo, reason);
    }
}