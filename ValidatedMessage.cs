using Confluent.Kafka;

public readonly struct ValidatedMessage
{
    public readonly ValidationOutcome Outcome;
    public readonly OrderEvent? Typed;       // null unless Outcome == Valid
    public readonly TopicPartitionOffset Tpo;
    public readonly string? Reason;          // populated for dead-lettering
 
    public ValidatedMessage(ValidationOutcome outcome, OrderEvent? typed, TopicPartitionOffset tpo, string? reason)
    {
        Outcome = outcome;
        Typed = typed;
        Tpo = tpo;
        Reason = reason;
    }
}