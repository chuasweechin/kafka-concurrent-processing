using System;
using Confluent.Kafka;
using System.Threading;

namespace kafka_concurrent_processing
{
	class Program
	{
		static void Main(string[] args)
		{
			Console.WriteLine("Hello World!");
		}

		static void KafkaLongRunningProcessTest()
		{
			var topic = "SampleEvent";

			var conf = new ConsumerConfig
			{
				GroupId = "sample-event-consumer-group-2",
				BootstrapServers = "localhost:9092",
				AutoOffsetReset = AutoOffsetReset.Earliest,
				EnableAutoCommit = true,
				EnableAutoOffsetStore = false,
				MaxPollIntervalMs = 86400000
			};

			using (var consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
			{
				consumer.Subscribe(topic);

				CancellationTokenSource cts = new CancellationTokenSource();

				Console.CancelKeyPress += (_, e) =>
				{
					e.Cancel = true;
					cts.Cancel();
				};

				try
				{
					while (true)
					{
						try
						{
							var result = consumer.Consume(cts.Token);

							Console.WriteLine(
								$"Consumed message '{ result.Message.Value }' at: { DateTime.Now }'."
							);

							// Test 100 seconds, 1 minute 40 seconds - works
							// Test 200 seconds, 3 minutes 20 seconds - works
							// Test 400 seconds, 6 minutes 40 seconds - Hit error: Application maximum poll interval (300000ms)
							// exceeded by 98ms (adjust max.poll.interval.ms for long-running message processing): leaving group
							// Resolve by setting MaxPollIntervalMs = 86400000
							// Test 800 seconds, 13 minutes 20 seconds - works
							// Test 3000 seconds, 50 minutes - works
							Thread.Sleep(TimeSpan.FromMilliseconds(20000));
							consumer.StoreOffset(result);

							Console.WriteLine($"Commit: { DateTime.Now }");
						}
						catch (ConsumeException ex)
						{
							Console.WriteLine($"Error occured: { ex.Error.Reason }");
						}
						catch (TopicPartitionException ex)
						{
							Console.WriteLine($"Commit error: {ex.Error.Reason}");
						}
						catch (KafkaException ex)
						{
							Console.WriteLine($"Commit error: {ex.Error.Reason}");
						}
						catch (Exception ex)
						{
							Console.WriteLine($"General error: {ex.Message}");
						}
					}
				}
				catch (OperationCanceledException)
				{
					// Ensure the consumer leaves the group cleanly and final offsets are committed.
					consumer.Close();
				}
				finally
				{
					consumer.Close();
				}
			}
		}

	}
}
