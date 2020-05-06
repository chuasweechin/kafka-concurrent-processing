using System;
using System.Linq;
using Confluent.Kafka;
using System.Threading;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaConcurrentProcessing
{
	public class Program
	{
		static void Main(string[] args)
		{
			ServiceCollection serviceCollection = new ServiceCollection();

			//serviceCollection.AddDbContext<EmployeeDbContext>(options =>
			//{
			//	options.UseNpgsql("Host=localhost;Port=5432;Database=employeedb;Username=chuasweechin");
			//});

			serviceCollection.AddDbContextPool<EmployeeDbContext>(options =>
				options.UseSqlServer("Server=localhost,1433\\Catalog=Employee;Database=EmployeeDB;User=sa;Password=Winter2019")
			);

			var serviceProvider = serviceCollection.BuildServiceProvider();
			EmployeeDbContext context = serviceProvider.GetService<EmployeeDbContext>();

			//Get(context);
			KafkaLongRunningProcessTest(context, args[0]);
		}

		static Employee Get(EmployeeDbContext context)
		{
			Employee employee = context.Employees.Find("h701-oajs9-02910");

			if (employee == null)
			{
				employee = Add(context);
			}

			return employee;
		}

		static Employee Add(EmployeeDbContext context)
		{
			Employee employee = new Employee
			{
				Id = "h701-oajs9-02910",
				Name = "Banana",
				Department = "SAP",
				Email = "Banana@test.com"
			};

			context.Employees.Add(employee);
			context.SaveChanges();

			return employee;
		}

		static void Update(EmployeeDbContext context, Employee employeeChanges, string arg)
		{
			employeeChanges.Department = arg;

			var employee = context.Employees.Attach(employeeChanges);
			employee.State = EntityState.Modified;

			context.SaveChanges();
		}

		static void KafkaLongRunningProcessTest(EmployeeDbContext context, string arg)
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
				//MaxPollIntervalMs = 12000,
				//AutoCommitIntervalMs = 3000
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
					ConsumeResult<Ignore,string> result = null;

					while (true)
					{
						try
						{
							result = consumer.Consume(cts.Token);

							Employee employee = Get(context);
							
							Console.WriteLine(
								$"Consumed message '{ result.Message.Value }' at: { DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss,fff") }'."
							);

							// Test 100 seconds, 1 minute 40 seconds - works
							// Test 200 seconds, 3 minutes 20 seconds - works
							// Test 400 seconds, 6 minutes 40 seconds - Hit error: Application maximum poll interval (300000ms)
							// exceeded by 98ms (adjust max.poll.interval.ms for long-running message processing): leaving group
							// Resolve by setting MaxPollIntervalMs = 86400000
							// Test 800 seconds, 13 minutes 20 seconds - works
							// Test 3000 seconds, 50 minutes - works
							Thread.Sleep(TimeSpan.FromMilliseconds(30000));
							Update(context, employee, arg);

							//consumer.Commit(result); // sync commit will failed if consumer leave group 
							consumer.StoreOffset(result); // async commit with background thread

							Console.WriteLine(
								$"Commit: { DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss,fff") }"
							);
						}
						catch (ConsumeException ex)
						{
							Console.WriteLine($"Error occured: { ex.Error.Reason }");
						}
						catch (TopicPartitionException ex)
						{
							Console.WriteLine($"Commit error: { ex.Error.Reason }");
						}
						catch (KafkaException ex)
						{
							Console.WriteLine($"Commit error: { ex.Error.Reason }");
						}
						catch (DbUpdateConcurrencyException ex)
						{
							consumer.StoreOffset(result);
							ex.Entries.Single().Reload();
							Console.WriteLine($"DbUpdateConcurrencyException error: { ex.Message }");
						}
						catch (Exception ex)
						{
							Console.WriteLine($"General error: { ex.Message }");
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
