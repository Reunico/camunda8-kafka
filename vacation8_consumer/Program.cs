using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Zeebe.Client;
using Zeebe.Client.Impl.Builder;

namespace vacation8_consumer;

class Program
{
    
    private static String clientID;
    private static String clientSecret;
    private static String clusterURL;
    private static IZeebeClient zeebeClient;
    private static readonly String kafkaServer = "localhost:9092";
    private static ConsumerConfig consumerConfig;
    private static IConsumer<string, string> consumer;
    private static readonly String producerTopic = "vacation";

    static async Task Main(string[] args)
    {
        var builder = new ConfigurationBuilder().AddJsonFile($"appsettings.json", true, true);

        var config = builder.Build();

        clientID = config["clientID"];
        clientSecret = config["clientSecret"];
        clusterURL = config["clusterURL"];

        zeebeClient = CamundaCloudClientBuilder
                  .Builder()
                  .UseClientId(clientID)
                  .UseClientSecret(clientSecret)
                  .UseContactPoint(clusterURL)
                  .Build();
        
        var topology = await zeebeClient.TopologyRequest().Send();
        
        Console.WriteLine(topology);

        consumerConfig = new ConsumerConfig
        {
            // User-specific properties that you must set
            BootstrapServers = kafkaServer,

            // Fixed properties
            GroupId         = "kafka-dotnet-getting-started",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        CancellationTokenSource cts = new();

        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        consumer.Subscribe(producerTopic);
        try
        {
            while (true)
            {
                var cr = consumer.Consume(cts.Token);

                Console.WriteLine($"Consumed event from topic {producerTopic}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");

                var jobKey = long.Parse(cr.Message.Value);

                try
                {
                    zeebeClient.NewCompleteJobCommand(jobKey)
                        .Send()
                        .GetAwaiter()
                        .GetResult();    
                    
                    Console.WriteLine("Completed the fetched Task");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
                
            }
        }
        catch (OperationCanceledException)
        {
            // Ctrl-C was pressed.
        }
        finally
        {
            consumer.Close();
        }

    }
}
