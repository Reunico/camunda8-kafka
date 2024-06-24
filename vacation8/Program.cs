using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using Zeebe.Client;
using Zeebe.Client.Api.Responses;
using Zeebe.Client.Api.Worker;
using Zeebe.Client.Impl.Builder;
using Microsoft.Extensions.Configuration;

namespace vacation8;

class Program
{
    private static String clientID;
    private static String clientSecret;
    private static String clusterURL;
    private static IZeebeClient zeebeClient;
    private static readonly String bpmnFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "../../../Resources", "vacation8.bpmn");
    private static readonly String jobType = "put";
    private static readonly String producerTopic = "vacation";
    private static readonly String kafkaServer = "localhost:9092";
    private static ProducerConfig producerConfig;
    private static IProducer<string, string> producer;
    private static readonly string WorkerName = Environment.MachineName;

    private static async Task Main(string[] args)
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

        producerConfig = new ProducerConfig
        {
            // User-specific properties that you must set
            BootstrapServers = kafkaServer,

            // Fixed properties
            Acks = Acks.All
        };

        producer = new ProducerBuilder<string, string>(producerConfig).Build();

        // Deploy Process and start and Instance
        string bpmnProcessId = await DeployProcess(bpmnFile);
        long processInstanceKey = await StartProcessInstance(bpmnProcessId);

        // Starting the Job Worker
        using (var signal = new EventWaitHandle(false, EventResetMode.AutoReset))
        {
            zeebeClient.NewWorker()
                .JobType(jobType)
                .Handler(JobHandler)
                .MaxJobsActive(5)
                .Name(WorkerName)
                .PollInterval(TimeSpan.FromSeconds(1))
                .Timeout(TimeSpan.FromSeconds(10))
                .Open();

            signal.WaitOne();
        }

    }

    /// <summary>
    /// Starting a Process Instance on Camunda 8 SaaS
    /// </summary>
    /// <param name=bpmnProcessId">Key of the Process Definition</param>
    private async static Task<long> StartProcessInstance(string bpmnProcessId)
    {
        var processInstanceResponse = await zeebeClient.NewCreateProcessInstanceCommand()
            .BpmnProcessId(bpmnProcessId)
            .LatestVersion()
            .Send();

        Console.WriteLine("Process Instance has been started!");

        var processInstanceKey = processInstanceResponse.ProcessInstanceKey;
        return processInstanceKey;
    }

    /// <summary>
    /// Deploying a BPMN Process Model to Camunda 8 SaaS
    /// </summary>
    /// <param name=processDefinitionKey">Key of the Process Definition</param>
    private async static Task<string> DeployProcess(String bpmnFile)
    {
        var deployRespone = await zeebeClient.NewDeployCommand()
            .AddResourceFile(bpmnFile)
            .Send();
        Console.WriteLine("Process Definition has been deployed!");

        var bpmnProcessId = deployRespone.Processes[0].BpmnProcessId;
        return bpmnProcessId;
    }

    /// <summary>
    /// Business Logic
    /// </summary>
    /// <param name=jobClient">The client with access to all job-related operation</param>
    /// <param name=job">Job Object</param>
    private static void JobHandler(IJobClient jobClient, IJob job)
    {
        JObject jsonObject = JObject.Parse(job.Variables);
        string key = (string)jsonObject["key"];
        string value = (string)jsonObject["value"];

        JObject customHeaders = JObject.Parse(job.CustomHeaders);
        string messageType = (string)customHeaders["messageType"];

        Console.WriteLine($"messageType: {messageType}");

        Console.WriteLine("Working on Task");

        
        Message<string, string> message = new()
        {
            Key = messageType,
            Value = job.Key.ToString()
        };

        producer.Produce(producerTopic, message, DeliveryReport(message));

        producer.Flush(TimeSpan.FromSeconds(10));

        Console.WriteLine($"message was produced to topic {producerTopic}");

    }

    private static Action<DeliveryReport<string, string>> DeliveryReport(Message<string, string> message)
    {
        return (deliveryReport) =>
        {
            if (deliveryReport.Error.Code != ErrorCode.NoError)
            {
                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
            }
            else
            {
                Console.WriteLine($"Produced event to topic {producerTopic}: key = {message.Key} value = {message.Value}");
            }
        };
    }
}
