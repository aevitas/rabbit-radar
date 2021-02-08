using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using Slack.Webhooks;

namespace RabbitRadar
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("settings.json", false)
                .AddUserSecrets(Assembly.GetEntryAssembly())
                .Build();
            
            var queues = configuration.GetSection("Queues").Get<List<string>>();
            var messageCounts = queues.ToDictionary(q => q, _ => (uint) 0);
            var connectionFactory = CreateConnectionFactory(configuration);
            var cts = new CancellationTokenSource();

            var runningTask = Task.Run(async () =>
            {
                using var connection = connectionFactory.CreateConnection();
                using var channel = connection.CreateModel();

                foreach (var q in queues)
                    messageCounts[q] = channel.MessageCount(q);
                
                while (!cts.IsCancellationRequested)
                {
                    foreach (var queueName in queues)
                    {
                        var count = channel.MessageCount(queueName);
                        
                        Log($"Queue {queueName} contains {count} messages.");

                        messageCounts.TryGetValue(queueName, out var oldCount);

                        if (count > oldCount)
                        {
                            Log($"{queueName} contains {count - oldCount} more message(s) than before.");

                            if (IsSlackConfigured())
                                await SendWebHookAsync(queueName, oldCount, count);
                        }

                        messageCounts[queueName] = count;
                    }

                    await Task.Delay(TimeSpan.FromMinutes(1), cts.Token);
                }
            }, cts.Token);
            
            Log("Monitoring RabbitMQ. Hit return to stop.");
            Console.ReadLine();

            cts.Cancel();
            try
            {
                await runningTask;
            }
            catch (OperationCanceledException)
            {
            }

            async Task SendWebHookAsync(string queueName, uint oldCount, uint newCount)
            {
                var slackClient = new SlackClient(configuration["Slack:WebHookEndPoint"]);
                var channelName = configuration["Slack:ChannelName"];

                Log($"-- Sending message to channel {channelName}..");

                await slackClient.PostAsync(new SlackMessage
                {
                    Channel = channelName,
                    Text = $"Queue {queueName} contains {newCount} messages, up from {oldCount}.",
                    IconEmoji = Emoji.Ghost
                });
            }
            
            bool IsSlackConfigured() => !string.IsNullOrWhiteSpace(configuration["Slack:WebHookEndPoint"]) &&
                !string.IsNullOrWhiteSpace(configuration["Slack:ChannelName"]);

            static void Log(string message) => Console.WriteLine($"[{DateTimeOffset.Now:g}] {message}");
        }

        private static ConnectionFactory CreateConnectionFactory(IConfiguration configuration)
        {
            var factory = new ConnectionFactory
            {
                HostName = configuration["Server:Host"],
                UserName = configuration["Server:Username"],
                Password = configuration["Server:Password"]
            };

            return factory;
        }
    }
}
