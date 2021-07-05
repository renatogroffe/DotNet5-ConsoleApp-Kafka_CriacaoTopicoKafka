using System;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace CriacaoTopicoKafka
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("##### Criação de Tópico no Apache Kafka #####");

            if (!ReadStringParameter("Informe o Host (Endereço + Porta)",
                "Host inválido", out string host))
                    return;

            if (!ValidateConnection(host))
                return;

            if (!ReadStringParameter("Informe o Nome do Tópico a ser criado",
                "Nome de Tópico inválido", out string topic))
                    return;

            if (!ReadStringParameter("Informe o Número de Partições",
                "Preencha o Número de Partições", out string particoes))
                    return;
            
            if (!int.TryParse(particoes, out int numParticoes) ||
                numParticoes <= 0)
            {
                PrintStatus("Número de Partições inválido", ConsoleColor.Red);
                return;
            }

            Console.WriteLine();
            CreateTopic(host, topic, numParticoes);
        }

        private static bool ReadStringParameter(string inputMessage,
            string errorMessage, out string parameter)
        {
            Console.WriteLine();
            Console.WriteLine($"{inputMessage}:");
            parameter = Console.ReadLine();
            if (String.IsNullOrWhiteSpace(parameter))
            {
                PrintStatus(errorMessage, ConsoleColor.Red);
                return false;
            }

            return true;
        }

        private static bool ValidateConnection(string host)
        {
            Console.WriteLine();
            Console.WriteLine("***** Tópicos já existentes no Host *****");
            Console.WriteLine();

            try
            {
                using var adminClient = new AdminClientBuilder(
                    new AdminClientConfig { BootstrapServers = host }).Build();
                var topics =adminClient.GetMetadata(TimeSpan.FromSeconds(25)).Topics
                    .Select(t => new { Name = t.Topic, PartitionsCount = t.Partitions.Count })
                    .OrderBy(t => t.Name);

                foreach (var topic in topics)
                    Console.WriteLine($"{topic.Name} = {topic.PartitionsCount} partições");

                return true;
            }
            catch (Exception ex)
            {
                PrintStatus($"Erro durante a conexão com o Host {host}: {ex.Message}",
                    ConsoleColor.Red);
                return false;
            }
        }

        private static void PrintStatus(string message, ConsoleColor colorStatus)
        {
            var previousColor = Console.ForegroundColor;
            Console.ForegroundColor = colorStatus;
            Console.WriteLine($"***** {message} *****");
            Console.ForegroundColor = previousColor;
        } 

        private static void CreateTopic(string host, string topicName, int numPartitions)
        {
            using (var adminClient = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = host }).Build())
            {
                try
                {
                    adminClient.CreateTopicsAsync(new TopicSpecification[] { 
                        new TopicSpecification
                        {
                            Name = topicName,
                            NumPartitions = numPartitions
                        }}).Wait();
                    PrintStatus($"Tópico {topicName} criado com sucesso", ConsoleColor.Cyan);
                }
                catch (Exception ex)
                {
                    PrintStatus($"Erro durante a criação do Tópico {topicName}: {ex.Message}",
                        ConsoleColor.Red);
                }
            }
        }
    }
}