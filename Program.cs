using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using BencodeLibrary;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;

namespace DHTCrawler
{
    class Program
    {
        static public TimeSpan MAX_SYNC_WAIT = new TimeSpan(20000000); // 10000 = 1ms
        static public TimeSpan MAX_WAIT_AFTER_IDLE = new TimeSpan(60000000);//new TimeSpan(60000000);
        static public int MAX_STARTING_NODES; // 1500000
        static public int MAX_PACKETS_PER_SECOND_BUCKET_MANAGER = 10000; // Only sending // 4000
        static public int MAX_PACKETS_PER_SECOND_DHT_NODE = 10000; //2500 5000
        static public int MAX_INFOHASHES_PER_NODE = 100; // Must be below 65536 for 2 byte conversation IDs!
        static public int DHT_BUCKETS = 65536;
        static public int MAX_DHT_NODES;
        static public int MAX_BUCKET_RADIUS = 1; // default 1 for 1.5m
        static public int MAX_PASSES = 3;
        static public string DHT_FILE_NAME = "DHT_NODES.dat";
        static public int MAX_CONTACT_NODES = 100;
        static public int MAX_GET_PEERS_BURST = 100;
        static public int WAIT_BETWEEN_NODE_COLLECTION_CYCLES = 0; // 1200
        static public MySQL mySQL = new MySQL();
        static public bool debug = true;
        static public IPAddress bootstrapIP = IPAddress.Parse("67.215.246.10"); // router.bittorrent.com:6881
        static public IPEndPoint bootstrapAdr = new IPEndPoint(bootstrapIP, 6881);
        static private List<Thread> threads;
        public static string dbLock = "a";

        static void Main(string[] args)
        {
            threads = new List<Thread>();

            // Get number of threads
            Console.Write("Enter number of sending threads (default 4): ");
            string input = Console.ReadLine();
            bool result = Int32.TryParse(input, out MAX_DHT_NODES);
            if (!result)
                MAX_DHT_NODES = 4;

            // Get number of packets per second sending
            Console.Write("Enter number of packets send per second for each thread (default 10000): ");
            input = Console.ReadLine();
            result = Int32.TryParse(input, out MAX_PACKETS_PER_SECOND_DHT_NODE);
            if (!result)
                MAX_PACKETS_PER_SECOND_DHT_NODE = 10000;

            // Get number of starting nodes per thread
            Console.Write("Enter number of starting nodes per thread (default 1.5 million): ");
            input = Console.ReadLine();
            result = Int32.TryParse(input, out MAX_STARTING_NODES);
            if (!result)
                MAX_STARTING_NODES = 1500000; /* 1500000*/

            // Get number of starting nodes per thread
            Console.Write("Enter number of infohashes queried per loop (default 20 - determines size of SQL queries): ");
            input = Console.ReadLine();
            result = Int32.TryParse(input, out MAX_INFOHASHES_PER_NODE);
            if (!result)
                MAX_INFOHASHES_PER_NODE = 20; /* 1500000*/

            // Get bucket radius
            Console.Write("Enter number of neighbor buckets to check in each direction (default 1): ");
            input = Console.ReadLine();
            result = Int32.TryParse(input, out MAX_BUCKET_RADIUS);
            if (!result)
                MAX_BUCKET_RADIUS = 1;

            // Get number of passes
            Console.Write("Enter number of times to ask all nodes (default 3): ");
            input = Console.ReadLine();
            result = Int32.TryParse(input, out MAX_PASSES);
            if (!result)
                MAX_PASSES = 3;


            for (int i = 0; i < MAX_DHT_NODES; ++i)
            {
                DHTNode dhtNode = new DHTNode(i * 2 + 3244);
                Thread child = new Thread(t => { dhtNode.Run(); });
                threads.Add(child);
                child.Start();
            }

            // None of the childs will return
            foreach(Thread t in threads)
               t.Join();
        }
    }
}
