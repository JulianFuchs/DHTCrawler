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
using System.Collections;
using System.Diagnostics;

namespace DHTCrawler
{
    /// <summary>
    /// Holds IP/Port/NodeID of all/most nodes in the DHT network
    /// Needs to be refreshed occasionally, could also be refreshed using get peer request answers.
    /// </summary>
    class DHTBucketManager
    {
        // List of all DHT nodes, could be extended with a timestamp
        public List<HashSet<Tuple<string, IPEndPoint>>> lDHTNodes;
        public List<string> locks = Enumerable.Repeat("x", Program.DHT_BUCKETS).ToList();
        public int nofNodes = 0;
        private int countRecvPeerPackets;
        private int countRecvNodePackets;
        private int countRecvQuery;
        private int getPeerReqSent;
        private int localPort;
        private string infoHHex;
        private string nodeIDHex;
        private string conversationID;
        private static readonly Random random = new Random();
        private Thread receiveThread;
        private IPEndPoint receiveFrom;
        private UdpClient udpClientReceive;
        private UdpClient udpClientSend;
        private Socket s;
        private const int SIO_UDP_CONNRESET = -1744830452;
        
        /// <summary>
        /// Initializes lDHTNodes with starting nodes, the list is loaded from disk if available
        /// </summary>
        /// <param name="bootstrapAdr"></param>
        /// <param name="localport"></param>
        public DHTBucketManager(IPEndPoint bootstrapAdr, int localport)
        {
            localPort = localport;

            // Read starting nodes from disk - slower than getting them from network!
            //LoadFromDiskFormatter();
            //LoadFromDiskCustom();
            if (lDHTNodes == null)
            {
                lDHTNodes = new List<HashSet<Tuple<string, IPEndPoint>>>();
                for (int i = 0; i < Program.DHT_BUCKETS; i++)
                {
                    HashSet<Tuple<string, IPEndPoint>> hs = new HashSet<Tuple<string, IPEndPoint>>();
                    lDHTNodes.Add(hs);
                }
            }
            Console.WriteLine("Thread " + Thread.CurrentThread.ManagedThreadId+": Getting random starting nodes from the network!");
            for (int c1 = 0; c1 < lDHTNodes.Count; c1++)
            {
                nofNodes += lDHTNodes[c1].Count;
            }
            udpClientReceive = new UdpClient();
            udpClientReceive.Client.ReceiveBufferSize = 10000000; // in bytes
            udpClientReceive.Client.IOControl((IOControlCode)SIO_UDP_CONNRESET, new byte[] { 0, 0, 0, 0 }, null);
            udpClientReceive.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            udpClientReceive.Client.Ttl = 255;
            udpClientReceive.Client.Bind(new IPEndPoint(IPAddress.Any, localPort));

            s = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            s.Bind(new IPEndPoint(IPAddress.Any, localPort));

            udpClientSend = new UdpClient();
            udpClientSend.Client.SendBufferSize = 10000000;
            udpClientSend.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            udpClientSend.Client.Ttl = 255;
            udpClientSend.Client.Bind(new IPEndPoint(IPAddress.Any, localPort));

            receiveThread = new Thread(new ThreadStart(ReceivePackets));
            receiveThread.IsBackground = true;
            receiveThread.Start();

            nodeIDHex = getRandomID();
            conversationID = getRandomConvID();

            // Get some random nodes from the bootstrap nodes
            while (nofNodes < Program.MAX_CONTACT_NODES)
            {
                infoHHex = getRandomID();
                GetPeers(bootstrapAdr, infoHHex);
                getPeerReqSent++;
                Wait();
            }
            DateTime startTime = DateTime.Now;
            DateTime endTime; 

            // Continually query all nodes for random infohashes
            int cycleCount = 1;
            while (nofNodes < Program.MAX_STARTING_NODES)
            {
                for (int i = 0; i < Program.DHT_BUCKETS && nofNodes < Program.MAX_STARTING_NODES; i++)
                {
                    endTime = DateTime.Now;
                    TimeSpan span = endTime.Subtract(startTime);
                    // Perdiodically display node count
                    if (span.Seconds >= 10)
                    {
                        Console.WriteLine("Thread " + Thread.CurrentThread.ManagedThreadId + ": {0}/{1}", nofNodes, Program.MAX_STARTING_NODES);
                        startTime = DateTime.Now;
                    }
                    Stopwatch stopwatch = new Stopwatch();
                    double getPeerReqSentCurrentTimespan = 0;
                    lock (locks[i])
                    {
                        stopwatch.Start();

                        //Stopwatch sw = new Stopwatch();
                        //sw.Start();
                        foreach (Tuple<string, IPEndPoint> t in lDHTNodes[i])
                        {
                            GetPeersBurst(t.Item2, infoHHex);
                            getPeerReqSent += Program.MAX_GET_PEERS_BURST;
                            getPeerReqSentCurrentTimespan += Program.MAX_GET_PEERS_BURST;
                        }
                        //Console.writeline(sw.elapsedmilliseconds * 8);
                        //sw.stop();
                    }

                    while ((1000*getPeerReqSentCurrentTimespan)/ (1+(double)stopwatch.ElapsedMilliseconds) > Program.MAX_PACKETS_PER_SECOND_BUCKET_MANAGER) { }
                    stopwatch.Stop();

                    // Don't kill gateways with too many UDP connections
                    if(getPeerReqSent > cycleCount*5000*Program.MAX_GET_PEERS_BURST)
                    {
                        Wait(Program.WAIT_BETWEEN_NODE_COLLECTION_CYCLES);
                        cycleCount++;
                    }
                }
            }
            Console.WriteLine("Thread " + Thread.CurrentThread.ManagedThreadId + ": {0}/{1}", nofNodes, Program.MAX_STARTING_NODES);
            Wait(3000);
            receiveThread.Abort();

            //Log("Saving to " + Program.DHT_FILE_NAME);
            //stream = new FileStream(@Program.DHT_FILE_NAME, FileMode.Open);
            //formatter.Serialize(stream, lDHTNodes);
            //stream.Close();
        }
        private void LoadFromDiskCustom()
        {
            FileStream stream;

            if (!File.Exists(Program.DHT_FILE_NAME))
            {
                lDHTNodes = new List<HashSet<Tuple<string, IPEndPoint>>>();
                for (int i = 0; i < Program.DHT_BUCKETS; i++)
                {
                    HashSet<Tuple<string, IPEndPoint>> hs = new HashSet<Tuple<string, IPEndPoint>>();
                    lDHTNodes.Add(hs);
                }
                stream = new FileStream(@Program.DHT_FILE_NAME, FileMode.Create);
                stream.Close();
                Console.WriteLine("Created " + Program.DHT_FILE_NAME + "!");
            }
            else
            {
                Console.WriteLine("Reading from " + Program.DHT_FILE_NAME + "!");
            }
            string[] readText = File.ReadAllLines(@Program.DHT_FILE_NAME);
        }
        private void LoadFromDiskFormatter()
        {
            FileStream stream;
            BinaryFormatter formatter = new BinaryFormatter();

            if (!File.Exists(Program.DHT_FILE_NAME))
            {
                lDHTNodes = new List<HashSet<Tuple<string, IPEndPoint>>>();
                for (int i = 0; i < Program.DHT_BUCKETS; i++)
                {
                    HashSet<Tuple<string, IPEndPoint>> hs = new HashSet<Tuple<string, IPEndPoint>>();
                    lDHTNodes.Add(hs);
                }
                stream = new FileStream(@Program.DHT_FILE_NAME, FileMode.Create);
                formatter.Serialize(stream, lDHTNodes);
                stream.Close();
                Console.WriteLine("Created " + Program.DHT_FILE_NAME + "!");
            }
            else
            {
                Console.WriteLine("Reading from " + Program.DHT_FILE_NAME + "!");
            }
            stream = new FileStream(@Program.DHT_FILE_NAME, FileMode.Open);
            formatter = new BinaryFormatter();
            lDHTNodes = formatter.Deserialize(stream) as List<HashSet<Tuple<string, IPEndPoint>>>;
            stream.Close();
        }
        private void GetPeers(IPEndPoint toAdr, string infohash)
        {
            infoHHex = getRandomID(); 
            string getPeersMsg = "d1:ad2:id20:" + HexSToString(nodeIDHex) + "9:info_hash20:" + HexSToString(infohash) + "e1:q9:get_peers1:t2:" + HexSToString(conversationID) + "1:v4:abcd1:y1:qe";
            //Log("Sent GetPeers request to " + toAdr);
            SendMessage(toAdr, getPeersMsg); 
        }
        private void GetPeersBurst(IPEndPoint toAdr, string infohash)
        {
            for (int i = 0; i < Program.MAX_GET_PEERS_BURST; i++)
            {
                infoHHex = getRandomID();
                GetPeers(toAdr, infoHHex);
            }
        }
        private void SendMessage(IPEndPoint destination, string message)
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            byte[] send_buffer = BencodingUtils.ExtendedASCIIEncoding.GetBytes(message);
            try
            {
                // Poor performance when remote host address changes between individual packets! OS limitation?
                //udpClientSend.SendAsync(send_buffer, send_buffer.Length,new IPEndPoint(IPAddress.Parse("123.123.123.123"),2323));// destination);
               // udpClientSend.SendAsync(send_buffer, send_buffer.Length, destination);// destination);
                udpClientSend.Send(send_buffer,send_buffer.Length, destination);
                //Log(message);
            }
            catch (Exception ex)
            {
            }
            //Console.WriteLine(sw.ElapsedMilliseconds);
            //sw.Stop();
        }
        /// <summary>
        /// Convert string in hex to string - assumes length of string is 2n
        /// </summary>
        /// <param name="hexNodeID"></param>
        /// <returns></returns>
        private string HexSToString(string hexString)
        {
            int length = hexString.Length / 2;
            byte[] arr = new byte[length];
            for (int i = 0; i < length; i++)
            {
                arr[i] = Convert.ToByte(hexString.Substring(2 * i, 2), 16);
            }
            return BencodingUtils.ExtendedASCIIEncoding.GetString(arr);
        }
        /// <summary>
        /// Updates the list of contact nodes with closer nodes
        /// </summary>
        /// <param name="nodeIpPortString"></param>
        private void UpdateContactList(BString nodeIpPortString)
        {
            byte[] arr = BencodingUtils.ExtendedASCIIEncoding.GetBytes(nodeIpPortString.Value);
            byte[] ip = new byte[4];
            byte[] port = new byte[2];
            byte[] nodeID = new byte[20];
            byte[] currLock;
            int bucketNr;
            for (int i = 0; i < arr.Length / 26; i++)
            {
                currLock = new byte[4] { 0, 0, 0, 0 };
                Array.Copy(arr, i * 26, nodeID, 0, 20);
                Array.Copy(arr, i * 26, currLock, 2, 2);
                Array.Copy(arr, i * 26 + 20, ip, 0, 4);
                Array.Copy(arr, i * 26 + 24, port, 0, 2);
                Array.Reverse(currLock);
                bucketNr = BitConverter.ToInt32(currLock, 0);
                Array.Reverse(port);
                IPEndPoint ipEndP = new IPEndPoint((Int64)BitConverter.ToUInt32(ip, 0), (Int32)BitConverter.ToUInt16(port, 0));
                string sNodeID = ByteArrayToHexString(nodeID);

                if ((Int64)BitConverter.ToUInt32(ip, 0) != 0 && (Int32)BitConverter.ToUInt16(port, 0) != 0)
                {
                    lock (locks[bucketNr])
                    {
                        if (!lDHTNodes[bucketNr].Contains(Tuple.Create(sNodeID, ipEndP)))
                        {
                            lDHTNodes[bucketNr].Add(Tuple.Create(sNodeID, ipEndP));
                            nofNodes++;
                        }
                    }
                }
            }
        }
        /// <summary>
        /// Background thread handling all incoming traffic
        /// </summary>
        private void ReceivePackets()
        {
            while (true)
            {
                try
                {
                    // Get a datagram
                    receiveFrom = new IPEndPoint(IPAddress.Any, localPort);
                    // Stopwatch sw = new Stopwatch();
                    // sw.Start();
                    byte[] data = udpClientReceive.Receive(ref receiveFrom);
                    //if (sw.ElapsedMilliseconds > 10) { Console.WriteLine(sw.ElapsedMilliseconds); }
                    //sw.Stop();

                    // Decode the message
                    Stream stream = new MemoryStream(data);
                    IBencodingType receivedMsg = BencodingUtils.Decode(stream);
                    string decoded = BencodingUtils.ExtendedASCIIEncoding.GetString(data.ToArray());
                    //Log("Received message!");

                    // t is transaction id
                    // y is e for error, r for reply, q for query
                    if (receivedMsg is BDict) // throws error.. todo: fix
                    {
                        BDict dictMsg = (BDict)receivedMsg;
                        if (dictMsg.ContainsKey("y"))
                        {
                            if (dictMsg["y"].Equals(new BString("e")))
                            {
                                //Log("Received error! (ignored)");
                            }
                            else if (dictMsg["y"].Equals(new BString("r")))
                            {
                                // received reply
                                if (dictMsg.ContainsKey("r"))
                                {
                                    if (dictMsg["r"] is BDict)
                                    {
                                        BDict dictMsg2 = (BDict)dictMsg["r"];
                                        if (dictMsg2.ContainsKey("values"))
                                        {
                                            //Log("Received list of peers for torrent!");
                                            countRecvPeerPackets++;
                                        }
                                        else if (dictMsg2.ContainsKey("nodes"))
                                        {
                                            // could be an answer to find node or get peers
                                            //Log("Received list of nodeID & IP & port!");
                                            countRecvNodePackets++;
                                            BString nodeIDString = (BString)dictMsg2["nodes"];
                                            UpdateContactList(nodeIDString);
                                        }
                                        else
                                        {
                                            // no values and no nodes, assuming its a ping,
                                            // at least some form of response
                                            
                                        }
                                    }
                                    else
                                    {
                                    }
                                }
                            }
                            else if (dictMsg["y"].Equals(new BString("q")))
                            {
                                // received query
                                countRecvQuery++;
                                //Log("Received query! (ignored)");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    //Log("Error receiving data: " + ex.ToString());
                }
            }
        }
        /// <summary>
        /// Checks whether A is closer to C than B using the XOR metric
        /// </summary>
        /// <param name="nodeIDA"></param>
        /// <param name="nodeIDB"></param>
        /// <param name="nodeIDC"></param>
        /// <returns></returns>
        private bool isAcloserToCThanB(BString nodeIDA, BString nodeIDB, BString nodeIDC)
        {
            byte[] arrA = BencodingUtils.ExtendedASCIIEncoding.GetBytes(nodeIDA.Value);
            byte[] arrB = BencodingUtils.ExtendedASCIIEncoding.GetBytes(nodeIDB.Value);
            byte[] arrC = BencodingUtils.ExtendedASCIIEncoding.GetBytes(nodeIDC.Value);
            bool[] bitsA = arrA.SelectMany(GetBits).ToArray();
            bool[] bitsB = arrB.SelectMany(GetBits).ToArray();
            bool[] bitsC = arrC.SelectMany(GetBits).ToArray();
            for (int i = bitsA.Length - 1; i >= 0; i--)
            {
                if (bitsA[i] ^ bitsC[i] == true && bitsB[i] ^ bitsC[i] == false) { return false; }
                else if (bitsA[i] ^ bitsC[i] == false && bitsB[i] ^ bitsC[i] == true) { return true; }
            }
            return false;
        }
        /// <summary>
        /// Checks whether A is closer to C than B using the XOR metric
        /// </summary>
        /// <param name="nodeIDA"></param>
        /// <param name="nodeIDB"></param>
        /// <param name="nodeIDC"></param>
        /// <returns></returns>
        private bool isAcloserToCThanB(string nodeIDA, string nodeIDB, string nodeIDC)
        {
            byte[] arrA = Encoding.ASCII.GetBytes(nodeIDA);
            byte[] arrB = Encoding.ASCII.GetBytes(nodeIDB);
            byte[] arrC = Encoding.ASCII.GetBytes(nodeIDC);
            bool[] bitsA = arrA.SelectMany(GetBits).ToArray();
            bool[] bitsB = arrB.SelectMany(GetBits).ToArray();
            bool[] bitsC = arrC.SelectMany(GetBits).ToArray();
            for (int i = bitsA.Length - 1; i >= 0; i--)
            {
                if (bitsA[i] ^ bitsC[i] == true && bitsB[i] ^ bitsC[i] == false) { return false; }
                else if (bitsA[i] ^ bitsC[i] == false && bitsB[i] ^ bitsC[i] == true) { return true; }
            }
            return false;
        }
        private string getRandomID()
        {
            byte[] b = new byte[20];
            random.NextBytes(b);
            return ByteArrayToHexString(b);
        }
        private string getRandomConvID()
        {
            byte[] b = new byte[2];
            random.NextBytes(b);
            return ByteArrayToHexString(b);
        }
        IEnumerable<bool> GetBits(byte b)
        {
            for (int i = 0; i < 8; i++)
            {
                yield return (b & 0x80) != 0;
                b *= 2;
            }
        }
        /// <summary>
        /// Converts nodeID from byte array to displayable hex string
        /// </summary>
        /// <param name="arr"></param>
        /// <returns></returns>
        private string ByteArrayToHexString(byte[] arr)
        {
            string hex = BitConverter.ToString(arr);
            return hex.Replace("-", "");
        }
        private void Log(string message)
        {
            if (Program.debug)
                Console.WriteLine(message);
        }
        /// <summary>
        /// Wait for specified ms
        /// </summary>
        /// <param name="ms"></param>
        private void Wait(int ms)
        {
            DateTime called = DateTime.Now;
            while (DateTime.Now < called.Add(new TimeSpan(ms * 10000))) { }
        }
        /// <summary>
        /// Wait for Program.MAX_SYNC_WAIT ms
        /// </summary>
        private void Wait()
        {
            DateTime called = DateTime.Now;
            while (DateTime.Now < called.Add(Program.MAX_SYNC_WAIT)) { }
        }
    }
}
