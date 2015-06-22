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
using System.Runtime.InteropServices;

namespace DHTCrawler
{
    /// <summary>
    /// A node in the DHT network
    /// </summary>
    class DHTNode
    {
        private DHTBucketManager dhtBucketM;
        private string nodeIDHex;
        private int infohashCount = 0;
        private int localPort;
        private int nofPeersFound = 0;
        private Thread receiveThread;
        private UdpClient udpClient;
        private List<string> linfoHHex;
        private List<string> linfoHHex2;
        private List<string> linfoHHexForMySQLInsert;
        private List<string> lockContactNodes = Enumerable.Repeat("c", Program.MAX_INFOHASHES_PER_NODE).ToList();
        private List<List<Tuple<int,string, IPEndPoint>>> lContactNodesA;
        private List<List<Tuple<int,string, IPEndPoint>>> lContactNodesB;
        private bool useA = true;
        private List<List<IPEndPoint>> lTorrentPeers;
        private List<List<IPEndPoint>> lTorrentPeersForQueries;
        private List<int> bucketIndices;

        private IPEndPoint receiveFrom;
        private const int SIO_UDP_CONNRESET = -1744830452;

        private int countRecvPeerPackets = 0;
        private int countRecvNodePackets = 0;
        private int countGetPeerReqSent = 0;

        private static readonly Random random = new Random();

        public DHTNode(int localPort)
        {
            this.localPort = localPort;
            useA = true;
            lContactNodesA = new List<List<Tuple<int,string,IPEndPoint>>>();
            lContactNodesB = new List<List<Tuple<int, string,IPEndPoint>>>();
            lTorrentPeers = new List<List<IPEndPoint>>();
            lTorrentPeersForQueries = new List<List<IPEndPoint>>();
            linfoHHex = new List<string>();
            linfoHHex2 = new List<string>();
            bucketIndices = new List<int>();
            
            udpClient = new UdpClient(localPort);
            udpClient.Client.ReceiveBufferSize = 10000; // in bytes
            udpClient.Client.IOControl((IOControlCode)SIO_UDP_CONNRESET, new byte[] { 0, 0, 0, 0 }, null);

            receiveThread = new Thread(new ThreadStart(ReceivePackets));
            receiveThread.IsBackground = true;
        }
        public void Run()
        {
            // Get starting nodes from network
            dhtBucketM = new DHTBucketManager(Program.bootstrapAdr, localPort+1);

            // 1. Get oldest infohashes from DB
            /*
            Console.WriteLine("Thread " + Thread.CurrentThread.ManagedThreadId + ": Fetching " + Program.MAX_INFOHASHES_PER_NODE.ToString() + " new infohashes from DB!");
            InitInfoHashes(false);*/

            while(true)
            {
                // 1. Get oldest infohashes from DB
                Console.WriteLine("Thread " + Thread.CurrentThread.ManagedThreadId + ": Fetching " + Program.MAX_INFOHASHES_PER_NODE.ToString() + " new infohashes from DB!");
                linfoHHex.Clear();
                InitInfoHashes(false);

                infohashCount = linfoHHex.Count;
                //Console.WriteLine("Thread " + Thread.CurrentThread.ManagedThreadId + ": Getting " + Program.MAX_INFOHASHES_PER_NODE.ToString() +
                //    " new infohashes from DB (background thread)!");
                //Thread bg2 = new Thread(o => { InitInfoHashes(true); });
                //bg2.Start();

                // Clear old data            
                nodeIDHex = GetRandomID();
                nofPeersFound = 0;
                bucketIndices.Clear();
                lTorrentPeers.Clear();
                lContactNodesA.Clear();
                lContactNodesB.Clear();

                for (int k = 0; k < Program.MAX_INFOHASHES_PER_NODE; k++)
                {
                    List<IPEndPoint> lt1 = new List<IPEndPoint>();
                    lTorrentPeers.Add(lt1);
                    List<IPEndPoint> lt0 = new List<IPEndPoint>();
                    lTorrentPeersForQueries.Add(lt0);
                    List<Tuple<int, string, IPEndPoint>> lt2 = new List<Tuple<int, string, IPEndPoint>>();
                    lContactNodesA.Add(lt2);
                    List<Tuple<int, string, IPEndPoint>> lt3 = new List<Tuple<int, string, IPEndPoint>>();
                    lContactNodesB.Add(lt3);
                }

                receiveThread = new Thread(new ThreadStart(ReceivePackets));
                receiveThread.IsBackground = true;
                receiveThread.Start();

                // 2. Send get_peers request to all nodes in corresponding buckets for each infohash
                Stopwatch sw = new Stopwatch();

                for (int curr = -Program.MAX_BUCKET_RADIUS; curr < Program.MAX_BUCKET_RADIUS+1; curr++)
                {
                    for (int i = 0; i < infohashCount; i++)
                    {
                        string conversationID = GetConversatioID(i);
                        bucketIndices.Add(GetBucketIndexFromInfohash(linfoHHex[i]));
                        sw.Start();
                        int getPeerReqSentCurrentTimespan = 0;

                        if (bucketIndices[i] + curr < 0) break;
                        if (bucketIndices[i] + curr >= 65536) break;
                        lock (dhtBucketM.locks[bucketIndices[i] + curr])
                        {
                            foreach (Tuple<string, IPEndPoint> t in dhtBucketM.lDHTNodes[bucketIndices[i] + curr])
                            {
                                GetPeers(t.Item2, linfoHHex[i], conversationID);
                                countGetPeerReqSent++; getPeerReqSentCurrentTimespan++;
                            }
                        }

                        while ((1000 * getPeerReqSentCurrentTimespan) / (1 + (double)sw.ElapsedMilliseconds) > Program.MAX_PACKETS_PER_SECOND_DHT_NODE) { }
                        sw.Stop();

                        dhtBucketM.nofNodes = 0;
                        for (int u = 0; u < dhtBucketM.lDHTNodes.Count; u++)
                        {
                            dhtBucketM.nofNodes += dhtBucketM.lDHTNodes[u].Count;
                        }
                    }
                }

                useA = !useA;
                Wait(300);
                receiveThread.Abort();
                Merge();

                 //3. Send get peer requests to all new contact nodes
                for (int passes = 0; passes < 4; passes++)
                {
                    receiveThread = new Thread(new ThreadStart(ReceivePackets));
                    receiveThread.IsBackground = true;
                    receiveThread.Start();

                    // Wait for responses to be processed
                    for (int i = 0; i < infohashCount; i++)
                    {
                        string conversationID = GetConversatioID(i);
                        sw.Start();
                        int getPeerReqSentCurrentTimespan = 0;

                        if (!useA)
                        {
                            foreach (Tuple<int, string, IPEndPoint> t in lContactNodesA[i])
                            {
                                GetPeers(t.Item3, linfoHHex[i], conversationID);
                                countGetPeerReqSent++; getPeerReqSentCurrentTimespan++;
                            }
                            lContactNodesA[i].Clear();
                        }
                        else
                        {
                            foreach (Tuple<int, string, IPEndPoint> t in lContactNodesB[i])
                            {
                                GetPeers(t.Item3, linfoHHex[i], conversationID);
                                countGetPeerReqSent++; getPeerReqSentCurrentTimespan++;
                            }
                            lContactNodesB[i].Clear();
                        }

                        while ((1000 * getPeerReqSentCurrentTimespan) / (1 + (double)sw.ElapsedMilliseconds) > Program.MAX_PACKETS_PER_SECOND_DHT_NODE) { }
                        sw.Stop();

                        dhtBucketM.nofNodes = 0;
                        for (int u = 0; u < dhtBucketM.lDHTNodes.Count; u++)
                        {
                            dhtBucketM.nofNodes += dhtBucketM.lDHTNodes[u].Count;
                        }
                    }
                    useA = !useA;
                    Wait(300);
                    receiveThread.Abort();
                    Merge();
                }
                Console.WriteLine("Thread " + Thread.CurrentThread.ManagedThreadId + ": Nodes/peers found: {0}/{1} for {2} infohashes",
dhtBucketM.nofNodes, nofPeersFound, Program.MAX_INFOHASHES_PER_NODE);

                Wait(5000);
                receiveThread.Abort();
                // Wait for get infohash thread to finish
                //while (bg2.IsAlive) { }
                /*linfoHHexForMySQLInsert = new List<string>(linfoHHex);
                lTorrentPeersForQueries = new List<List<IPEndPoint>>(lTorrentPeers);
                linfoHHex.Clear();
                linfoHHex = new List<string>(linfoHHex2);
                linfoHHex2.Clear();*/
                Console.WriteLine("Thread " + Thread.CurrentThread.ManagedThreadId + ": Inserting new peers into DB !");
                Program.mySQL.InsertPeers(linfoHHex, lTorrentPeers);
                /*Thread bg = new Thread(o => { Program.mySQL.InsertPeers(linfoHHexForMySQLInsert, lTorrentPeersForQueries); });
                bg.Start();*/
            }

            //Log("Contact nodes: " + lContactNodes.Count);
            //Log("Peers found: " + lTorrentPeers.Count);
            //Log("Peer requests sent: " + countGetPeerReqSent);
            //Log("Received peer packets: " + countRecvPeerPackets);
            //Log("Received node packets: " + countRecvNodePackets);
        }
        private void Merge()
        {
            if (!useA)
            {
                for (int i = 0; i < lContactNodesA.Count; i++)
                {
                    for (int j = 0; j < lContactNodesA[i].Count; j++)
                    {
                        if (!dhtBucketM.lDHTNodes[lContactNodesA[i][j].Item1].Contains(Tuple.Create(
                            lContactNodesA[i][j].Item2, lContactNodesA[i][j].Item3)))
                        {
                            dhtBucketM.lDHTNodes[lContactNodesA[i][j].Item1].Add(Tuple.Create(
                            lContactNodesA[i][j].Item2, lContactNodesA[i][j].Item3));
                            dhtBucketM.nofNodes++;
                        }
                        else
                        {
                            lContactNodesA[i].RemoveAt(j);
                        }
                    }
                }
            }
            else 
            {
                for (int i = 0; i < lContactNodesB.Count; i++)
                {
                    for (int j = 0; j < lContactNodesB[i].Count; j++)
                    {
                        if (!dhtBucketM.lDHTNodes[lContactNodesB[i][j].Item1].Contains(Tuple.Create(
                            lContactNodesB[i][j].Item2, lContactNodesB[i][j].Item3)))
                        {
                            dhtBucketM.lDHTNodes[lContactNodesB[i][j].Item1].Add(Tuple.Create(
                            lContactNodesB[i][j].Item2, lContactNodesB[i][j].Item3));
                            dhtBucketM.nofNodes++;
                        }
                        else
                        {
                            lContactNodesB[i].RemoveAt(j);
                        }
                    }
                }
            }
        }
        private int GetIndexFromConvID(string conversationID)
        {
            byte[] b = System.Text.Encoding.ASCII.GetBytes(conversationID);
            return Int32.Parse(conversationID, System.Globalization.NumberStyles.HexNumber);
        }
        private int GetBucketIndexFromInfohash(string infohash)
        {
            string infohash2 = HexSToString(infohash);
            byte[] b = BencodingUtils.ExtendedASCIIEncoding.GetBytes(infohash2);
            byte[] b2 = new byte[4]{0,0,0,0};
            Array.Copy(b, 0, b2, 2, 2);
            Array.Reverse(b2);
            return BitConverter.ToInt32(b2, 0);
        }
        private void GetPeers(IPEndPoint toAdr, string infohash, string conversationID)
        {
            string getPeersMsg = "d1:ad2:id20:" + HexSToString(nodeIDHex) + "9:info_hash20:" + HexSToString(infohash) + "e1:q9:get_peers1:t2:" + HexSToString(conversationID) + "1:v4:abcd1:y1:qe";
            Log("Sent GetPeers request to " + toAdr);
            SendMessage(toAdr, getPeersMsg);
        }
        private void FindNode(IPEndPoint toAdr, string findNodeID, string conversationID)
        {
            string findNodeMsg = "d1:ad2:id20:" + HexSToString(nodeIDHex) + "6:target20:" + HexSToString(findNodeID) + "e1:q9:find_node1:t2:" + HexSToString(conversationID) + "1:v4:abcd1:y1:qe";
            Log("Sent FindNode request to " + toAdr);
            SendMessage(toAdr, findNodeMsg);
        }
        private void SendMessage(IPEndPoint destination, string message)
        {
            byte[] send_buffer = BencodingUtils.ExtendedASCIIEncoding.GetBytes(message);

                udpClient.Send(send_buffer, send_buffer.Length, destination);

        }
        /// <summary>
        /// Not thread safe, call when all threads have finished
        /// </summary>
        private void OutputContactList()
        {
            Log("Contact list:");
            for (int index = 0; index < lContactNodesA.Count; index++)
            {
                //Log(index.ToString() + ": " + lContactNodes.ElementAt(index).Item1 + " " + lContactNodes.ElementAt(index).Item2.ToString());
            }
        }
        private void OutputTorrentPeerList()
        {
            Log("Torrent peer list:");
            for (int i = 0; i < lTorrentPeers.Count; i++)
            {
                Log(i.ToString() + ": " + lTorrentPeers[i].ToString());
            }
        }
        private void Log(string message)
        {
            //if (Program.debug)
            //    Console.WriteLine(message);
        }
        /// <summary>
        /// Updates the buckets with closer nodes
        /// </summary>
        /// <param name="nodeIpPortString"></param>
        private void UpdateContactList(BString nodeIpPortString, BString transactionID)
        {
            byte[] arr = BencodingUtils.ExtendedASCIIEncoding.GetBytes(nodeIpPortString.Value);
            byte[] arr2 = BencodingUtils.ExtendedASCIIEncoding.GetBytes(transactionID.Value);
            byte[] transID = new byte[2];
            Array.Copy(arr2, 0, transID, 0, 2);
            string stransID = ByteArrayToHexString(transID);
            int index = GetIndexFromConvID(stransID);
            byte[] ip = new byte[4];
            byte[] port = new byte[2];
            byte[] nodeID = new byte[20];
            byte[] currLock;
            int bucketNr;

            for (int i = 0; i < arr.Length / 26; i++)
            {
                currLock = new byte[4] { 0, 0, 0, 0 };
                Array.Copy(arr, i * 26, currLock, 2, 2);
                Array.Copy(arr, i * 26, nodeID, 0, 20);
                Array.Copy(arr, i * 26 + 20, ip, 0, 4);
                Array.Copy(arr, i * 26 + 24, port, 0, 2);
                Array.Reverse(port);
                Array.Reverse(currLock);
                bucketNr = BitConverter.ToInt32(currLock, 0);
                IPEndPoint ipEndP = new IPEndPoint((Int64)BitConverter.ToUInt32(ip, 0), (Int32)BitConverter.ToUInt16(port, 0));
                string sNodeID = ByteArrayToHexString(nodeID);

                if ((Int64)BitConverter.ToUInt32(ip, 0) != 0 && (Int32)BitConverter.ToUInt16(port, 0) != 0)
                {
                    Tuple<int, string, IPEndPoint> toInsert = Tuple.Create(bucketNr, sNodeID, ipEndP);
                    if (useA && CheckIfBucketCloseEnough(toInsert, index))
                        lContactNodesA[index].Add(toInsert);
                    else if (!useA && CheckIfBucketCloseEnough(toInsert, index))
                        lContactNodesB[index].Add(toInsert);
                }
            }
        }
        private bool CheckIfBucketCloseEnough(Tuple<int, string, IPEndPoint> toInsert,int index)
        {
            int bucketIndex = GetBucketIndexFromInfohash(linfoHHex[index]);
            if (toInsert.Item1 > bucketIndex - 1 && toInsert.Item1 < bucketIndex + 1)
            {
                return true;
            }
            else return false;
        }
        /// <summary>
        /// Updates Peer list for given infohash
        /// </summary>
        /// <param name="lIPPortString"></param>
        /// <param name="transactionID"></param>
        private void UpdateTorrentPeerList(BList lIPPortString, BString transactionID)
        {
            byte[] ip = new byte[4];
            byte[] port = new byte[2];
            byte[] arr2 = BencodingUtils.ExtendedASCIIEncoding.GetBytes(transactionID.Value);
            byte[] transID = new byte[2];
            Array.Copy(arr2, 0, transID, 0, 2);
            string stransID = ByteArrayToHexString(transID);
            int index = GetIndexFromConvID(stransID);

            for (int k = 0; k < lIPPortString.Count; k++)
            {
                BString tempBS = (BString)lIPPortString[k];
                byte[] arr = BencodingUtils.ExtendedASCIIEncoding.GetBytes(tempBS.Value);
                Array.Copy(arr, 0, ip, 0, 4);
                Array.Copy(arr, 4, port, 0, 2);
                Array.Reverse(port);
                IPEndPoint ipEndP = new IPEndPoint((Int64)BitConverter.ToUInt32(ip, 0), (Int32)BitConverter.ToUInt16(port, 0));

                if ((Int64)BitConverter.ToUInt32(ip, 0) != 0 && (Int32)BitConverter.ToUInt16(port, 0) != 0)
                {
                    if (!lTorrentPeers[index].Contains(ipEndP))
                    {
                        lTorrentPeers[index].Add(ipEndP);
                        nofPeersFound++;
                    }
                }
            }
        }
        /// <summary>
        /// Converts nodeID from BString to displayable hex string
        /// </summary>
        /// <param name="nodeID"></param>
        /// <returns></returns>
        private string BStringToHexNodeID(BString nodeID)
        {
            string hex = BitConverter.ToString(BencodingUtils.ExtendedASCIIEncoding.GetBytes(nodeID.Value));
            return hex.Replace("-", "");
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
        /// Wait for specified ms
        /// </summary>
        /// <param name="ms"></param>
        private void Wait(int ms)
        {
            DateTime called = DateTime.Now;
            while (DateTime.Now < called.Add(new TimeSpan(ms * 10000))) { }
        }
        /// <summary>
        /// Wait for Program.MAX_SYNC_WAIT
        /// </summary>
        private void Wait()
        {
            DateTime called = DateTime.Now;
            while (DateTime.Now < called.Add(Program.MAX_SYNC_WAIT)) { }
        }
        private string GetRandomID()
        {
            byte[] b = new byte[20];
            random.NextBytes(b);
            return ByteArrayToHexString(b);
        }
        private string GetRandomConvID()
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
        private string GetConversatioID(int index)
        {
            //byte[] b = new byte[2];
            //b[1] = (byte)(index & 0xFF);
            //b[0] = (byte)((index >> 8) & 0xFF);
            //return ByteArrayToHexString(b);
           // return index.ToString();
            return String.Format("{0:X4}", index);
        }
        private void InitInfoHashes(bool useSecondary)
        {
            List<string> infohashes = new List<string>();
            infohashes = Program.mySQL.getNextInfoHashes(Program.MAX_INFOHASHES_PER_NODE);

            for (int i = 0; i < infohashes.Count; i++)
            {
                if (infohashes[i] != null && !linfoHHex.Contains(infohashes[i])) // todo
                {
                    if (useSecondary)
                    {
                        linfoHHex2.Add(infohashes[i]);
                    }
                    else
                    {
                        linfoHHex.Add(infohashes[i]);
                    }
                }
                else
                { break; }
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
                    byte[] data = udpClient.Receive(ref receiveFrom);

                    // Decode the message
                    Stream stream = new MemoryStream(data);
                    IBencodingType receivedMsg = BencodingUtils.Decode(stream);
                    string decoded = BencodingUtils.ExtendedASCIIEncoding.GetString(data.ToArray());
                    Log("Received message!");
                    //Log(decoded);

                    // t is transaction id : todo: check, since ports are reused
                    // y is e for error, r for reply, q for query
                    if (receivedMsg is BDict) // throws error.. todo: fix
                    {
                        BDict dictMsg = (BDict)receivedMsg;
                        if (dictMsg.ContainsKey("y"))
                        {
                            if (dictMsg["y"].Equals(new BString("e")))
                            {
                                // received error
                                Log("Received error! (ignored)");
                            }
                            else if (dictMsg["y"].Equals(new BString("r")))
                            {
                                // received reply
                                if (dictMsg.ContainsKey("r"))
                                {
                                    if (dictMsg["r"] is BDict)
                                    {
                                        BDict dictMsg2 = (BDict)dictMsg["r"];
                                        if (dictMsg2.ContainsKey("values") && dictMsg.ContainsKey("t"))
                                        {
                                            Log("Received list of peers for torrent!");
                                            countRecvPeerPackets++;
                                            BList peerAdrs = (BList)dictMsg2["values"];
                                            UpdateTorrentPeerList(peerAdrs, (BString)dictMsg["t"]);
                                        }
                                        else if (dictMsg2.ContainsKey("nodes") && dictMsg.ContainsKey("t"))
                                        {
                                            // could be an answer to find node or get peers
                                            Log("Received list of nodeID & IP & port!");
                                            countRecvNodePackets++;
                                            BString nodeIDString = (BString)dictMsg2["nodes"];
                                            UpdateContactList(nodeIDString, (BString)dictMsg["t"]);
                                        }
                                        else
                                        {
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
                                Log("Received query! (ignored)");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log("Error receiving data: " + ex.ToString());
                }
            }
        }
    }
}