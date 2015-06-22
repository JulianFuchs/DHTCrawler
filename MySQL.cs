using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.Net;

namespace DHTCrawler
{
    class MySQL
    {
        private MySqlConnection connection;
        private MySqlCommand command;
        private MySqlDataReader reader;

        public MySQL()
        {
            //string sConnection = "SERVER=localhost;" + "DATABASE=bachelor;" + "UID=root;" + "PASSWORD=salami;" +"Connection Timeout=10000";
            //string sConnection = "SERVER=pc-10129.ethz.ch;" + "DATABASE=bt_dht_recommender;" + "UID=bt_dht_recommend;" + "PASSWORD=xSXLQHfSj9hUsmj4;" + "Connection Timeout=10000";
            string sConnection = "SERVER=localhost;" + "DATABASE=bachelor;" + "UID=root;" + "PASSWORD=xSXLQHfSj9hUsmj4;" + "Connection Timeout=10000";

            connection = new MySqlConnection(sConnection);
            command = connection.CreateCommand();
        }
        public List<string> getNextInfoHashes(int nofInfohashes)
        {
            List<String> lResult = new List<String>();
            lock (Program.dbLock)
            {
                // Get oldest torrent
                command.CommandText = "SELECT  `infohash`,`id` FROM  `torrents45` ORDER BY updated ASC LIMIT "
                    + nofInfohashes.ToString() + ";";
                command.CommandTimeout = 60000;
                connection.Open();

                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                while (reader.Read())
                {
                    lResult.Add(reader.GetString("infohash"));
                }

                reader.Close();
                connection.Close();
                connection.Open();

                command.CommandText = "UPDATE `torrents45` SET updated = CURRENT_TIMESTAMP WHERE infohash = '" + lResult[0] + "' OR ";
                for (int i = 1; i < lResult.Count-1; i++)
                {
                    command.CommandText += "infohash = '" + lResult[i] + "' OR ";
                }
                command.CommandText += "infohash = '" + lResult[lResult.Count-1] + "';";
                command.CommandTimeout = 60000;
                command.ExecuteNonQuery();
                connection.Close();
            }
            return lResult;
        }
        public void InsertPeers(List<string> linfoHHex, List<List<IPEndPoint>> lTorrentPeers)
        {
            lock (Program.dbLock)
            {
                connection.Open();
                // Insert all peers
                for (int c = 0; c < lTorrentPeers.Count; c++)
                {
                    if (lTorrentPeers[c].Count != 0)
                    {
                        command.CommandText = "INSERT IGNORE INTO `peers` (ipAddress) VALUES ('" + lTorrentPeers[c][0].ToString() + "')";
                        command.CommandTimeout = 60000;
                        for (int peers = 1; peers < lTorrentPeers[c].Count; peers++)
                        {
                            command.CommandText += ", ('" + lTorrentPeers[c][peers].ToString() + "')";
                        }
                        command.ExecuteNonQuery();
                    }
                }
                // Insert torrent-peer associations 
                command.CommandTimeout = 6000;
                command.CommandText = "INSERT INTO associated (torrentID, peerID) (SELECT torrents.id,peers.id " + 
                "FROM torrents, peers WHERE ";
                for (int c = 0; c < lTorrentPeers.Count; c++)
                {
                    if (lTorrentPeers[c].Count != 0)
                    {
                        for (int peers = 0; peers < lTorrentPeers[c].Count; peers++)
                        {
                            command.CommandText += "torrents.infohash = '" + linfoHHex[c].ToString() + "' " +
                                "AND peers.ipAddress = '" + lTorrentPeers[c][peers].ToString() + "' OR ";
                        }
                    }
                }
                if (command.CommandText.Length > 200)
                {
                    // Dont execute invalid SQL commands
                    command.CommandText += "torrents.infohash = 'abc' AND peers.ipAddress = 'abc') " +
                        "ON DUPLICATE KEY UPDATE `updated` = CURRENT_TIMESTAMP;";
                    command.ExecuteNonQuery();
                }

                /*
                if (lTorrentPeers[c].Count != 0)
                {
                    // Get torrentID
                    command.CommandText = "SELECT  `id` FROM  `torrents45` WHERE `infohash` = '" + linfoHHex[c].ToString() + "'";
                    command.CommandTimeout = 6000;
                    reader = command.ExecuteReader();
                    reader.Read();
                    torrentID = (int)reader.GetValue(0);
                    reader.Close();

                    for (int peers = 0; peers < lTorrentPeers[c].Count; peers++)
                    {
                        // Get peerID
                        command.CommandText = "SELECT  `id` FROM  `peers` WHERE `ipAddress` = '" + lTorrentPeers[c][peers].ToString() + "'";
                        command.CommandTimeout = 6000;
                        reader = command.ExecuteReader();
                        reader.Read();
                        peerID = (int)reader.GetValue(0);
                        reader.Close();

                        // Insert association
                        command.CommandText = "INSERT INTO `associated` (torrentID,peerID) VALUES (" + torrentID.ToString() + "," +
                        peerID.ToString() + ") ON DUPLICATE KEY UPDATE `updated` = CURRENT_TIMESTAMP";
                        command.CommandTimeout = 6000;
                        command.ExecuteNonQuery();
                    }
                }*/

                connection.Close();
            }
        }
    }
}
