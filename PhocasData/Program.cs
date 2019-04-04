#define new

using System;
using System.Data;
using Npgsql;
using System.Net;
using System.Text;
using System.IO;
using System.Security.Cryptography;
using System.Xml;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;

[assembly: log4net.Config.XmlConfigurator(Watch = true)]

namespace PhocasBidData
{
    public partial class Generate
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger
            (System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        static NpgsqlConnection conn;

        static string csvDataPath = "C:\\Phocas\\Rawdata\\";

        static string vehiclestats = "";

        /**
         * Created on 02/11/2016.
         * Reads various AMS database tables and exports then as .csv files for Phocas
         * @author andy
         *
         */
        public static void Main(String[] args)
        {
            ConnectToDB();

            DateTime start = DateTime.Now;

            Console.WriteLine("Beginning data extract at " + start);
            log.Info("Beginning data extract at " + start);

            // Generate Mobile Stats
            GetMobileStatsCSV();

            DateTime end = DateTime.Now;
            Console.WriteLine("Completed data extract " + end);
            log.Info("Completed data extract " + end);

            TimeSpan duration = end - start;

            Console.WriteLine("Time taken " + duration.ToString(@"hh\:mm\:ss"));
                log.Info("Completed data extract " + duration.ToString(@"hh\:mm\:ss"));

            //Console.WriteLine("Press any key to clear...");
            //Console.ReadKey();

        }

        /**
         * Created on 02/11/2016.
         * Sets up Postgrsql database connection using npgsql
         * @author andy
         *
         */
        private static void ConnectToDB()
        {
            // Login to PostgreSQL
            string[] lines = null;
            string sqlconnection;
            string password;
            try
            {
                lines = System.IO.File.ReadAllLines(@"C:\\Users\\AMS\\dbadmin.cfg");
            }
            catch (IOException ie)
            {
                Console.Write("Please check config file");
                log.Warn("Please check config file " + ie.Message);
                return;
            }

            password = DecodeFrom64(lines[4]);

            sqlconnection = "Server=" + lines[0] + ";Port=" + lines[1] + ";Database=" + lines[2] + ";User Id=" + lines[3] + ";Password=" + password + ";" + "CommandTimeout=1440;";

            conn = new NpgsqlConnection(sqlconnection);

            return;
        }

        /**
         * Created on 02/11/2016.
         * Writes the contents of a DataReader to a .csv file line by line for Phocas
         * @author andy
         *
         */
        private static StreamWriter WriteCSV(string fn, NpgsqlDataReader dr)
        {
            StringBuilder sb = new StringBuilder();
            using (StreamWriter writetext = new StreamWriter(fn))
            {
                var columnNames = Enumerable.Range(0, dr.FieldCount).Select(dr.GetName).ToList();
                sb.AppendLine(string.Join(",", columnNames));
                writetext.Write(sb.ToString());
                while (dr.Read())
                {
                    var fields = Enumerable.Range(0, dr.FieldCount).Select(dr.GetValue).ToList();
                    sb.Clear();
                    sb.AppendLine(string.Join(",", fields.Select(field => string.Concat("\"", field.ToString().Replace("\"", "\"\""), "\""))));
                    writetext.Write(sb.ToString());
                }
                return writetext;
            }
        }

        /**
         * Created on 02/11/2016.
         * Appends the contents of a DataReader to a .csv file line by line for Phocas
         * @author andy
         *
         */
        private static void AppendCSV(string fn, NpgsqlDataReader dr)
        {
            StringBuilder sb = new StringBuilder();
            using (StreamWriter writetext = new StreamWriter(fn, true))
            {
                while (dr.Read())
                {
                    var fields = Enumerable.Range(0, dr.FieldCount).Select(dr.GetValue).ToList();
                    sb.Clear();
                    sb.AppendLine(string.Join(",", fields.Select(field => string.Concat("\"", field.ToString().Replace("\"", "\"\""), "\""))));
                    writetext.Write(sb.ToString());
                }
            }
        }


        /**
         * Created on 31/10/2017.
         * Gets Mobile / Online Bid stats etc for Phocas
         * @author andy
         *
         */
        private static void GetMobileStatsCSV()
        {
            try
            {
                string datePostfix = DateTime.Now.ToString("-yyyy-MM-dd");
                string bidcsvFile = csvDataPath + "bidstats.csv";
                string lotcsvFile = csvDataPath + "lotstats.csv";
                string biddercsvFile = csvDataPath + "bidderstats.csv";
                string usercsvFile = csvDataPath + "userstats.csv";
                string savedbidcsvFile = csvDataPath + "bidstats" + datePostfix + ".csv";
                string savedlotcsvFile = csvDataPath + "lotstats" + datePostfix + ".csv";
                string savedbiddercsvFile = csvDataPath + "bidderstats" + datePostfix + ".csv";
                string savedusercsvFile = csvDataPath + "userstats" + datePostfix + ".csv";

                // Copy the existing csv files and add a date suffix
                File.Copy(bidcsvFile, savedbidcsvFile, true) ;
                File.Copy(lotcsvFile, savedlotcsvFile, true);
                File.Copy(biddercsvFile, savedbiddercsvFile, true);
                File.Copy(usercsvFile, savedusercsvFile, true);

                Dictionary<int, string> processedSales = new Dictionary<int, string>();
                processedSales = loadProcessedSales(savedbidcsvFile);

                Dictionary<string, string> processedLots = new Dictionary<string, string>();
                processedLots = loadProcessedLots(savedlotcsvFile);

                Dictionary<string, string> processedBidders = new Dictionary<string, string>();
                processedBidders = loadProcessedBidders(savedbiddercsvFile);
                
                Dictionary<string, string> processedUsers = new Dictionary<string, string>();
                processedUsers = loadProcessedUsers(savedusercsvFile);

                using (StreamWriter bidtext = new StreamWriter(bidcsvFile))
                {
                    using (StreamWriter lottext = new StreamWriter(lotcsvFile))
                    {
                        using (StreamWriter biddertext = new StreamWriter(biddercsvFile))
                        {
                            using (StreamWriter usertext = new StreamWriter(usercsvFile))
                            {
                                StringBuilder bidHeaders = new StringBuilder();
                                bidHeaders.Append("SaleNo").Append(",");
                                bidHeaders.Append("Site").Append(",");
                                bidHeaders.Append("Start").Append(",");
                                bidHeaders.Append("Lots").Append(",");
                                bidHeaders.Append("HallBids").Append(",");
                                bidHeaders.Append("OnlineBidders").Append(",");
                                bidHeaders.Append("OnlineBids").Append(",");
                                bidHeaders.Append("MobileBidders").Append(",");
                                bidHeaders.Append("MobileBids").Append(",");
                                bidHeaders.Append("OnlineClients").Append(",");
                                bidHeaders.Append("MobileClients").Append("\n");
                                bidtext.Write(bidHeaders);

                                StringBuilder bidderHeaders = new StringBuilder();
                                bidderHeaders.Append("SaleNo").Append(",");
                                bidderHeaders.Append("Site").Append(",");
                                bidderHeaders.Append("Start").Append(",");
                                bidderHeaders.Append("Name").Append(",");
                                bidderHeaders.Append("Id").Append(",");
                                bidderHeaders.Append("Company").Append(",");
                                bidderHeaders.Append("Type").Append(",");
                                bidderHeaders.Append("Bid").Append("\n");
                                biddertext.Write(bidderHeaders);

                                StringBuilder lotHeaders = new StringBuilder();
                                lotHeaders.Append("Site").Append(",");
                                lotHeaders.Append("SaleLot").Append(",");
                                lotHeaders.Append("SaleNo").Append(",");
                                lotHeaders.Append("Lot").Append(",");
                                lotHeaders.Append("Registration").Append(",");
                                lotHeaders.Append("Make").Append(",");
                                lotHeaders.Append("Model").Append(",");
                                lotHeaders.Append("Name").Append(",");
                                lotHeaders.Append("Company").Append(",");
                                lotHeaders.Append("Type").Append(",");
                                lotHeaders.Append("ClosingBid").Append(",");
                                lotHeaders.Append("Outcome").Append(",");
                                lotHeaders.Append("Time").Append(",");
                                lotHeaders.Append("Seller").Append(",");
                                lotHeaders.Append("Seller Code").Append(",");
                                lotHeaders.Append("Hall Bids").Append(",");
                                lotHeaders.Append("Online Bids").Append(",");
                                lotHeaders.Append("Mobile Bids").Append(",");
                                lotHeaders.Append("Vehicle Id").Append("\n");
                                lottext.Write(lotHeaders);

                                StringBuilder userHeaders = new StringBuilder();
                                userHeaders.Append("SaleNo").Append(",");
                                userHeaders.Append("Site").Append(",");
                                userHeaders.Append("Start").Append(",");
                                userHeaders.Append("Name").Append(",");
                                userHeaders.Append("Id").Append(",");
                                userHeaders.Append("Company").Append(",");
                                userHeaders.Append("Type").Append("\n");
                                usertext.Write(userHeaders);

                                // FTP all the transaction logs we might need
                                for (int saleNo = 1650; saleNo < 4000; saleNo++)
                                //for (int saleNo = 1650; saleNo < 1655; saleNo++)
//                                for (int saleNo = 2532; saleNo < 2535; saleNo++)
                                {
                                    try
                                    {
                                        Boolean alreadyProcessed = CheckIfProcessed(saleNo, processedSales, bidtext);
//                                                                                Boolean alreadyProcessed = false;

                                        if (alreadyProcessed)
                                        {
                                            // Update the other csvs
                                            UpdateCSVs(saleNo, processedLots, processedBidders, processedUsers, lottext, biddertext, usertext);
                                        }
                                        else
                                        {
                                            String thisFile = FTPXML.GetXMLFile(saleNo);

                                            ShowTranscriptOfSale.ProcessXml(saleNo);

                                            ShowTranscriptOfSale.ProcessTransactionLog();

                                            string bidCSVData = ShowTranscriptOfSale.SaveBidData(ShowTranscriptOfSale.ThisSale.SaleNo);
                                            string lotCSVData = ShowTranscriptOfSale.SaveLotData(ShowTranscriptOfSale.ThisSale.SaleNo);
                                            string bidderCSVData = ShowTranscriptOfSale.SaveBidderData(ShowTranscriptOfSale.ThisSale.SaleNo);
                                            string userCSVData = ShowTranscriptOfSale.SaveUserData(ShowTranscriptOfSale.ThisSale.SaleNo);

                                            LogMsg("Bid CSV " + bidCSVData);
                                            LogMsg("Lot CSV " + lotCSVData);
                                            LogMsg("Bidder CSV " + bidderCSVData);
                                            LogMsg("User CSV " + userCSVData);

                                            if (saleNo == ShowTranscriptOfSale.ThisSale.SaleNo)
                                            {
                                                bidtext.Write(bidCSVData);
                                                bidtext.Write("\n");
                                                lottext.Write(lotCSVData);
                                                biddertext.Write(bidderCSVData);
                                                usertext.Write(userCSVData);
                                            }
                                            //                                        File.Delete(thisFile);
                                        }

                                    }
                                    catch (Exception ee)
                                    {
                                        LogMsg("Something went wrong " + ee.Message + "\n");
                                        Console.Write("Something went wrong " + ee.Message + "\n");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ee)
            {
                Console.Write("Exception " + ee.Message);
                log.Warn("Couldn't get Mobile data " + ee);
            }
        }

        private static void UpdateCSVs(int saleNo, Dictionary<string, string> processedLots, Dictionary<string, string> processedBidders, Dictionary<string, string> processedUsers, StreamWriter lottext, StreamWriter biddertext, StreamWriter usertext)
        {
            foreach (KeyValuePair<string, string> entry in processedLots)
            {
                if (entry.Key.IndexOf(saleNo.ToString()) == 0)
                {
                    lottext.Write(entry.Value + "\n");
                }
            }
            foreach (KeyValuePair<string, string> entry in processedUsers)
            {
                if (entry.Key.IndexOf(saleNo.ToString()) == 0)
                {
                    usertext.Write(entry.Value + "\n");
                }
            }
            foreach (KeyValuePair<string, string> entry in processedBidders)
            {
                if (entry.Key.IndexOf(saleNo.ToString()) == 0)
                {
                    biddertext.Write(entry.Value + "\n");
                }
            }
        }

        private static Dictionary<string, string> loadProcessedUsers(string filename)
        {
            Dictionary<string, string> users = new Dictionary<string, string>();

            using (var reader = new StreamReader(filename))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    try
                    {
                        users.Add(values[0] + "-" + values[4], line);
                    }
                    catch (ArgumentException de)
                    { 
                    }
                    catch (Exception ee)
                    {
                        Console.Write("Something else went wrong " + ee.Message);
                    }
                }
            }

            return users;
        }

        private static Dictionary<string, string> loadProcessedBidders(string filename)
        {
            Dictionary<string, string> bidders = new Dictionary<string, string>();


            using (var reader = new StreamReader(filename))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    try
                    {
                        bidders.Add(values[0] + "-" + values[4], line);
                    }
                    catch (ArgumentException de)
                    {

                    }
                    catch (Exception ee)
                    {
                        Console.Write("Something else wnet wrong " + ee.Message);
                    }
                }
            }

            return bidders;
        }

        private static Dictionary<string, string> loadProcessedLots(string filename)
        {
            Dictionary<string, string> lots = new Dictionary<string, string>();


            using (var reader = new StreamReader(filename))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    try
                    {
                        lots.Add(values[1], line);
                    }
                    catch (ArgumentException de)
                    {
                        // Ignoring duplicates
                        //Console.Write("How? " + de.Message);
                    }
                    catch (Exception ee)
                    {
                        // Catchall exception 
                        Console.Write("Something else went wrong " + ee.Message);
                    }
                }
            }

            return lots;
        }

        private static Dictionary<int, string> loadProcessedSales(string filename)
        {
            Dictionary<int, string> sales = new Dictionary<int, string>();

            using (var reader = new StreamReader(filename))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    int saleNo;
                    if (Int32.TryParse(values[0], out saleNo))
                    {
                        DateTime saleStart;
                        if (DateTime.TryParse(values[2], out saleStart))
                        {
                            if (saleStart < DateTime.Today)
                            {
                                if (saleNo > 2626)
                                {
                                    Console.WriteLine("Up to date");
                                }
                                sales.Add(saleNo, line);
                            }
                        }
                    }
                    else
                    {
                        // Ignoring parsing errors - header row etc
                        Console.WriteLine("String could not be parsed - " + values);
                    }

                }
            }

            return sales;
        }

        private static bool CheckIfProcessed(int saleNo, Dictionary<int, string> ProcessedSales, StreamWriter biddata)
        {
            // Check the existing BidStats file if this sale is already there
            if (ProcessedSales.ContainsKey(saleNo)) 
            {
                string data;
                if (ProcessedSales.TryGetValue(saleNo, out data))
                {
                    biddata.Write(data + "\n");
                    return true;
                }
            }
            return false;

        }


        static public string DecodeFrom64(string encodedData)
        {
            byte[] encodedDataAsBytes
                = System.Convert.FromBase64String(encodedData);
            string returnValue =
               System.Text.ASCIIEncoding.ASCII.GetString(encodedDataAsBytes);
            return returnValue;
        }

        static public void LogMsg(string info)
        {
            String[] LogMsg = { info };

            log.Info(LogMsg);

            return;
        }

        static public void LogMsg(Exception e)
        {
            String[] LogMsg = { e.Message };

            log.Fatal(LogMsg, e);

            return;
        }

    }

}
