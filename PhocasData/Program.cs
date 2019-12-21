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
using System.Net.Http.Headers;
using System.Net.Http;
using Newtonsoft.Json;

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
            DateTime start = DateTime.Now;

            Console.WriteLine("Starting e-Hub data extract at " + start);
            GeteHubData();
            Console.WriteLine("Completed e-Hub data extract at " + DateTime.Now);

            ConnectToDB();

            start = DateTime.Now;

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

//            Console.WriteLine("Press any key to clear...");
//            Console.ReadKey();

        }

        public class Pwd
        {
            public String password { get; set; }
            public String username { get; set; }
        }

        public class SendToAuction
        {
            public List<e_Hub_Data> data { get; set; }
        }

        public class Started
        {
            public List<e_Hub_Data> data { get; set; }
        }

        public class SoldToTcbg
        {
            public List<e_Hub_Data> data { get; set; }
        }
        public class Submitted
        {
            public List<e_Hub_Data> data { get; set; }
        }
        public class SuccessfullyReturnedCap
        {
            public List<e_Hub_Data> data { get; set; }
        }
        public class SuccessfullyReturnedTcbg
        {
            public List<e_Hub_Data> data { get; set; }
        }
        public class eObject
        {
            public object exListed { get; set; }
            public SendToAuction sendToAuction { get; set; }
            public object exUnsoldAuction { get; set; }
            public Started started { get; set; }
            public List<string> people { get; set; }
            public SoldToTcbg soldToTcbg { get; set; }
            public Submitted submitted { get; set; }
            public object exUnsoldTcbg { get; set; }
            public object exSoldVolLyrtd { get; set; }
            public SuccessfullyReturnedCap successfullyReturnedCap { get; set; }
            public SuccessfullyReturnedTcbg successfullyReturnedTcbg { get; set; }
            public object exSold { get; set; }
            public object exSoldVolFyrtd { get; set; }
        }

        public class RootObject
        {
            public eObject eObject { get; set; }
            public object message { get; set; }
            public object errors { get; set; }
        }

        public class e_Hub_Data
        {
            public long Id { get; set; }
            public String UserName { get; set; }
            public String sendDate { get; set; }
            public String message { get; set; }
            public String vrm { get; set; }
            public String type { get; set; }
        }


        public class e_Hub_User
        {
            public String UserName { get; set; }
            public String AccountCode { get; set; }
        }

        private static void GeteHubData()
        {
            //String endPoint = "http://humdev.astonbarclay.net:8080/login";
            String endPoint = "https://api2.astonbarclay.net/appraisal/login";
            Pwd data = new Pwd();
            data.password = "P@ssw0rd";
            data.username = "AAR003c";
            data.password = "P@ssw0rd";
            data.username = "ehub.stats";

            try
            {
                // Login to end point
                HttpClient httpClientAuthorize = new HttpClient();
                httpClientAuthorize.BaseAddress = new Uri(endPoint);
                httpClientAuthorize.DefaultRequestHeaders.Accept.Clear();
                httpClientAuthorize.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                HttpResponseMessage task = httpClientAuthorize.PostAsJsonAsync(endPoint, data).Result;
                HttpStatusCode success = task.StatusCode;

                if (success.Equals(HttpStatusCode.OK))
                {
                    String bearer = "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0ZXN0YWFyMDAzY0B0ZXN0LmNvbSIsInJvbGVzIjoiQnV5IEl0IE5vdyBBY2Nlc3MsRGVhbGVyIEdlbmVyaWMgQXBwcmFpc2FsIEFwcCxMaXZlIEJpZCBBY2Nlc3MsYWNjZXNzX3RvX2V4Y2hhbmdlX2xpc3Rfb25seSxhbXNfY2FzY2FkZV91c2VyLGFtc192ZW5kb3JfY29kZTpBQVIwMDMsZS1IdWIgVXNlcixlLVhjaGFuZ2UgQnV5ZXIsIiwiZXhwIjoxNTU4Njg5MTEzfQ.boUyd0nKyv3H8YsU85wkwxjvXPeC2sUscX_rE7MMjo-1KqLdKg4by2H-xQDWEiSNxTQbu8vgIyVzd9xKhOvaIg";
//                    String headers = task.Headers.ToString();
                    HttpResponseHeaders rh = task.Headers;
                    IEnumerable<string> values;
                    if (rh.TryGetValues("Authorization", out values))
                    {
                        bearer = values.First();
                    }

                    // endPoint = "http://humdev.astonbarclay.net:8080/v.1/report/from/2019-01-01/to/2019-12-31";

                    endPoint = "https://api2.astonbarclay.net/appraisal/v.1/report/from/2019-05-01/to/2019-12-31";
                    HttpClient httpClientData = new HttpClient();
                    httpClientData.BaseAddress = new Uri(endPoint);
                    httpClientData.DefaultRequestHeaders.Accept.Clear();
                    httpClientData.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    httpClientData.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", bearer);

                    task = httpClientData.GetAsync(endPoint).Result;

                    success = task.StatusCode;
                    if (success.Equals(HttpStatusCode.OK))
                    {
                        var x = task.Content.ReadAsStringAsync();
                        var objects = Newtonsoft.Json.JsonConvert.DeserializeObject(x.Result);
                        List<e_Hub_Data> ed = new List<e_Hub_Data>();
                        List<e_Hub_User> eu = new List<e_Hub_User>();
                        JsonTextReader reader = new JsonTextReader(new StringReader(x.Result));
                        //RootObject edata = Newtonsoft.Json.JsonConvert.DeserializeObject<RootObject>(x.Result);

                        e_Hub_Data thisEh = new e_Hub_Data();
                        e_Hub_User thisEu = new e_Hub_User();
                        String val = "";
                        try
                        {
                            while (reader.Read())
                            {
                                if (reader.Value != null)
                                {
                                    switch (val)
                                    {
                                        case "id":
                                            thisEh = new e_Hub_Data();
                                            thisEh.Id = (long)reader.Value;
                                            val = "";
                                            break;
                                        case "username":
                                            thisEh.UserName = (string)reader.Value;
                                            val = "";
                                            break;
                                        case "date":
                                            thisEh.sendDate = reader.Value.ToString();
                                            val = "";
                                            break;
                                        case "message":
                                            thisEh.message = (string)reader.Value;
                                            val = "";
                                            break;
                                        case "vrm":
                                            thisEh.vrm = (string)reader.Value;
                                            val = "";
                                            break;
                                        case "type":
                                            thisEh.type = (string)reader.Value;
                                            ed.Add(thisEh);
                                            val = "";
                                            break;
                                        case "people":
                                            if (thisEu.UserName != null)
                                            {
                                                thisEu.AccountCode = (string)reader.Value;
                                                eu.Add(thisEu);
                                                thisEu = new e_Hub_User();
                                            }
                                            else
                                            {
                                                thisEu.UserName = (string)reader.Value;
                                            }
                                            break;
                                        default:
                                            break;
                                    }
                                    if (reader.TokenType.Equals(JsonToken.PropertyName))
                                    {
                                        if (val.IndexOf("people") > -1)
                                        {
                                            if (reader.Value.Equals("message"))
                                            {
                                                val = "";
                                            }

                                        }
                                        else
                                        {
                                            val = (String)reader.Value;
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        String csvData = "";
                        foreach (e_Hub_Data eh in ed)
                        {
                            DateTime dt = Convert.ToDateTime(eh.sendDate);
                            String dts = dt.ToString("yyyy/MM/dd");
                            csvData += eh.Id + "," + eh.UserName + "," + dts + "," + eh.vrm + "," + eh.message + "," + eh.type + "\n";
                        }

                        string ehubcsvFile = csvDataPath + "ehubstats.csv";
                        using (StreamWriter usertext = new StreamWriter(ehubcsvFile))
                        {
                            StringBuilder userHeaders = new StringBuilder();
                            userHeaders.Append("Id").Append(",");
                            userHeaders.Append("UserName").Append(",");
                            userHeaders.Append("SendDate").Append(",");
                            userHeaders.Append("VRM").Append(",");
                            userHeaders.Append("Message").Append(",");
                            userHeaders.Append("Type").Append("\n");
                            usertext.Write(userHeaders);
                            usertext.Write(csvData);
                        }

                        csvData = "";
                        foreach (e_Hub_User et in eu)
                        {
                            csvData += et.UserName + "," + et.AccountCode + "\n";
                        }

                        string usercsvFile = csvDataPath + "ehubusers.csv";
                        using (StreamWriter usertext = new StreamWriter(usercsvFile))
                        {
                            StringBuilder userHeaders = new StringBuilder();
                            userHeaders.Append("UserName").Append(",");
                            userHeaders.Append("AccountNumber").Append("\n");
                            usertext.Write(userHeaders);
                            usertext.Write(csvData);
                        }
                    }
                    else
                    {
                        Console.WriteLine("Data Unexpected response " + success);
                        return;
                    }
                }
                else
                {
                    Console.WriteLine("Login Unexpected response " + success);
                    return;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

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

                // Tidy up any older csv files so we don't run out of disk space
                // Go back a month
                DateTime old = DateTime.Now.AddMonths(-1);
                string dp = old.ToString("-yyy-MM-dd");
                while (old.AddDays(4) < DateTime.Now)
                {
                    Console.WriteLine("looking for " + dp);
                    string oldbidcsvFile = csvDataPath + "bidstats" + dp + ".csv";
                    string oldlotcsvFile = csvDataPath + "lotstats" + dp + ".csv";
                    string oldbiddercsvFile = csvDataPath + "bidderstats" + dp + ".csv";
                    string oldusercsvFile = csvDataPath + "userstats" + dp + ".csv";
                    File.Delete(oldbidcsvFile);
                    File.Delete(oldlotcsvFile);
                    File.Delete(oldbiddercsvFile);
                    File.Delete(oldusercsvFile);
                    old = old.AddDays(1);
                    dp = old.ToString("-yyy-MM-dd");
                }

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
                                // Find the latest /  largest sale no and loop until that
                                long latestSaleNo = FTPXML.GetLatestSaleNo();

                                for (int saleNo = 1650; saleNo <= latestSaleNo; saleNo++)
//                                for (int saleNo = 1650; saleNo < 5000; saleNo++)
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
            try
            {
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
            }
            catch (FileNotFoundException fe)
            {
                Console.WriteLine("Process sales - file not found " + filename);
            }

            return users;
        }

        private static Dictionary<string, string> loadProcessedBidders(string filename)
        {
            Dictionary<string, string> bidders = new Dictionary<string, string>();

            try { 
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
            }
            catch (FileNotFoundException fe)
            {
                Console.WriteLine("Process sales - file not found " + filename);
            }

            return bidders;
        }

        private static Dictionary<string, string> loadProcessedLots(string filename)
        {
            Dictionary<string, string> lots = new Dictionary<string, string>();

            try { 
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
        }
            catch (FileNotFoundException fe)
            {
                Console.WriteLine("Process sales - file not found " + filename);
            }

            return lots;
        }

        private static Dictionary<int, string> loadProcessedSales(string filename)
        {
            Dictionary<int, string> sales = new Dictionary<int, string>();
            Console.WriteLine("Process sales " + filename);

            try
            {
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
                            if (values.Length > 2)
                            {
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
                                Console.WriteLine("Date format?");
                            }
                        }
                        else
                        {
                            // Ignoring parsing errors - header row etc
                            Console.WriteLine("String could not be parsed - " + values[0].ToString());
                        }

                    }
                }
            }
            catch (FileNotFoundException fe)
            {
                Console.WriteLine("Process sales - file not found " + filename);
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
