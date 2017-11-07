using System;
using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using System.Threading;

namespace PhocasData
{
    public partial class ShowTranscriptOfSale
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger
            (System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public static string Transcript = null;

        public class transcriptline
        {
            public string saleId { get; set; }
            public string saleType { get; set; }
            public string description { get; set; }
            public string date { get; set; }
            public string lot { get; set; }
            public string vehicleId { get; set; }
            public string make { get; set; }
            public string model { get; set; }
            public string enginesize { get; set; }
            public string registration { get; set; }
            public string bidValue { get; set; }
            public string nextBid { get; set; }
            public string retractBid { get; set; }
            public string bidderId { get; set; }
            public string bidderType { get; set; }
            public string name { get; set; }
            public string company { get; set; }
            public string webCode { get; set; }
            public string saleCode { get; set; }
            public string siteCode { get; set; }
            public string siteName { get; set; }
            public string max { get; set; }
            public string ask { get; set; }
            public string action { get; set; }
            public string outcomeType { get; set; }
            public string vendorId { get; set; }
            public string vendorName { get; set; }
            public string vendorCode { get; set; }
            public string highestHallBid { get; set; }
            public string highestOnlineBid { get; set; }
            public string auctioneerMessage { get; set; }
            public string auctioneerMessageToAll { get; set; }
            public string clientMessage { get; set; }
        }

        public class transcript : IEnumerable
        {
            public readonly System.Collections.Generic.List<transcriptline> tl = new System.Collections.Generic.List<transcriptline>();

            public void addline(transcriptline tr)
            {
                tl.Add(tr);
            }

            public System.Collections.Generic.IEnumerator<transcriptline> GetEnumerator()
            {
                return tl.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

        }

        static transcript tl;

        public class lot
        {
            public string lotNumber { get; set; }
            public string outcomeType { get; set; }
            public string vendorId { get; set; }
            public string vendorName { get; set; }
            public string vendorCode { get; set; }
            public string buyerId { get; set; }
            public string buyerName { get; set; }
            public string buyerCompany { get; set; }
            public string buyerType { get; set; }
            public string buyerWebCode { get; set; }
            public string buyerSiteCode { get; set; }
            public string closingBid { get; set; }
            public string highestHallBid { get; set; }
            public string highestOnlineBid { get; set; }
            public string registration { get; set; }
            public string description { get; set; }
        }

        public class lots : IEnumerable
        {
            public readonly System.Collections.Generic.List<lot> ll = new System.Collections.Generic.List<lot>();

            public void addlot(lot thislot)
            {
                ll.Add(thislot);
            }

            public System.Collections.Generic.IEnumerator<lot> GetEnumerator()
            {
                return ll.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

        }

        static public lots Lots;

        static public int numLots = 0;

        public class buyer
        {
            public string buyerId { get; set; }
            public string buyerName { get; set; }
            public string buyerCompany { get; set; }
            public string buyerType { get; set; }
            public string buyerWebCode { get; set; }
            public string buyerSiteCode { get; set; }
            public int Bids { get; set; }
            public double TotalClosingPrice { get; set; }
            public int numLots { get; set; }
            public lots Lots { get; set; }

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

        public class buyers : IEnumerable
        {
            public readonly System.Collections.Generic.List<buyer> bb = new System.Collections.Generic.List<buyer>(500);

            public bool addbuyer(buyer thisbuyer)
            {
                try
                {
                    if (thisbuyer.buyerCompany == null)
                    {
                        // How?
                        LogMsg("Add? " + thisbuyer.buyerName);
                        if ((bb.Find(x => x.buyerName == thisbuyer.buyerName)) == null)
                        {
                            LogMsg("Add " + thisbuyer.buyerName);
                            bb.Add(thisbuyer);
                            return true;
                        }
                        else
                        {
                            LogMsg("Buyer found");
                            return false;
                        }
                    }
                    if ((bb.Find(x => x.buyerName == thisbuyer.buyerName)) == null)
                    {
                        LogMsg("Add with company? " + thisbuyer.buyerName);
                        bb.Add(thisbuyer);
                        return true;
                    }

                    LogMsg("Buyer and company found");
                    return false;
                }
                catch
                {
                    LogMsg("Buyer name or company exception");
                    return false;
                }
            }

            public System.Collections.Generic.IEnumerator<buyer> GetEnumerator()
            {
                return bb.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

        }

        static public buyers Buyers;
        static public int numBuyers = 0;

        public class vendor
        {
            public string vendorId { get; set; }
            public string vendorCode { get; set; }
            public string vendorName { get; set; }
            public int Bids { get; set; }
            public int onlineBids { get; set; }
            public int hallBids { get; set; }
            public int numOnlineSales { get; set; }
            public int numHallSales { get; set; }
            public int numOnlineBidders { get; set; }
            public int numHallBidders { get; set; }
            public int numProvisionalOnlineSales { get; set; }
            public int numProvisionalHallSales { get; set; }
            public int numBuyers { get; set; }
            public int numOnlineBuyers { get; set; }
            public int numHallBuyers { get; set; }
            public double TotalClosingPrice { get; set; }
            public double minAddedValue { get; set; }
            public int numLots { get; set; }
            public lots Lots { get; set; }
        }

        public class vendors : IEnumerable
        {
            public readonly System.Collections.Generic.List<vendor> vv = new System.Collections.Generic.List<vendor>();

            public void addvendor(vendor thisvendor)
            {
                vv.Add(thisvendor);
            }

            public System.Collections.Generic.IEnumerator<vendor> GetEnumerator()
            {
                return vv.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

        }

        static public vendors Vendors;
        static public int numVendors = 0;

        public class Bidder
        {
            public string bidderId { get; set; }
            public string bidderName { get; set; }
            public string bidderCompany { get; set; }
            public int numBids { get; set; }
            public string bidderType { get; set; }
        }

        public class Bidders : IEnumerable
        {
            public readonly System.Collections.Generic.List<Bidder> dd = new System.Collections.Generic.List<Bidder>();

            public bool addbidder(Bidder thisbidder, bool client)
            {
                var b = dd.Where(d => ((d.bidderName == thisbidder.bidderName) && (d.bidderType == thisbidder.bidderType))).FirstOrDefault();
                if (b == null)
                {
                    if (client) thisbidder.numBids = 1;
                    dd.Add(thisbidder);
                    return true;
                }
                else
                {
                    if ((thisbidder.bidderName != null) && (thisbidder.bidderName.IndexOf("Mitesh") > -1))
                    {
                        int i = 0;
                    }
                    updatebidder(thisbidder, client);
                }
                return false;
            }

            public bool updatebidder(Bidder thisbidder, bool client)
            {
                var b = dd.Where(d => ((d.bidderName == thisbidder.bidderName) && (d.bidderType == thisbidder.bidderType))).FirstOrDefault();
                if (b != null)
                {
                    if (!client)
                    {
                        dd.Where(bb => bb.bidderName == b.bidderName).FirstOrDefault().numBids++;
                        dd.Where(bb => bb.bidderName == b.bidderName).FirstOrDefault().bidderType = thisbidder.bidderType;
                    }

                }
                return true;
            }

            public System.Collections.Generic.IEnumerator<Bidder> GetEnumerator()
            {
                return dd.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

        }

        static public Bidders onlineBidders;

        // Also record all online clients
        static public Bidders onlineClients = new Bidders();

        public class Sale
        {
            public int SaleNo { get; set; }
            public DateTime StartTime { get; set; }
            public string Description { get; set; }
            public string SiteName { get; set; }
            public int SiteId { get; set; }
            public int Lots { get; set; }
            public int SoldLots { get; set; }
            public int ProvisionallySoldLotsonline { get; set; }
            public int ProvisionallySoldLotsHall { get; set; }
            public int HallBids { get; set; }
            public int onlineBids { get; set; }
            public int mobileBids { get; set; }
            public int HallSales { get; set; }
            public int onlineSales { get; set; }
            public int mobileSales { get; set; }
            public int Bidders { get; set; }
            public int Buyers { get; set; }
            public double TotalClosingPrices { get; set; }
            public double MinAddedValue { get; set; }
            public int LotsWithOnlineBids { get; set; }
            public int ClientsOnline { get; set; }
        }

        public enum messageType { Auctioneer, AuctioneerToAll, Client };

        public class Message
        {
            public messageType type { get; set; }
            public string message { get; set; }
            public string clientId { get; set; }
            public string clientName { get; set; }
            public string clientCompany { get; set; }
        }

        static public Sale ThisSale = new Sale();
        static public string SalePrefix;

//        ReportCreation rc;

        private readonly static int saleid;

        public static void ShowTOS(int saleid)
        {

            int ii = 0;

//            PhocasData.ShowTranscriptOfSale.saleid = saleid;

//            InitializeComponent();

            ProcessXml(saleid);

            ii = 0;
            if (Transcript == null) return;
            string[] tokens = Regex.Split(Transcript, @"\r?\n|\r");

            for (ii = 0; ii < tokens.Length; ii++)
            {
//                lbTranscript.Items.Add(tokens[ii]);
            }
//            SaveTransactionLog.Enabled = true;
        }

        public static void ProcessXml(int CurrentSale)
        {
            string field1 = "";
            string field2 = "";
            string field3 = "";
            string field4 = "";
            string lot = "";
            string vehicleId = "";
            string bidderId = "";
            string vendorId = "";
            string userId = "";
            string saleId = "";
            string saleType = "";
            string SearchString = "";
            string highestOnlineBid = "";
            string highestHallBid = "";
            int clientCount = 0;

            onlineClients = new Bidders();

            var directory = new DirectoryInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                            @"Humboldt\AuctionController\transactionlogs"));

            directory = new DirectoryInfo("e:\\transactionlogs\\");

            if (directory.Exists == false)
            {
                directory.Create();
            }

            Transcript = "";
            if (CurrentSale > 0)
            {
                SearchString = CurrentSale.ToString() + "*.xml";
            }
            else
            {
                SearchString = "*.xml";
            }

            XmlTextReader reader;

            // Extract Sale Number from filename
            try
            {
                var myFile = directory.GetFiles(SearchString)
                 .OrderByDescending(f => f.LastWriteTime)
                 .First();

                SalePrefix = myFile.Name.Substring(0, (myFile.Name.IndexOf("_")));
                if (SalePrefix.Length < 3)
                {
                    LogMsg("Can't find XML transaction file");
                    return;
                }

                reader = new XmlTextReader(directory.FullName + "\\" + myFile.Name);
                //                reader = new XmlTextReader(directory.FullName + "\\" + "a2257_2017-09-28_08-59-34_Transaction.xml");
            }
            catch (IOException ie)
            {
                LogMsg(ie);
                return;
            }
            catch (IndexOutOfRangeException ie)
            {
                LogMsg(ie);
                return;
            }
            catch (InvalidOperationException ie)
            {
                // No files
                LogMsg(ie);
                return;
            }
            int len = reader.AttributeCount;

            tl = new transcript();
            transcriptline tr = new transcriptline();

            bool bBefore = true;

            try
            {
                while (reader.Read())
                {
                    switch (reader.NodeType)
                    {
                        case XmlNodeType.Element: // The node is an element.
                            if (reader.Name.IndexOf("transcript") > -1)
                            {
                                field1 = "";
                                field2 = "";
                                field3 = "";
                                field4 = "";
                            }

                            if (reader.Name.IndexOf("sale") > -1)
                            {
                                saleId = reader["id"];
                                saleType = reader["typeId"];
                                tr.saleId = saleId;
                                tr.saleType = saleType;
                            }
                            if (reader.Name.IndexOf("lot") > -1)
                            {
                                lot = reader["id"];
                            }
                            if (reader.Name.IndexOf("vehicle") > -1)
                            {
                                vehicleId = reader["id"];
                            }
                            if (reader.Name.IndexOf("bidder") > -1)
                            {
                                bidderId = reader["id"];
                                tr.bidderId = bidderId;
                            }
                            if (reader.Name.IndexOf("vendor") > -1)
                            {
                                vendorId = reader["id"];
                                tr.vendorId = vendorId;
                            }
                            if (reader.Name.IndexOf("user") > -1)
                            {
                                userId = reader["id"];
                                tr.bidderId = userId;
                            }

                            if (reader.Name.IndexOf("action") > -1)
                            {
                                tr.action = reader.Value;
                            }

                            if (reader.Name.IndexOf("message") > -1)
                            {
                                tr.auctioneerMessage = reader["message"];
                            }

                            if (reader.IsEmptyElement)
                            {
                                // Do nothing
                            }
                            else if (field1.Length == 0)
                            {
                                field1 = reader.Name;
                            }
                            else if (field2.Length == 0)
                            {
                                field2 = reader.Name;
                            }
                            else if (field3.Length == 0)
                            {
                                field3 = reader.Name;
                            }
                            else
                            {
                                field4 = reader.Name;
                            }
                            break;

                        case XmlNodeType.Text: //Display the text in each element.
                            if (field2.IndexOf("date") > -1)
                            {
                                tr.date = Convert.ToDateTime(reader.Value).ToString("dd/MM/yyyy HH:mm:ss.fff");
                                field2 = "";
                            }
                            if (field3.IndexOf("date") > -1)
                            {
                                tr.date = Convert.ToDateTime(reader.Value).ToString("dd/MM/yyyy HH:mm:ss.fff");
                                field3 = "";
                            }

                            if (field2.IndexOf("action") > -1)
                            {
                                tr.action = reader.Value;
                                field2 = "";
                            }
                            if (field3.IndexOf("action") > -1)
                            {
                                tr.action = reader.Value;
                                field3 = "";
                            }

                            if (field2.IndexOf("outcomeType") > -1)
                            {
                                tr.outcomeType = reader.Value;
                                field2 = "";
                            }

                            if (field2.IndexOf("bidAmount") > -1)
                            {
                                tr.bidValue = reader.Value;
                                field2 = "";
                            }
                            if (field2.IndexOf("newBidIncrement") > -1)
                            {
                                tr.nextBid = reader.Value;
                                field2 = "";
                            }
                            if (field2.IndexOf("retractBidAmount") > -1)
                            {
                                tr.retractBid = reader.Value;
                                field2 = "";
                            }
                            if (field2.IndexOf("users") > -1)
                            {
                                //tr.name = reader.Value;
                                field2 = "";
                            }
                            if (field2.IndexOf("name") > -1)
                            {
                                tr.name = reader.Value;
                                field2 = "";
                            }
                            if (field2.IndexOf("company") > -1)
                            {
                                tr.company = reader.Value;
                                field2 = "";
                            }
                            if (field2.IndexOf("origin") > -1)
                            {
                                tr.bidderType = reader.Value;
                                field2 = "";
                            }
                            if (field2.IndexOf("message") > -1)
                            {
                                if ((tr.action.IndexOf("Auctioneer message") > -1) && (tr.action.Length == 18))
                                {
                                    tr.auctioneerMessage = reader.Value;
                                }
                                if (tr.action.IndexOf("Auctioneer message to all") > -1)
                                {
                                    tr.auctioneerMessageToAll = reader.Value;
                                }
                                if (tr.action.IndexOf("Client message") > -1)
                                {
                                    tr.clientMessage = reader.Value;
                                }
                                field2 = "";
                            }

                            if (field3.Length > 0)
                            {
                                if (field3.IndexOf("description") > -1)
                                {
                                    tr.description = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("code") > -1)
                                {
                                    tr.saleCode = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("site") > -1)
                                {
                                    tr.siteName = reader.Value;
                                    field3 = "";
                                }

                                if (field3.IndexOf("make") > -1)
                                {
                                    tr.make = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("model") > -1)
                                {
                                    tr.model = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("lotNumber") > -1)
                                {
                                    tr.lot = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("registrationNumber") > -1)
                                {
                                    tr.registration = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("bidAmount") > -1)
                                {
                                    tr.bidValue = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("newBidIncrement") > -1)
                                {
                                    tr.nextBid = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("retractBidAmount") > -1)
                                {
                                    tr.retractBid = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("type") > -1)
                                {
                                    if ((reader.Value.IndexOf("Hall") > -1) || (reader.Value.IndexOf("Web") > -1) || (reader.Value.IndexOf("Mobile") > -1))
                                    {
                                        tr.bidderType = reader.Value;
                                        if ((tr.bidderType == "Web") || (tr.bidderType == "Mobile"))
                                        {
                                            highestOnlineBid = tr.bidValue;
                                        }
                                        if (tr.bidderType == "Hall")
                                        {
                                            highestHallBid = tr.bidValue;
                                        }
                                    }
                                    if ((reader.Value.IndexOf("Sold") > -1) || (reader.Value.IndexOf("Provisional") > -1))
                                    {
                                        // Check this with fix...
                                        if (tr.bidderId != null)
                                        {
                                            //                                            tr.bidderType = "Web";
                                            tr.highestOnlineBid = highestOnlineBid;
                                            highestOnlineBid = "";
                                            highestHallBid = "";
                                        }
                                        else
                                        {
                                            tr.bidderType = "Hall";
                                            tr.highestHallBid = highestHallBid;
                                            highestHallBid = "";
                                            highestOnlineBid = "";
                                        }
                                    }
                                    field3 = "";
                                }
                                if (field3.IndexOf("origin") > -1)
                                {
                                    tr.bidderType = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("webCode") > -1)
                                {
                                    tr.webCode = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("siteCode") > -1)
                                {
                                    tr.siteCode = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("name") > -1)
                                {
                                    tr.name = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("company") > -1)
                                {
                                    tr.company = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("max") > -1)
                                {
                                    tr.max = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("ask") > -1)
                                {
                                    tr.ask = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("user") > -1)
                                {
                                    userId = reader["id"];
                                    tr.bidderId = userId;
                                    field3 = "";
                                }
                            }

                            if (field4.Length > 0)
                            {
                                if (field4.IndexOf("name") > -1)
                                {
                                    tr.vendorName = reader.Value;
                                    tr.name = reader.Value;
                                    field4 = "";
                                }
                                if (field4.IndexOf("code") > -1)
                                {
                                    tr.vendorCode = reader.Value;
                                    field4 = "";
                                }
                                if (field4.IndexOf("company") > -1)
                                {
                                    tr.company = reader.Value;
                                    field4 = "";
                                }
                                if (field4.IndexOf("siteCode") > -1)
                                {
                                    tr.siteCode = reader.Value;
                                    field4 = "";
                                }
                                if (field4.IndexOf("webCode") > -1)
                                {
                                    tr.webCode = reader.Value;
                                    field4 = "";
                                }
                                if (field4.IndexOf("origin") > -1)
                                {
                                    tr.bidderType = reader.Value;
                                    field4 = "";
                                }
                            }

                            break;

                        case XmlNodeType.EndElement: //Display the end of the element.
                            //Transcript += "</" + reader.Name;
                            //Transcript += ">" + "\n";
                            if (reader.Name.IndexOf("transcripts") > -1)
                            {
                                Transcript += "\n";
                            }
                            else if (reader.Name.IndexOf("transcript") > -1)
                            {
                                Transcript += tr.date + " - ";

                                if (tr.action != null)
                                {
                                    if (tr.action.IndexOf("Lot Changed") > -1)
                                    {
                                        Transcript += "Lot Changed " + tr.lot + " - " + tr.make + " " + tr.model + " - " + tr.registration;
                                        // if (tr.enginesize.Length > 0) Transcript += tr.enginesize + "cc";
                                    }
                                    if (tr.action.IndexOf("Sale Closed") > -1)
                                    {
                                        Transcript += tr.action;
                                    }
                                    if (tr.action.IndexOf("Bidding Closed") > -1)
                                    {
                                        Transcript += tr.action;
                                    }
                                    if (tr.action.IndexOf("Selected") > -1)
                                    {
                                        Transcript += "Selected: Auction " + tr.saleCode + " - " + tr.siteName + " " + tr.description;
                                    }
                                    if (tr.action.IndexOf("Starting") > -1)
                                    {
                                        Transcript += "Starting: Lot " + tr.lot + " - " + tr.make + " " + tr.model + " at £" + tr.bidValue;
                                    }
                                    if (tr.action.IndexOf("Placing Proxy Bid") > -1)
                                    {
                                        Transcript += "Proxy Bid: Lot " + tr.lot + " - £" + tr.max + " for (" + tr.name + ", " + tr.company + ")";
                                    }
                                    if (tr.action.IndexOf("Bid Retracted") > -1)
                                    {
                                        if (tr.bidderId == null)
                                        {
                                            Transcript += "Bid Retracted: Lot " + tr.lot + " - £" + tr.retractBid + " for Hall Bidder";
                                        }
                                        else
                                        {
                                            Transcript += "Bid Retracted: Lot " + tr.lot + " - £" + tr.retractBid + " for (" + tr.name + ", " + tr.company + ")";
                                        }
                                    }
                                    if (tr.action.IndexOf("Online bid") > -1)
                                    {
                                        if (tr.bidderType == "Mobile")
                                        {
                                            Transcript += "Received Mobile Bid: " + tr.lot + " - £" + tr.bidValue + " for (" + tr.bidderId + ", " + tr.name + ", " + tr.company + ")";
                                        }
                                        else
                                        {
                                            Transcript += "Received Online Bid: " + tr.lot + " - £" + tr.bidValue + " for (" + tr.bidderId + ", " + tr.name + ", " + tr.company + ")";
                                        }
                                    }
                                    if (tr.action.IndexOf("Accepted Online Bid") > -1)
                                    {
                                        if (tr.bidderType == "Mobile")
                                        {
                                            Transcript += "Accepted Mobile Bid: " + tr.lot + " - £" + tr.bidValue + " for (" + tr.bidderId + ", " + tr.company + ")";
                                        }
                                        else
                                        {
                                            Transcript += "Accepted Online Bid: " + tr.lot + " - £" + tr.bidValue + " for (" + tr.bidderId + ", " + tr.company + ")";
                                        }
                                    }
                                    if ((tr.action.IndexOf("Placing") > -1) && (tr.action.Length == 7))
                                    {
                                        Transcript += "Proxy Bid: Lot " + tr.lot + " - £" + tr.max + " for (" + tr.name + ", " + tr.company + ")";
                                    }
                                    if (tr.action.IndexOf("Hall bidder") > -1)
                                    {
                                        Transcript += "Hall Bidder: Bid £" + tr.bidValue;
                                    }
                                    if (tr.action.IndexOf("Now taking bids at") > -1)
                                    {
                                        Transcript += "Now taking bids at £" + tr.bidValue;
                                    }
                                    if (tr.action.IndexOf("Online user Joined") > -1)
                                    {
                                        if (tr.bidderType != null)
                                        {
                                            if (tr.bidderType == "Web")
                                            {
                                                Transcript += "Online client joined " + tr.name + " " + tr.company;
                                            }
                                            if (tr.bidderType == "Mobile")
                                            {
                                                Transcript += "Mobile client joined " + tr.name + " " + tr.company;
                                            }
                                        }
                                        else
                                        {
                                            Transcript += "Online client joined " + tr.name + " " + tr.company;
                                        }
                                        Bidder thisclient = new Bidder();
                                        thisclient.bidderName = tr.name;
                                        thisclient.bidderId = tr.bidderId;
                                        thisclient.bidderCompany = tr.company;
                                        thisclient.bidderType = tr.bidderType;
                                        if (onlineClients.addbidder(thisclient, true))
                                        {
                                            clientCount++;
                                        }
                                    }
                                    if (tr.action.IndexOf("Online user Left") > -1)
                                    {
                                        if (tr.bidderType == "Mobile")
                                        {
                                            Transcript += "Mobile client left " + tr.bidderId + " " + tr.name + " " + tr.company;
                                        }
                                        else
                                        {
                                            Transcript += "Online client left " + tr.bidderId + " " + tr.name + " " + tr.company;
                                        }
                                    }
                                    if (tr.action.IndexOf("Sale Clients") > -1)
                                    {
                                        if (bBefore == true)
                                        {
                                            Transcript += "Dealers who logged before the sale\n";
                                            bBefore = false;
                                        }
                                        else
                                        {
                                            Transcript += "Dealers who were logged on at the end of sale\n";
                                        }
                                        // Currently not reading from log but from list built up from Online user joined
                                        foreach (Bidder thisclient in onlineClients)
                                        {
                                            if (thisclient.bidderType == "Mobile")
                                            {
                                                Transcript += "Mobile client " + thisclient.bidderName + " " + thisclient.bidderCompany + "\n";
                                            }
                                            else
                                            {
                                                if (thisclient.bidderType == "Web")
                                                {
                                                    Transcript += "Online client " + thisclient.bidderName + " " + thisclient.bidderCompany + "\n";
                                                }
                                            }
                                        }
                                    }
                                    if (tr.action.IndexOf("Outcome") > -1)
                                    {
                                        if (tr.outcomeType != null)
                                        {
                                            if (tr.outcomeType.IndexOf("Unsold") > -1)
                                            {
                                                Transcript += "Not Sold: Lot " + tr.lot + " - £" + tr.bidValue;
                                            }
                                            if (tr.outcomeType.IndexOf("Sold") > -1)
                                            {
                                                if ((tr.bidderType != null) && (tr.bidderType.IndexOf("Hall") > -1))
                                                {
                                                    Transcript += "Lot " + tr.lot + " Sold to Hall Bidder for £" + tr.bidValue;
                                                    tr.highestHallBid = tr.bidValue;
                                                    tr.highestOnlineBid = highestOnlineBid;
                                                }
                                                else
                                                {
                                                    if (tr.bidderType != null)
                                                    {
                                                        Transcript += "Lot " + tr.lot + " Sold to " + tr.bidderType + " Bidder " + tr.bidderId + " for £" + tr.bidValue + " for (" + tr.vendorCode + ", " + tr.vendorName + ")";
                                                    }
                                                    else
                                                    {
                                                        Transcript += "Lot " + tr.lot + " Sold to " + tr.bidderId + " for £" + tr.bidValue + " for (" + tr.vendorCode + ", " + tr.vendorName + ")";
                                                    }
                                                    tr.highestHallBid = highestHallBid;
                                                    tr.highestOnlineBid = tr.bidValue;
                                                }
                                            }
                                            if (tr.outcomeType.IndexOf("Retracted") > -1)
                                            {
                                                Transcript += "Retracted: Lot " + tr.lot + " - £" + tr.bidValue;
                                            }
                                            if (tr.outcomeType.IndexOf("Provisional") > -1)
                                            {

                                                if ((tr.bidderType != null) && (tr.bidderType.IndexOf("Hall") > -1))
                                                {
                                                    Transcript += "Lot " + tr.lot + " Provisionally Sold to Hall Bidder for £" + tr.bidValue;
                                                    tr.highestHallBid = tr.bidValue;
                                                    tr.highestOnlineBid = highestOnlineBid;
                                                }
                                                else
                                                {
                                                    if (tr.bidderType != null)
                                                    {
                                                        Transcript += "Lot " + tr.lot + " Provisionally Sold to " + "(" + tr.bidderType + " Bidder " + tr.bidderId + ", " + tr.company + ")" + " for " + tr.bidValue;
                                                    }
                                                    else
                                                    {
                                                        Transcript += "Lot " + tr.lot + " Provisionally Sold to " + "(" + tr.bidderId + ", " + tr.company + ")" + " for " + tr.bidValue;
                                                    }
                                                    tr.highestHallBid = highestHallBid;
                                                    tr.highestOnlineBid = tr.bidValue;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            // No action - currently assuming Vehicle entered
                                            Transcript += "Vehicle Entered " + tr.lot + " - " + tr.make + " " + tr.model + " " + tr.registration;
                                        }
                                    }
                                    if ((tr.action.IndexOf("Auctioneer message") > -1) && (tr.action.Length == 18))
                                    {
                                        Message msg = new Message();
                                        msg.type = messageType.Auctioneer;
                                        msg.clientId = tr.bidderId;
                                        msg.clientName = tr.name;
                                        msg.clientCompany = tr.company;
                                        msg.message = tr.auctioneerMessage;
                                        Transcript += "Auctioneer Message to " + msg.clientName + " [" + msg.message + "]";
                                    }
                                    if (tr.action.IndexOf("Auctioneer message to all") > -1)
                                    {
                                        Message msg = new Message();
                                        msg.type = messageType.AuctioneerToAll;
                                        msg.clientId = tr.bidderId;
                                        msg.clientName = tr.name;
                                        msg.clientCompany = tr.company;
                                        msg.message = tr.auctioneerMessageToAll;
                                        Transcript += "Auctioneer Message to all [" + msg.message + "]";
                                    }
                                    if (tr.action.IndexOf("Client message") > -1)
                                    {
                                        Message msg = new Message();
                                        msg.type = messageType.Client;
                                        msg.clientId = tr.bidderId;
                                        msg.clientName = tr.name;
                                        msg.clientCompany = tr.company;
                                        msg.message = tr.clientMessage;
                                        Transcript += "Client Message from " + msg.clientName + " [" + msg.message + "]";
                                    }
                                }
                                else
                                {
                                    if (tr.outcomeType != null)
                                    {
                                        if (tr.outcomeType.IndexOf("Unsold") > -1)
                                        {
                                            Transcript += "Not Sold: Lot " + tr.lot + " - £" + tr.bidValue;
                                        }
                                        if (tr.outcomeType.IndexOf("Sold") > -1)
                                        {
                                            if ((tr.bidderType != null) && (tr.bidderType.IndexOf("Hall") > -1))
                                            {
                                                Transcript += "Sold to Hall Bidder for £" + tr.bidValue;
                                                tr.highestHallBid = tr.bidValue;
                                                tr.highestOnlineBid = highestOnlineBid;
                                            }
                                            else
                                            {
                                                if (tr.bidderType != null)
                                                {
                                                    Transcript += "Sold to " + tr.bidderType + " Bidder " + tr.bidderId + " for £" + tr.bidValue + " for (" + tr.vendorName + ", " + tr.company + ")";
                                                }
                                                else
                                                {
                                                    Transcript += "Sold to " + tr.bidderId + " for £" + tr.bidValue + " for (" + tr.vendorName + ", " + tr.company + ")";
                                                }
                                                tr.highestHallBid = highestHallBid;
                                                tr.highestOnlineBid = tr.bidValue;
                                            }
                                        }
                                        if (tr.outcomeType.IndexOf("Retracted") > -1)
                                        {
                                            Transcript += "Retracted: Lot " + tr.lot + " - £" + tr.bidValue;
                                        }
                                        if (tr.outcomeType.IndexOf("Provisional") > -1)
                                        {

                                            if ((tr.bidderType != null) && (tr.bidderType.IndexOf("Hall") > -1))
                                            {
                                                Transcript += "Provisionally Sold to Hall Bidder for £" + tr.bidValue;
                                            }
                                            else
                                            {
                                                if (tr.bidderType != null)
                                                {
                                                    Transcript += "Provisionally Sold to " + "(" + tr.bidderType + " Bidder " + tr.bidderId + ", " + tr.company + ")" + " for " + tr.bidValue;
                                                }
                                                else
                                                {
                                                    Transcript += "Provisionally Sold to " + "(" + tr.bidderId + ", " + tr.company + ")" + " for " + tr.bidValue;
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        // No action - currently assuming Vehicle entered
                                        Transcript += "Vehicle Entered " + tr.lot + " - " + tr.make + " " + tr.model + " " + tr.registration;
                                    }
                                }
                                Transcript += "\n";
                                tl.addline(tr);
                                tr = new transcriptline();
                            }

                            if (reader.Name.IndexOf("vehicle") > -1)
                            {
                                field2 = "";
                            }
                            if (reader.Name.IndexOf("bidder") > -1)
                            {
                                field2 = "";
                            }
                            if (reader.Name.IndexOf("proxyBid") > -1)
                            {
                                field2 = "";
                            }
                            if (reader.Name.IndexOf("lotNumber") > -1)
                            {
                                //tr.lot = lot;
                                field3 = "";
                            }
                            if (reader.Name.IndexOf("vendor") > -1)
                            {
                                field3 = "";
                            }
                            if (reader.Name.IndexOf("name") > -1)
                            {
                                // Store name for list of online users
                                // Wait for the company
                                field2 = "";
                            }
                            if (reader.Name.IndexOf("company") > -1)
                            {
                                // Now wait for origin
                                /*                                // Store name for list of online users
                                                                Bidder thisclient = new Bidder();
                                                                if (field3.IndexOf("vendor") > -1)
                                                                {
                                                                    // Do nothing
                                                                }
                                                                else
                                                                {
                                                                    thisclient.bidderName = tr.name;
                                                                    if (tr.bidderId == null)
                                                                    {
                                                                        thisclient.bidderId = tr.name;
                                                                    }
                                                                    else
                                                                    {
                                                                        thisclient.bidderId = tr.bidderId;
                                                                    }
                                                                    thisclient.bidderCompany = tr.company;
                                                                    if (tr.bidderType != null)
                                                                    {
                                                                        thisclient.bidderType = tr.bidderType;
                                                                    }
                                                                    else
                                                                    {
                                                                        // thisclient.bidderType = "Web";
                                                                    }
                                                                    if (onlineClients.addbidder(thisclient, false))
                                                                    {
                                                                        clientCount++;
                                                                    }
                                                                    else
                                                                    {
                                                                        onlineClients.updatebidder(thisclient, false);
                                                                    }
                                                                }*/
                                field2 = "";
                            }

                            if (reader.Name.IndexOf("origin") > -1)
                            {
                                // Store name for list of online users
                                Bidder thisclient = new Bidder();
                                if (field3.IndexOf("vendor") > -1)
                                {

                                }
                                else
                                {
                                    thisclient.bidderName = tr.name;
                                    if (tr.bidderId == null)
                                    {
                                        thisclient.bidderId = tr.name;
                                    }
                                    else
                                    {
                                        thisclient.bidderId = tr.bidderId;
                                    }
                                    thisclient.bidderCompany = tr.company;
                                    if (tr.bidderType != null)
                                    {
                                        thisclient.bidderType = tr.bidderType;
                                    }
                                    else
                                    {
                                        //thisclient.bidderType = "Web";
                                    }
                                    if (onlineClients.addbidder(thisclient, false))
                                    {
                                        clientCount++;
                                    }
                                    else
                                    {
                                        onlineClients.updatebidder(thisclient, false);
                                    }
                                }
                                field2 = "";
                            }
                            break;

                        default:
                            //MessageBox.Show("Node Type " + reader.NodeType);
                            break;

                    }
                }
            }
            catch (IOException ie)
            {
                LogMsg(ie);
                return;
            }
            catch (FormatException fe)
            {
//                MessageBox.Show(fe.Message);
                LogMsg(fe);
            }
            catch (IndexOutOfRangeException ie)
            {
//                MessageBox.Show("Too many transaction records");
                LogMsg(ie);
            }
            catch (XmlException xe)
            {
//                MessageBox.Show(xe.Message);
                LogMsg(xe);
            }

            return;
        }

        private void CloseTranscript_Click(object sender, EventArgs e)
        {
//            this.Close();
        }

/*        private void SaveTransactionLog_Click(object sender, EventArgs e)
        {
            SaveFileDialog savefile;

            var directory = new DirectoryInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                            @"Humboldt\AuctionController\transactionlogs"));

            var myFile = directory.GetFiles()
             .OrderByDescending(f => f.LastWriteTime)
             .First();

            SaveTransactionLog.Enabled = false;
            CloseTranscript.Enabled = false;

            savefile = new SaveFileDialog();

            // set a default file name
            savefile.FileName = SalePrefix + "_" + "transaction.xls";
            // set filters - this can be done in properties as well
            // savefile.Filter = "Text files (*.txt)|*.txt|All files (*.*)|*.*";

#if saved
            if (savefile.ShowDialog() == DialogResult.OK)
            {
                using (StreamWriter sw = new StreamWriter(savefile.FileName))
                    sw.Write(Transcript);

                MessageBox.Show("File Saved");
            }
#endif
            Microsoft.Office.Interop.Excel.Application xlApp;

            try
            {
                xlApp = new Microsoft.Office.Interop.Excel.Application();

                if (xlApp == null)
                {
                    MessageBox.Show("Excel is not properly installed!!");
                    LogMsg("Excel is not properly installed!!");
                    return;
                }
            }

            catch (Exception)
            {
                MessageBox.Show("Excel is not available");
                LogMsg("Excel is not available");
                return;
            }

            Workbook xlWorkBook;
            Worksheet xlWorkSheet;
            object misValue = System.Reflection.Missing.Value;

            try
            {
                xlWorkBook = xlApp.Workbooks.Add(misValue);
                xlWorkSheet = (Worksheet)xlWorkBook.Worksheets.get_Item(1);
                xlWorkSheet.Name = "Transcript Report";

                Range er = xlWorkSheet.get_Range("A:A", System.Type.Missing);
                er.EntireColumn.ColumnWidth = 140;

                xlWorkSheet.get_Range("A1", "A1").Cells.Font.Size = 20;
                xlWorkSheet.get_Range("A2", "A2").Cells.Font.Size = 16;
                xlWorkSheet.get_Range("A3", "A4").Cells.Font.Size = 12;

                xlWorkSheet.PageSetup.Application.ActiveWindow.DisplayGridlines = false;

                xlWorkSheet.get_Range("A1", "A2").Cells.Font.Color = Color.ForestGreen;
                xlWorkSheet.get_Range("A4", "A4").Interior.Color = Color.ForestGreen;
                xlWorkSheet.get_Range("A4", "A4").Cells.Font.Color = Color.White;

                xlWorkSheet.Cells[1, 1] = "Live Bid";
                xlWorkSheet.Cells[2, 1] = "Transcript Report";
                if (saleid > -1)
                {
                    xlWorkSheet.Cells[3, 1] = "Sale Number " + saleid;
                }
                xlWorkSheet.Cells[4, 1] = "Transcript Text";
                int ii = 5;
                using (StringReader sr = new StringReader(Transcript))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        xlWorkSheet.Cells[ii, 1] = line;
                        ii++;
                    }
                }
                xlWorkSheet.get_Range("A5", "A" + ii).Cells.Font.Size = 10;
                xlWorkSheet.get_Range("A5", "A" + ii).Cells.Borders.LineStyle = XlLineStyle.xlContinuous;
                xlWorkSheet.get_Range("A5", "A" + ii).Cells.Borders.Color = Color.Gray;

                xlApp.DisplayAlerts = false;

                xlWorkBook.SaveAs(directory + "\\" + savefile.FileName, XlFileFormat.xlWorkbookNormal, misValue, misValue, misValue, misValue, XlSaveAsAccessMode.xlExclusive, misValue, misValue, misValue, misValue, misValue);
                xlWorkBook.Close(true, misValue, misValue);
                xlApp.Quit();
            }
            catch (Exception ee)
            {
                MessageBox.Show(ee.Message);
                LogMsg(ee);
            }

            // Process transaction log for totals etc.
            ProcessTransactionLog();

            if (backgroundWorker1.IsBusy != true)
            {
                // create a new instance of the alert form
                rc = new ReportCreation();

                // event handler for the Cancel button in AlertForm
                rc.Canceled += new EventHandler<EventArgs>(buttonCancel_Click);

                rc.Show();

                // Start the asynchronous operation.
                backgroundWorker1.RunWorkerAsync();
            }

            backgroundWorker1.ReportProgress(5);

            //SaveTransactionLog.Enabled = true;
            CloseTranscript.Enabled = true;

        }

        // This event handler cancels the backgroundworker, fired from Cancel button in AlertForm.
        private void buttonCancel_Click(object sender, EventArgs e)
        {
            if (backgroundWorker1.WorkerSupportsCancellation == true)
            {
                // Cancel the asynchronous operation.
                backgroundWorker1.CancelAsync();

                // Close the form
                rc.Close();
            }
        }

        // This event handler updates the progress.
        private void backgroundWorker1_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            // Show the progress in main form (GUI)
            labelResult.Text = "Creating Reports " + (e.ProgressPercentage.ToString() + "% Complete");
            // Pass the progress to AlertForm label and progressbar
            rc.Message = "In progress, please wait... " + e.ProgressPercentage.ToString() + "%";
            rc.ProgressValue = e.ProgressPercentage;
        }

        private void backgroundWorker1_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker worker = sender as BackgroundWorker;

            if (worker.CancellationPending == true)
            {
                e.Cancel = true;
                return;
            }
            else
            {
                worker.ReportProgress(10);

                // Save Online Sales Report
                SaveOnlineSalesReport(saleid);
                worker.ReportProgress(20);

                // Save Clients Online Report
                SaveClientsOnlineReport(saleid);
                worker.ReportProgress(40);

                // Save Global Online Report
                SaveGlobalOnlineReport();
                worker.ReportProgress(60);

                // Save Vendor Online Report
                SaveVendorOnlineReport();
                worker.ReportProgress(80);

                // Save Sale Report
                SaveSaleReport(saleid);
                worker.ReportProgress(100);

            }

        }

        // This event handler deals with the results of the background operation.
        private void backgroundWorker1_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Cancelled == true)
            {
                labelResult.Text = "Canceled!";
            }
            else if (e.Error != null)
            {
                labelResult.Text = "Error: " + e.Error.Message;
            }
            else
            {
                labelResult.Text = "Done!";
            }

            rc.Close();
        }

        */
        // Work out lots per buyer, total clients etc.
        static public void ProcessTransactionLog()
        {
            int numUnsolds = 0;
            int numRetracted = 0;
            int numSoldProvisionallyonline = 0;
            int numSoldProvisionallyHall = 0;
            int numSold = 0;
            int numHallBids = 0;
            int numonlineBids = 0;
            int numonlineBidders = 0;
            int numOnlineSales = 0;
            int numHallSales = 0;
            int numMobileSales = 0;
            int numMobileBids = 0;
            double minAddedValue = 0.0;
            double TotalClosingPrice = 0.0;
            string SaleDescription = "";
            string SaleCode = "";
            DateTime SaleStart;
            string SiteName = "";
            string lastlot = "";
            int lotswithonlinebids = 0;

            numBuyers = 0;
            numLots = 0;
            numVendors = 0;

            try
            {
                // Initialise SaleStart
                SaleStart = Convert.ToDateTime("1/1/2001");

                Lots = new lots();
                onlineBidders = new Bidders();

                foreach (transcriptline tr in tl)
                {
                    if (tr.outcomeType != null)
                    {
                        if ((tr.outcomeType.IndexOf("Sold") == 0) || (tr.outcomeType.IndexOf("Provisional") == 0))
                        {
                            lot thislot = new lot();
                            thislot.lotNumber = tr.lot;
                            if (tr.name == tr.vendorName)
                            {
                                thislot.buyerId = tr.bidderId;
                            }
                            else
                            {
                                thislot.buyerName = tr.name;
                            }
                            thislot.buyerId = tr.bidderId;
                            if ((tr.bidderType == "Web") || (tr.bidderType == "Mobile")) thislot.buyerCompany = tr.company;
                            thislot.buyerSiteCode = tr.siteCode;
                            thislot.buyerWebCode = tr.webCode;
                            thislot.buyerType = tr.bidderType;
                            thislot.closingBid = tr.bidValue;
                            thislot.outcomeType = tr.outcomeType;
                            thislot.vendorId = tr.vendorId;
                            thislot.vendorName = tr.vendorName;
                            thislot.vendorCode = tr.vendorCode;
                            thislot.highestHallBid = tr.highestHallBid;
                            thislot.highestOnlineBid = tr.highestOnlineBid;
                            if ((thislot.highestOnlineBid != null) && (thislot.highestHallBid != null))
                            {
                                if ((thislot.highestOnlineBid.Length > 0) && (thislot.highestHallBid.Length > 0))
                                {
                                    if (Convert.ToDouble(thislot.highestOnlineBid) > Convert.ToDouble(thislot.highestHallBid))
                                    {
                                        minAddedValue += (Convert.ToDouble(thislot.highestOnlineBid) - Convert.ToDouble(thislot.highestHallBid));
                                    }
                                }
                            }
                            thislot.registration = tr.registration;
                            thislot.description = tr.make + " " + tr.model + " " + tr.description;
                            Lots.addlot(thislot);
                            numLots++;
                        }

                        if (tr.outcomeType.IndexOf("Provisional") == 0)
                        {
                            if (tr.bidderType != null)
                            {
                                if ((tr.bidderType.IndexOf("Web") > -1) || (tr.bidderType.IndexOf("Mobile") > -1))
                                {
                                    numSoldProvisionallyonline++;
                                }
                                if (tr.bidderType.IndexOf("Hall") > -1)
                                {
                                    numSoldProvisionallyHall++;
                                }
                            }
                        }
                        if (tr.outcomeType.IndexOf("Sold") == 0)
                        {
                            numSold++;
                        }
                        if (tr.outcomeType.IndexOf("Unsold") == 0)
                        {
                            numUnsolds++;
                        }
                        if (tr.outcomeType.IndexOf("Retracted") == 0)
                        {
                            numRetracted++;
                        }
                    }
                    if (tr.action != null)
                    {
                        if (tr.action.IndexOf("Hall") == 0)
                        {
                            numHallBids++;
                        }
                        if (tr.action.IndexOf("Online bid") == 0)
                        {
                            numonlineBids++;
                            if ((tr.bidderType != null) && (tr.bidderType.IndexOf("Mobile") > -1))
                            {
                                numMobileBids++;
                            }
                            if (tr.lot != lastlot)
                            {
                                lotswithonlinebids++;
                                lastlot = tr.lot;
                            }

                            string bidderId = tr.bidderId;

                            UpdateBidderCounts(bidderId,
                                                tr.name,
                                                tr.company,
                                                tr.bidderType);
                        }
                        if (tr.action.IndexOf("Accepted Online Bid") == 0)
                        {
                            string bidderId = tr.bidderId;
                        }
                        if (tr.action.IndexOf("Selected") == 0)
                        {
                            SaleDescription = tr.description;
                            try
                            {
                                //tr.date = "13/13/2015";
                                //SaleStart = Convert.ToDateTime(tr.date);
                                DateTime.TryParse(tr.date, out SaleStart);
                            }
                            catch (FormatException fe)
                            {
                                LogMsg("Date " + tr.date + " " + fe.Message);
                            }
                            SaleCode = tr.saleCode;
                            SiteName = tr.siteName;
                        }
                        // Check for online clients - rough guide
                        if (tr.action.IndexOf("Online user Joined") > -1)
                        {
                            ThisSale.ClientsOnline++;
                        }
                        if (tr.action.IndexOf("Online user Left") > -1)
                        {
                            ThisSale.ClientsOnline--;
                        }
                    }
                }

                // Calculate number of unique online clients
                int uniqueClients = 0;
                Bidders uc = new Bidders();
                foreach (Bidder thisbidder in onlineClients)
                {
                    if (thisbidder.bidderId != null)
                    {
                        bool bfound = false;
                        foreach (Bidder thisclient in uc)
                        {
                            if ((String.Equals(thisbidder.bidderName, thisclient.bidderName)) &&
                                (String.Equals(thisbidder.bidderCompany, thisclient.bidderCompany)) &&
                                (String.Equals(thisbidder.bidderType, thisclient.bidderType)))
                            {
                                bfound = true;
                                break;
                            }
                        }
                        if (bfound == false)
                        {
                            uc.addbidder(thisbidder, true);
                            uniqueClients++;
                        }
                    }
                }
                ThisSale.ClientsOnline = uniqueClients;
                onlineClients = uc;

                Buyers = new buyers();
                Vendors = new vendors();

                foreach (lot thislot in Lots)
                {
                    // Do we already know this buyer?
                    bool bFound = false;
                    foreach (buyer thisbuyer in Buyers)
                    {
                        if (thislot.buyerId == thisbuyer.buyerId)
                        {
                            thisbuyer.TotalClosingPrice += Convert.ToDouble(thislot.closingBid);
                            thisbuyer.Lots.addlot(thislot);
                            thisbuyer.numLots++;
                            bFound = true;
                            break;
                        }
                    }
                    if (bFound == false)
                    {
                        buyer thisbuyer = new buyer();

                        thisbuyer.buyerId = thislot.buyerId;
                        thisbuyer.buyerName = thislot.buyerName;
                        thisbuyer.buyerCompany = thislot.buyerCompany;
                        thisbuyer.buyerSiteCode = thislot.buyerSiteCode;
                        thisbuyer.buyerWebCode = thislot.buyerWebCode;
                        thisbuyer.buyerType = thislot.buyerType;
                        thisbuyer.TotalClosingPrice = Convert.ToDouble(thislot.closingBid);
                        thisbuyer.numLots = 1;
                        thisbuyer.Lots = new lots();
                        thisbuyer.Lots.addlot(thislot);
                        Buyers.addbuyer(thisbuyer);

                        if (thisbuyer.buyerId != null)
                        {
                            numBuyers++;
                        }
                    }

                    if (thislot.buyerType != null)
                    {
                        if (thislot.buyerType.IndexOf("Hall") > -1)
                        {
                            numHallSales++;
                        }
                        if (thislot.buyerType.IndexOf("Web") > -1)
                        {
                            numOnlineSales++;
                        }
                        if (thislot.buyerType.IndexOf("Mobile") > -1)
                        {
                            numMobileSales++;
                        }
                    }

                    TotalClosingPrice += Convert.ToDouble(thislot.closingBid);

                    // Do we already know this vendor?
                    bFound = false;
                    foreach (vendor thisvendor in Vendors)
                    {
                        if (thislot.vendorId == thisvendor.vendorId)
                        {
                            thisvendor.TotalClosingPrice += Convert.ToDouble(thislot.closingBid);
                            thisvendor.Lots.addlot(thislot);
                            thisvendor.numLots++;
                            if (thislot.buyerType != null)
                            {
                                if (thislot.buyerType.IndexOf("Web") > -1) thisvendor.numOnlineSales++;
                                if (thislot.buyerType.IndexOf("Hall") > -1) thisvendor.numHallSales++;
                            }
                            if (thislot.outcomeType.IndexOf("Provisional") > -1)
                            {
                                if (thislot.buyerType != null)
                                {
                                    if (thislot.buyerType.IndexOf("Web") > -1) thisvendor.numProvisionalOnlineSales++;
                                    if (thislot.buyerType.IndexOf("Hall") > -1) thisvendor.numProvisionalHallSales++;
                                }
                            }
                            if (thislot.highestOnlineBid != null) thisvendor.onlineBids++;
                            if (thislot.highestHallBid != null) thisvendor.hallBids++;
                            if ((thislot.highestOnlineBid != null) && (thislot.highestHallBid != null))
                            {
                                if ((thislot.highestOnlineBid.Length > 0) && (thislot.highestHallBid.Length > 0))
                                {
                                    if (Convert.ToDouble(thislot.highestOnlineBid) > Convert.ToDouble(thislot.highestHallBid))
                                    {
                                        thisvendor.minAddedValue += (Convert.ToDouble(thislot.highestOnlineBid) - Convert.ToDouble(thislot.highestHallBid));
                                    }
                                }
                            }

                            bFound = true;
                            break;
                        }
                    }
                    if (bFound == false)
                    {
                        vendor thisvendor = new vendor();

                        thisvendor.vendorId = thislot.vendorId;
                        //thisvendor.vendorCode = thislot.vendorCode;
                        thisvendor.vendorCode = thislot.vendorId;
                        thisvendor.vendorName = thislot.vendorName;
                        thisvendor.TotalClosingPrice = Convert.ToDouble(thislot.closingBid);
                        thisvendor.numLots = 1;
                        if (thislot.buyerType != null)
                        {
                            if (thislot.buyerType.IndexOf("Web") > -1) thisvendor.numOnlineSales = 1;
                            if (thislot.buyerType.IndexOf("Hall") > -1) thisvendor.numHallSales = 1;
                            if (thislot.highestOnlineBid != null) thisvendor.onlineBids = 1;
                        }
                        if (thislot.highestHallBid != null) thisvendor.hallBids = 1;
                        if (thislot.outcomeType.IndexOf("Provisional") > -1)
                        {
                            if (thislot.buyerType != null)
                            {
                                if (thislot.buyerType.IndexOf("Web") > -1) thisvendor.numProvisionalOnlineSales = 1;
                                if (thislot.buyerType.IndexOf("Hall") > -1) thisvendor.numProvisionalHallSales = 1;
                                if (thislot.buyerType.IndexOf("Provisional") > -1)
                                {
                                    if (thislot.buyerId.Length > 1)
                                    {
                                        thisvendor.numProvisionalOnlineSales++;
                                    }
                                    else
                                    {
                                        thisvendor.numProvisionalHallSales++;
                                    }
                                }
                            }
                        }
                        thisvendor.Lots = new lots();
                        thisvendor.Lots.addlot(thislot);

                        Vendors.addvendor(thisvendor);

                        numVendors++;
                    }
                }

                numonlineBidders = 0;
                foreach (Bidder thisbidder in onlineBidders)
                {
                    if (thisbidder.numBids > 0)
                    {
                        numonlineBidders++;
                    }
                }

                // Fill in Sale values
                ThisSale.Bidders = numonlineBidders;
                ThisSale.Buyers = numBuyers;
                ThisSale.Description = SaleDescription;
                ThisSale.HallBids = numHallBids;
                ThisSale.Lots = numSold + numUnsolds + numRetracted + numSoldProvisionallyHall + numSoldProvisionallyonline;
                ThisSale.onlineBids = numonlineBids;
                ThisSale.mobileBids = numMobileBids;
                ThisSale.onlineSales = numOnlineSales;
                ThisSale.mobileSales = numMobileSales;
                ThisSale.HallSales = numHallSales;
                ThisSale.ProvisionallySoldLotsHall = numSoldProvisionallyHall;
                ThisSale.ProvisionallySoldLotsonline = numSoldProvisionallyonline;
                ThisSale.MinAddedValue = minAddedValue;
                ThisSale.LotsWithOnlineBids = lotswithonlinebids;
                ThisSale.SaleNo = Convert.ToInt32(SaleCode);
                ThisSale.SoldLots = numSold;
                ThisSale.StartTime = SaleStart.ToLocalTime();
                ThisSale.SiteName = SiteName;
                switch (SiteName)
                {
                    case "Aston Barclay Chelmsford":
                        ThisSale.SiteId = 1;
                        break;
                    case "Aston Barclay Prees Heath":
                        ThisSale.SiteId = 659779;
                        break;
                    case "Aston Barclay Westbury":
                        ThisSale.SiteId = 659780;
                        break;
                    case "Aston Barclay Leeds":
                        ThisSale.SiteId = 2360542;
                        break;
                    case "Aston Barclay Donington Park":
                        ThisSale.SiteId = 20846447;
                        break;
                    default:
                        break;
                }
            }
            catch (IOException ie)
            {
                LogMsg(ie);
            }
            catch (FormatException fe)
            {
                LogMsg(fe);
            }

            return;
        }

        static private void UpdateBidderCounts(string bidderId, string bidderName, string bidderCompany, string bidderType)
        {
            bool bFound = false;

            // Look for this bidderId
            foreach (Bidder thisbidder in onlineBidders)
            {
                //if (thisbidder.bidderId == bidderId)
                if (bidderId.Length > 0)
                {
                    if (thisbidder.bidderId.IndexOf(bidderId) > -1)
                    {
                        var obj = onlineBidders.dd.FirstOrDefault(x => x.bidderId == bidderId);
                        if (obj != null) obj.numBids++;
                        bFound = true;
                        break;
                    }
                }
                else
                {
                    if (thisbidder.bidderName.IndexOf(bidderName) > -1)
                    {
                        var obj = onlineBidders.dd.FirstOrDefault(x => x.bidderName == bidderName);
                        if (obj != null) obj.numBids++;
                        bFound = true;
                        break;
                    }
                }
            }
            if (bFound == false)
            {
                Bidder thisbidder = new Bidder();

                thisbidder.bidderId = bidderId;
                thisbidder.bidderName = bidderName;
                thisbidder.bidderCompany = bidderCompany;
                thisbidder.bidderType = bidderType;
                thisbidder.numBids = 1;

                onlineBidders.addbidder(thisbidder, true);

            }

            return;
        }

/*        public static void SaveOnlineSalesReport(int saleid)
        {
            var directory = new DirectoryInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                            @"Humboldt\AuctionController\transactionlogs"));
            string filename = SalePrefix + "_" + "onlinesales.xls";
            double onlineSalesTotal = 0.0;

            Microsoft.Office.Interop.Excel.Application xlApp = new Microsoft.Office.Interop.Excel.Application();

            if (xlApp == null)
            {
                MessageBox.Show("Excel is not properly installed!!");
                LogMsg("Excel is not properly installed!!");
                return;
            }

            Workbook xlWorkBook;
            Worksheet xlWorkSheet;
            object misValue = System.Reflection.Missing.Value;

            try
            {
                xlWorkBook = xlApp.Workbooks.Add(misValue);
                xlWorkSheet = (Worksheet)xlWorkBook.Worksheets.get_Item(1);
                xlWorkSheet.Name = "Online Sales Report";

                xlWorkSheet.get_Range("A1", "A1").Cells.Font.Size = 20;
                xlWorkSheet.get_Range("A2", "A2").Cells.Font.Size = 16;
                xlWorkSheet.get_Range("A3", "A4").Cells.Font.Size = 12;

                xlWorkSheet.get_Range("A1", "A1").EntireColumn.ColumnWidth = 40;
                xlWorkSheet.get_Range("B1", "B1").EntireColumn.ColumnWidth = 30;
                xlWorkSheet.get_Range("C1", "C1").EntireColumn.ColumnWidth = 50;
                xlWorkSheet.get_Range("D1", "D1").EntireColumn.ColumnWidth = 30;
                xlWorkSheet.get_Range("E1", "E1").EntireColumn.ColumnWidth = 20;

                xlWorkSheet.PageSetup.Application.ActiveWindow.DisplayGridlines = false;

                xlWorkSheet.get_Range("A1", "A2").Cells.Font.Color = Color.ForestGreen;
                xlWorkSheet.get_Range("A4", "E4").Interior.Color = Color.ForestGreen;
                xlWorkSheet.get_Range("A4", "E4").Cells.Font.Color = Color.White;
                xlWorkSheet.get_Range("E1", "E1").EntireColumn.HorizontalAlignment = XlHAlign.xlHAlignRight;


                xlWorkSheet.Cells[1, 1] = "Live Bid";
                xlWorkSheet.Cells[2, 1] = "Online Sales Report";
                if (saleid > -1)
                {
                    xlWorkSheet.Cells[3, 1] = "Sale Number " + saleid;
                }
                xlWorkSheet.Cells[4, 1] = "Buyer Name";
                xlWorkSheet.Cells[4, 2] = "Registration";
                xlWorkSheet.Cells[4, 3] = "Description";
                xlWorkSheet.Cells[4, 4] = "Lot";
                xlWorkSheet.Cells[4, 5] = "Bid Amount";

                int ii = 5;

                foreach (buyer thisbuyer in Buyers)
                {
                    if ((thisbuyer.buyerType == null) || (thisbuyer.buyerType.IndexOf("Web") > -1))
                    {
                        foreach (lot thislot in thisbuyer.Lots)
                        {
                            xlWorkSheet.Cells[ii, 1] = thisbuyer.buyerName + " (" + thisbuyer.buyerCompany + ")";
                            xlWorkSheet.Cells[ii, 2] = thislot.registration;
                            xlWorkSheet.Cells[ii, 3] = thislot.description;
                            xlWorkSheet.Cells[ii, 4] = thislot.lotNumber;
                            xlWorkSheet.Cells[ii, 5] = "£" + thislot.closingBid;
                            ii++;
                        }
                        xlWorkSheet.Cells[ii, 4] = "Buyer: " + thisbuyer.buyerName + "\n(" + thisbuyer.buyerCompany + ")\n" + "Total";
                        xlWorkSheet.get_Range("D" + ii, "D" + ii).Cells.Font.Bold = true;
                        xlWorkSheet.get_Range("D" + ii, "D" + ii).Cells.WrapText = true;
                        xlWorkSheet.get_Range("D" + ii, "D" + ii).Cells.HorizontalAlignment = XlHAlign.xlHAlignRight;
                        xlWorkSheet.get_Range("D" + ii, "E" + ii).Cells.Interior.Color = Color.LightGray;
                        xlWorkSheet.Cells[ii, 5] = "£" + thisbuyer.TotalClosingPrice;
                        onlineSalesTotal += thisbuyer.TotalClosingPrice;
                        ii++;
                    }
                    if ((thisbuyer.buyerType != null) && (thisbuyer.buyerType.IndexOf("Mobile") > -1))
                    {
                        foreach (lot thislot in thisbuyer.Lots)
                        {
                            xlWorkSheet.Cells[ii, 1] = thisbuyer.buyerName + " (" + thisbuyer.buyerCompany + ")";
                            xlWorkSheet.Cells[ii, 2] = thislot.registration;
                            xlWorkSheet.Cells[ii, 3] = thislot.description;
                            xlWorkSheet.Cells[ii, 4] = thislot.lotNumber;
                            xlWorkSheet.Cells[ii, 5] = "£" + thislot.closingBid;
                            ii++;
                        }
                        xlWorkSheet.Cells[ii, 4] = "Mobile Buyer: " + thisbuyer.buyerName + "\n(" + thisbuyer.buyerCompany + ")\n" + "Total";
                        xlWorkSheet.get_Range("D" + ii, "D" + ii).Cells.Font.Bold = true;
                        xlWorkSheet.get_Range("D" + ii, "D" + ii).Cells.WrapText = true;
                        xlWorkSheet.get_Range("D" + ii, "D" + ii).Cells.HorizontalAlignment = XlHAlign.xlHAlignRight;
                        xlWorkSheet.get_Range("D" + ii, "E" + ii).Cells.Interior.Color = Color.LightGray;
                        xlWorkSheet.Cells[ii, 5] = "£" + thisbuyer.TotalClosingPrice;
                        onlineSalesTotal += thisbuyer.TotalClosingPrice;
                        ii++;
                    }
                }

                xlWorkSheet.get_Range("A5", "E" + (ii - 1)).Cells.Font.Size = 10;
                xlWorkSheet.get_Range("A5", "E" + (ii - 1)).Cells.Borders.LineStyle = XlLineStyle.xlContinuous;
                xlWorkSheet.get_Range("A5", "E" + (ii - 1)).Cells.Borders.Color = Color.Gray;
                xlWorkSheet.get_Range("D" + ii, "D" + ii).Cells.HorizontalAlignment = XlHAlign.xlHAlignRight;

                // Make sure totals appear after table
                if (ii == 5)
                {
                    ii++;
                }

                xlWorkSheet.Cells[ii, 4] = "Online Sales Total";
                xlWorkSheet.Cells[ii, 5] = "£" + onlineSalesTotal;

                xlApp.DisplayAlerts = false;

                xlWorkBook.SaveAs(directory + "\\" + filename, XlFileFormat.xlWorkbookNormal, misValue, misValue, misValue, misValue, XlSaveAsAccessMode.xlExclusive, misValue, misValue, misValue, misValue, misValue);
                xlWorkBook.Close(true, misValue, misValue);
                xlApp.Quit();

                xlWorkSheet = null;
                xlWorkBook = null;
                GC.Collect();
            }
            catch (Exception ee)
            {
                MessageBox.Show(ee.Message);
                LogMsg(ee);
            }
            finally
            {
                xlApp.Quit();
                xlWorkSheet = null;
                xlWorkBook = null;
                GC.Collect();
            }

            return;
        }
*/
        public static string SaveBidData(int saleid)
        {
            var directory = new DirectoryInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                            @"Humboldt\AuctionController\transactionlogs"));
            string filename = SalePrefix + "_" + "clientsonline.xls";
            int totalBids = 0;
            int totalClients = 0;
            string csv = "";

            object misValue = System.Reflection.Missing.Value;

            try
            {

                if (saleid > -1)
                {
                    csv += saleid + ",";
                    csv += ThisSale.SiteId + ",";
                    csv += ThisSale.StartTime + ",";
                    csv += ThisSale.Lots + ",";
                    csv += ThisSale.HallBids + ",";
                }
//                xlWorkSheet.Cells[4, 1] = "Buyer Name / Code";
//                xlWorkSheet.Cells[4, 2] = "Company";
//                xlWorkSheet.Cells[4, 3] = "Online Bids Placed";
//                xlWorkSheet.Cells[4, 4] = "Mobile Bids Placed";

                int ii = 5;

                int mobileBidders = 0;
                foreach (Bidder thisbidder in onlineBidders)
                {
                    if ((thisbidder.bidderId != null) && (thisbidder.numBids > 0))
                    {
                        var obj = onlineClients.dd.FirstOrDefault(x => x.bidderId == thisbidder.bidderId);
                        if (obj != null)
                        {
//                            xlWorkSheet.Cells[ii, 1] = obj.bidderName + " / " + obj.bidderId;
//                            xlWorkSheet.Cells[ii, 2] = obj.bidderCompany;
                        }
                        else
                        {
//                            xlWorkSheet.Cells[ii, 1] = thisbidder.bidderName + " / " + thisbidder.bidderId;
//                            xlWorkSheet.Cells[ii, 2] = thisbidder.bidderCompany;
                        }
                        if ((thisbidder.bidderType != null) && (thisbidder.bidderType.IndexOf("Mobile") > -1))
                        {
//                            xlWorkSheet.Cells[ii, 4] = thisbidder.numBids;
                            mobileBidders ++;
                        }
                        else
                        {
//                            xlWorkSheet.Cells[ii, 3] = thisbidder.numBids;
                        }
                        totalBids += thisbidder.numBids;
                        totalClients++;
                        ii++;
                    }
                }

                // Make sure totals appear after table
                if (ii == 5)
                {
                    ii++;
                }

//                xlWorkSheet.Cells[ii, 4] = "Total Bidders";
//                xlWorkSheet.Cells[ii, 5] = totalClients;
                csv += totalClients + ",";
//                xlWorkSheet.Cells[ii + 1, 4] = "Total Online Bids inc Mobile";
//                xlWorkSheet.Cells[ii + 1, 5] = ThisSale.onlineBids;
                csv += ThisSale.onlineBids + ",";
//                xlWorkSheet.Cells[ii + 2, 4] = "Total Mobile Bids";
//                xlWorkSheet.Cells[ii + 2, 5] = ThisSale.mobileBids;
                csv += mobileBidders + ",";
                csv += ThisSale.mobileBids + ",";

                ii++;
                ii++;
                ii++;
                ii++;

//                xlWorkSheet.get_Range("A" + ii, "E" + ii).Interior.Color = Color.ForestGreen;
//                xlWorkSheet.get_Range("A" + ii, "E" + ii).Cells.Font.Color = Color.White;

//                xlWorkSheet.Cells[ii, 1] = "Buyer Name / Code";
//                xlWorkSheet.Cells[ii, 2] = "Company";
//                xlWorkSheet.Cells[ii, 3] = "Bidder Type";
                ii++;

                int mb = 0;
                foreach (Bidder thisclient in onlineClients)
                {
                    if (thisclient.bidderId != null)
                    {
//                        xlWorkSheet.Cells[ii, 1] = thisclient.bidderName + " / " + thisclient.bidderId;
//                        xlWorkSheet.Cells[ii, 2] = thisclient.bidderCompany;
//                        xlWorkSheet.Cells[ii, 3] = thisclient.bidderType;
                        if (thisclient.bidderType == "Mobile")
                        {
                            mb++;
                        }
                        ii++;
                    }
                }

//                xlWorkSheet.get_Range("A5", "E" + (ii - 1)).Cells.Font.Size = 10;
//                xlWorkSheet.get_Range("A5", "E" + (ii - 1)).Cells.Borders.LineStyle = XlLineStyle.xlContinuous;
//                xlWorkSheet.get_Range("A5", "E" + (ii - 1)).Cells.Borders.Color = Color.Gray;

                ii++;

//                xlWorkSheet.Cells[ii, 4] = "Total Clients";
//                xlWorkSheet.Cells[ii, 5] = ThisSale.ClientsOnline;
                csv += ThisSale.ClientsOnline + ",";
                csv += mb;

                //                xlApp.DisplayAlerts = false;

//                xlWorkBook.SaveAs(directory + "\\" + filename, XlFileFormat.xlWorkbookNormal, misValue, misValue, misValue, misValue, XlSaveAsAccessMode.xlExclusive, misValue, misValue, misValue, misValue, misValue);
//                xlWorkBook.Close(true, misValue, misValue);
//                xlApp.Quit();
//                xlWorkSheet = null;
//                xlWorkBook = null;
                GC.Collect();
            }
            catch (Exception ee)
            {
//                MessageBox.Show(ee.Message);
                LogMsg(ee);
            }
            finally
            {
//                xlApp.Quit();
//                xlWorkSheet = null;
//                xlWorkBook = null;
                GC.Collect();
            }

            return csv;
        }

        public static string SaveLotData(int saleid)
        {
            var directory = new DirectoryInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                            @"Humboldt\AuctionController\transactionlogs"));
            string filename = SalePrefix + "_" + "clientsonline.xls";
            int totalBids = 0;
            int totalClients = 0;
            string csv = "";

            object misValue = System.Reflection.Missing.Value;

            try
            {
                int ii = 5;

                foreach (transcriptline tr in tl)
                {
                    if (tr.outcomeType != null)
                    {
                        csv += saleid + ",";
                        //                        xlWorkSheet.Cells[ii, 1] = tr.lot;
                        csv += tr.lot + ",";
//                        xlWorkSheet.Cells[ii, 2] = tr.registration;
                        csv += tr.registration + ",";
//                        xlWorkSheet.Cells[ii, 3] = tr.make + " " + tr.model + " " + tr.description;
                        csv += tr.make + "," + tr.model + "," + tr.description + " ,";

                        if ((tr.outcomeType.IndexOf("Sold") == 0) || (tr.outcomeType.IndexOf("Provisional") == 0))
                        {
                            if ((tr.bidderType == null) || (tr.bidderType.IndexOf("Hall") == 0))
                            {
//                                xlWorkSheet.Cells[ii, 4] = "Hall Bidder";
                                csv += ",,Hall,";
                            }
                            else
                            {
                                if ((tr.bidderType != null) && (tr.bidderType.IndexOf("Mobile") > -1))
                                {
//                                    xlWorkSheet.Cells[ii, 4] = tr.name + " (" + tr.company + ") (Mobile)";
                                    csv += tr.name + "," + tr.company + ",Mobile" + ",";
                                }
                                else
                                {
                                    csv += tr.name + "," + tr.company + "," + ",";
//                                    xlWorkSheet.Cells[ii, 4] = tr.name + " (" + tr.company + ")";
                                }
                            }
                            csv += tr.bidValue + "," ;
//                            xlWorkSheet.Cells[ii, 5] = "£" + tr.bidValue;
                            //xlWorkSheet.Cells[ii, 6] = tr.outcomeType;
                            csv += tr.outcomeType + ",";
                            if (tr.outcomeType.IndexOf("Sold") == 0)
                            {
//                                SoldTotal += Convert.ToDouble(tr.bidValue);
                            }
                            if (tr.outcomeType.IndexOf("Provisional") == 0)
                            {
//                                ProvisionalTotal += Convert.ToDouble(tr.bidValue);
                            }
//                            xlWorkSheet.Cells[ii, 7] = tr.date;
                            csv += tr.date + "\n";

                        }
                        else
                        {
//                            xlWorkSheet.Cells[ii, 4] = "";
//                            xlWorkSheet.Cells[ii, 5] = "";
//                            xlWorkSheet.Cells[ii, 6] = tr.outcomeType;
//                            xlWorkSheet.Cells[ii, 7] = tr.date;
                            csv += ",,,," + tr.outcomeType + "," + tr.date + "\n";
                        }
                        ii++;
                    }
                }
//                xlWorkSheet.Cells[ii, 1] = "Provisional";
//                xlWorkSheet.Cells[ii, 2] = "£" + ProvisionalTotal;
//                xlWorkSheet.Cells[ii + 1, 1] = "Sold";
//                xlWorkSheet.Cells[ii + 1, 2] = "£" + SoldTotal;

//                xlWorkSheet.get_Range("A5", "G" + (ii + 1)).Cells.Font.Size = 10;
//                xlWorkSheet.get_Range("A5", "G" + (ii - 1)).Cells.Borders.LineStyle = XlLineStyle.xlContinuous;
//                xlWorkSheet.get_Range("A5", "G" + (ii - 1)).Cells.Borders.Color = Color.Gray;
//                xlWorkSheet.get_Range("A1", "G" + (ii + 1)).Cells.HorizontalAlignment = XlHAlign.xlHAlignLeft;
//                xlWorkSheet.get_Range("E1", "E" + (ii + 1)).Cells.HorizontalAlignment = XlHAlign.xlHAlignRight;

//                xlApp.DisplayAlerts = false;

//                xlWorkBook.SaveAs(directory + "\\" + filename, XlFileFormat.xlWorkbookNormal, misValue, misValue, misValue, misValue, XlSaveAsAccessMode.xlExclusive, misValue, misValue, misValue, misValue, misValue);
//                xlWorkBook.Close(true, misValue, misValue);
//                xlApp.Quit();
//                xlWorkSheet = null;
//                xlWorkBook = null;
                GC.Collect();
            }
            catch (Exception ee)
            {
//                MessageBox.Show(ee.Message);
                LogMsg(ee);
            }
            finally
            {
//                xlApp.Quit();
//                xlWorkSheet = null;
//                xlWorkBook = null;
                GC.Collect();
            }
            return csv;
        }

        /*
           public static void SaveGlobalOnlineReport()
                {
                    var directory = new DirectoryInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                                    @"Humboldt\AuctionController\transactionlogs"));
                    string filename = SalePrefix + "_" + "globalsale.xls";

                    Microsoft.Office.Interop.Excel.Application xlApp = new Microsoft.Office.Interop.Excel.Application();

                    if (xlApp == null)
                    {
                        MessageBox.Show("Excel is not properly installed!!");
                        LogMsg("Excel is not properly installed!!");
                        return;
                    }

                    Workbook xlWorkBook;
                    Worksheet xlWorkSheet;
                    object misValue = System.Reflection.Missing.Value;

                    try
                    {
                        xlWorkBook = xlApp.Workbooks.Add(misValue);
                        xlWorkSheet = (Worksheet)xlWorkBook.Worksheets.get_Item(1);
                        xlWorkSheet.Name = "Global Sale Report";

                        xlWorkSheet.get_Range("A1", "A1").Cells.Font.Size = 20;
                        xlWorkSheet.get_Range("A2", "A2").Cells.Font.Size = 16;
                        xlWorkSheet.get_Range("A3", "A3").Cells.Font.Size = 12;

                        xlWorkSheet.get_Range("A1", "D18").Cells.Borders.Weight = 3d;
                        xlWorkSheet.get_Range("A1", "D18").Cells.WrapText = true;

                        xlWorkSheet.get_Range("A1", "A1").EntireColumn.ColumnWidth = 40;
                        xlWorkSheet.get_Range("B1", "B1").EntireColumn.ColumnWidth = 20;
                        xlWorkSheet.get_Range("C1", "C1").EntireColumn.ColumnWidth = 40;
                        xlWorkSheet.get_Range("D1", "D1").EntireColumn.ColumnWidth = 20;

                        xlWorkSheet.PageSetup.Application.ActiveWindow.DisplayGridlines = false;

                        xlWorkSheet.get_Range("A1", "A2").Cells.Font.Color = Color.ForestGreen;
                        xlWorkSheet.get_Range("A4", "A19").Interior.Color = Color.LightGray;
                        xlWorkSheet.get_Range("B4", "B19").Interior.Color = Color.DarkGray;
                        xlWorkSheet.get_Range("C4", "C19").Interior.Color = Color.LightGray;
                        xlWorkSheet.get_Range("D4", "D19").Interior.Color = Color.DarkGray;

                        xlWorkSheet.get_Range("C5", "D5").Interior.Color = Color.White;
                        xlWorkSheet.get_Range("C10", "D10").Interior.Color = Color.White;
                        xlWorkSheet.get_Range("C13", "D13").Interior.Color = Color.White;
                        xlWorkSheet.get_Range("C5", "D5").Cells.Font.Bold = true;
                        xlWorkSheet.get_Range("C10", "D10").Cells.Font.Bold = true;
                        xlWorkSheet.get_Range("C13", "D13").Cells.Font.Bold = true;

                        xlWorkSheet.Cells[1, 1] = "Live Bid";
                        xlWorkSheet.Cells[2, 1] = "Global Online Post Sale Report";
                        xlWorkSheet.Cells[3, 1] = "Sale Number " + ThisSale.SaleNo;
                        xlWorkSheet.Cells[4, 1] = "Branch";
                        xlWorkSheet.Cells[4, 2] = ThisSale.SiteName;
                        xlWorkSheet.Cells[4, 3] = "No of Vehicles Entered";
                        xlWorkSheet.Cells[4, 4] = ThisSale.Lots;

                        xlWorkSheet.Cells[5, 1] = "Sale Date";
                        xlWorkSheet.Cells[5, 2] = ThisSale.StartTime.ToLocalTime();
                        xlWorkSheet.Cells[5, 3] = "Sold";
                        xlWorkSheet.Cells[5, 4] = "";

                        xlWorkSheet.Cells[6, 1] = "Sale Description";
                        xlWorkSheet.Cells[6, 2] = ThisSale.Description;
                        xlWorkSheet.Cells[6, 3] = "No of Vehicles Sold Online (excluding Provisional)";
                        xlWorkSheet.Cells[6, 4] = ThisSale.onlineSales + ThisSale.mobileSales - ThisSale.ProvisionallySoldLotsonline;

                        xlWorkSheet.Cells[7, 1] = "Number of Dealers Logged On During Sale";
                        xlWorkSheet.Cells[7, 2] = ThisSale.ClientsOnline;
                        xlWorkSheet.Cells[7, 3] = "No of Vehicles Sold Physically";
                        xlWorkSheet.Cells[7, 4] = ThisSale.HallSales;

                        xlWorkSheet.Cells[8, 1] = "Number of Dealers who purchased (including Provisional)";
                        xlWorkSheet.Cells[8, 2] = ThisSale.Buyers;
                        xlWorkSheet.Cells[8, 3] = "% Sold Online";
                        if (ThisSale.Lots > 0)
                        {
                            xlWorkSheet.Cells[8, 4] = ((((ThisSale.onlineSales + ThisSale.mobileSales) * 100 / ThisSale.Lots)).ToString() + "%");
                        }
                        else
                        {
                            xlWorkSheet.Cells[8, 4] = "0%";
                        }

                        xlWorkSheet.Cells[9, 1] = "Min Added Value";
                        // Total bid online after last hall bid
                        xlWorkSheet.Cells[9, 2] = "£" + ThisSale.MinAddedValue;
                        xlWorkSheet.Cells[9, 3] = "% Sold Physically";
                        if (ThisSale.Lots > 0)
                        {
                            xlWorkSheet.Cells[9, 4] = ((ThisSale.HallSales * 100 / ThisSale.Lots)) + "%";
                        }
                        else
                        {
                            xlWorkSheet.Cells[9, 4] = "0%";
                        }

                        xlWorkSheet.Cells[10, 1] = "Total Number of Online Bids / Mobile Bids";
                        xlWorkSheet.Cells[10, 2] = ThisSale.onlineBids + " / " + ThisSale.mobileBids;
                        xlWorkSheet.Cells[10, 3] = "Provisionally Sold";
                        xlWorkSheet.Cells[10, 4] = "";

                        xlWorkSheet.Cells[11, 1] = "Total Bid Value LOL";
                        xlWorkSheet.Cells[11, 2] = ThisSale.TotalClosingPrices;
                        xlWorkSheet.Cells[11, 3] = "No of Vehicles Provisionally Sold Online";
                        xlWorkSheet.Cells[11, 4] = ThisSale.ProvisionallySoldLotsonline;

                        xlWorkSheet.Cells[12, 1] = "Min Added Value Per Unit Entered";
                        if (ThisSale.Lots > 0)
                        {
                            xlWorkSheet.Cells[12, 2] = "£" + (ThisSale.MinAddedValue / ThisSale.Lots).ToString("0.00");
                        }
                        else
                        {
                            xlWorkSheet.Cells[12, 2] = "£0.00";
                        }
                        xlWorkSheet.Cells[12, 3] = "No of Vehicles Provisionally Sold Physically";
                        xlWorkSheet.Cells[12, 4] = ThisSale.ProvisionallySoldLotsHall;

                        xlWorkSheet.Cells[13, 1] = "Min Added Value Per Unit with LOL Interest";
                        xlWorkSheet.Cells[13, 2] = "";
                        xlWorkSheet.Cells[13, 3] = "Not Sold";
                        xlWorkSheet.Cells[13, 4] = "";

                        xlWorkSheet.Cells[14, 1] = "LOL Bid Value per Unit Entered";
                        xlWorkSheet.Cells[14, 2] = "";
                        xlWorkSheet.Cells[14, 3] = "No of Vehicles Not Sold";
                        xlWorkSheet.Cells[14, 4] = (ThisSale.Lots - ThisSale.HallSales - ThisSale.onlineSales - ThisSale.mobileSales);

                        xlWorkSheet.Cells[15, 1] = "Total Vehicles Sold";
                        xlWorkSheet.Cells[15, 2] = (ThisSale.HallSales + ThisSale.onlineSales + ThisSale.mobileSales);
                        xlWorkSheet.Cells[15, 3] = "% Not Sold";
                        if (ThisSale.Lots > 0)
                        {
                            xlWorkSheet.Cells[15, 4] = ((ThisSale.Lots - ThisSale.HallSales - ThisSale.onlineSales - ThisSale.mobileSales) * 100 / ThisSale.Lots) + "%";
                        }
                        else
                        {
                            xlWorkSheet.Cells[15, 4] = "0%";
                        }

                        xlWorkSheet.Cells[16, 1] = "% of Units with Online Bid";
                        if (ThisSale.Lots > 0)
                        {
                            xlWorkSheet.Cells[16, 2] = ((ThisSale.LotsWithOnlineBids * 100 / ThisSale.Lots)) + "%";
                        }
                        else
                        {
                            xlWorkSheet.Cells[16, 2] = "£0.00";
                        }
                        xlWorkSheet.Cells[16, 3] = "";
                        xlWorkSheet.Cells[16, 4] = "";

                        xlWorkSheet.Cells[17, 1] = "% of Bids Placed Online";
                        if ((ThisSale.onlineBids + ThisSale.mobileBids + ThisSale.HallBids) > 0)
                        {
                            xlWorkSheet.Cells[17, 2] = ((ThisSale.onlineBids + ThisSale.mobileBids) * 100 / (ThisSale.onlineBids + ThisSale.mobileBids + ThisSale.HallBids)) + "%";
                        }
                        else
                        {
                            xlWorkSheet.Cells[17, 2] = "0%";
                        }
                        xlWorkSheet.Cells[17, 3] = "";
                        xlWorkSheet.Cells[17, 4] = "";

                        xlWorkSheet.Cells[18, 1] = "% of Online Sale Conversion";
                        if (((ThisSale.onlineSales + ThisSale.mobileSales) > 0) && (ThisSale.LotsWithOnlineBids > 0))
                        {
                            xlWorkSheet.Cells[18, 2] = ((ThisSale.onlineSales + ThisSale.mobileSales) * 100 / (ThisSale.LotsWithOnlineBids)) + "%";
                        }
                        else
                        {
                            xlWorkSheet.Cells[18, 2] = "0%";
                        }
                        xlWorkSheet.Cells[18, 3] = "";
                        xlWorkSheet.Cells[18, 4] = "";

                        xlWorkSheet.Cells[19, 1] = "% of Bids Placed by Mobile";
                        if ((ThisSale.onlineBids + ThisSale.mobileBids + ThisSale.HallBids) > 0)
                        {
                            xlWorkSheet.Cells[19, 2] = (ThisSale.mobileBids * 100 / (ThisSale.onlineBids + ThisSale.mobileBids + ThisSale.HallBids)) + "%";
                        }
                        else
                        {
                            xlWorkSheet.Cells[19, 2] = "0%";
                        }
                        xlWorkSheet.Cells[19, 3] = "";
                        xlWorkSheet.Cells[19, 4] = "";

                        xlWorkSheet.get_Range("A4", "D" + 19).Cells.Font.Size = 10;
                        xlWorkSheet.get_Range("A4", "D" + 19).Cells.Borders.LineStyle = XlLineStyle.xlContinuous;
                        xlWorkSheet.get_Range("A1", "D" + 19).Cells.Borders.Color = Color.White;
                        xlWorkSheet.get_Range("A1", "D" + 19).Cells.HorizontalAlignment = XlHAlign.xlHAlignLeft;

                        xlApp.DisplayAlerts = false;

                        xlWorkBook.SaveAs(directory + "\\" + filename, XlFileFormat.xlWorkbookNormal, misValue, misValue, misValue, misValue, XlSaveAsAccessMode.xlExclusive, misValue, misValue, misValue, misValue, misValue);
                        xlWorkBook.Close(true, misValue, misValue);
                        xlApp.Quit();
                        xlWorkSheet = null;
                        xlWorkBook = null;
                        GC.Collect();
                    }
                    catch (Exception ee)
                    {
                        MessageBox.Show(ee.Message);
                        LogMsg(ee);
                    }
                    finally
                    {
                        xlApp.Quit();
                        xlWorkSheet = null;
                        xlWorkBook = null;
                        GC.Collect();
                    }

                    return;
                }

                public static void SaveSaleReport(int saleid)
                {
                    var directory = new DirectoryInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                                    @"Humboldt\AuctionController\transactionlogs"));
                    string filename = SalePrefix + "_" + "salereport.xls";
                    double SoldTotal = 0.0;
                    double ProvisionalTotal = 0.0;

                    Microsoft.Office.Interop.Excel.Application xlApp = new Microsoft.Office.Interop.Excel.Application();

                    if (xlApp == null)
                    {
                        MessageBox.Show("Excel is not properly installed!!");
                        LogMsg("Excel is not properly installed!!");
                        return;
                    }

                    Workbook xlWorkBook;
                    Worksheet xlWorkSheet;
                    object misValue = System.Reflection.Missing.Value;

                    try
                    {
                        xlWorkBook = xlApp.Workbooks.Add(misValue);
                        xlWorkSheet = (Worksheet)xlWorkBook.Worksheets.get_Item(1);
                        xlWorkSheet.Name = "Full Sale Report";

                        xlWorkSheet.get_Range("A1", "A1").Cells.Font.Size = 20;
                        xlWorkSheet.get_Range("A2", "A2").Cells.Font.Size = 16;
                        xlWorkSheet.get_Range("A3", "A4").Cells.Font.Size = 12;

                        xlWorkSheet.get_Range("A1", "A1").EntireColumn.ColumnWidth = 15;
                        xlWorkSheet.get_Range("B1", "B1").EntireColumn.ColumnWidth = 20;
                        xlWorkSheet.get_Range("C1", "C1").EntireColumn.ColumnWidth = 50;
                        xlWorkSheet.get_Range("D1", "D1").EntireColumn.ColumnWidth = 50;
                        xlWorkSheet.get_Range("E1", "E1").EntireColumn.ColumnWidth = 20;
                        xlWorkSheet.get_Range("F1", "F1").EntireColumn.ColumnWidth = 20;
                        xlWorkSheet.get_Range("G1", "G1").EntireColumn.ColumnWidth = 30;

                        xlWorkSheet.get_Range("C1", "D1").EntireColumn.WrapText = true;

                        xlWorkSheet.PageSetup.Application.ActiveWindow.DisplayGridlines = false;

                        xlWorkSheet.get_Range("A1", "A2").Cells.Font.Color = Color.ForestGreen;
                        xlWorkSheet.get_Range("A4", "G4").Interior.Color = Color.ForestGreen;
                        xlWorkSheet.get_Range("A4", "G4").Cells.Font.Color = Color.White;

                        xlWorkSheet.Cells[1, 1] = "Live Bid";
                        xlWorkSheet.Cells[2, 1] = "Full Sale Report";
                        if (saleid > -1)
                        {
                            xlWorkSheet.Cells[3, 1] = "Sale Number " + saleid;
                        }
                        xlWorkSheet.Cells[4, 1] = "Lot";
                        xlWorkSheet.Cells[4, 2] = "Registration";
                        xlWorkSheet.Cells[4, 3] = "Description";
                        xlWorkSheet.Cells[4, 4] = "Name";
                        xlWorkSheet.Cells[4, 5] = "Amount";
                        xlWorkSheet.Cells[4, 6] = "Status";
                        xlWorkSheet.Cells[4, 7] = "Date/Time";

                        int ii = 5;

                        foreach (transcriptline tr in tl)
                        {
                            if (tr.outcomeType != null)
                            {
                                xlWorkSheet.Cells[ii, 1] = tr.lot;
                                xlWorkSheet.Cells[ii, 2] = tr.registration;
                                xlWorkSheet.Cells[ii, 3] = tr.make + " " + tr.model + " " + tr.description;
                                if ((tr.outcomeType.IndexOf("Sold") == 0) || (tr.outcomeType.IndexOf("Provisional") == 0))
                                {
                                    if ((tr.bidderType == null) || (tr.bidderType.IndexOf("Hall") == 0))
                                    {
                                        xlWorkSheet.Cells[ii, 4] = "Hall Bidder";
                                    }
                                    else
                                    {
                                        if ((tr.bidderType != null) && (tr.bidderType.IndexOf("Mobile") > -1))
                                        {
                                            xlWorkSheet.Cells[ii, 4] = tr.name + " (" + tr.company + ") (Mobile)";
                                        }
                                        else
                                        {
                                            xlWorkSheet.Cells[ii, 4] = tr.name + " (" + tr.company + ")";
                                        }
                                    }
                                    xlWorkSheet.Cells[ii, 5] = "£" + tr.bidValue;
                                    xlWorkSheet.Cells[ii, 6] = tr.outcomeType;
                                    if (tr.outcomeType.IndexOf("Sold") == 0)
                                    {
                                        SoldTotal += Convert.ToDouble(tr.bidValue);
                                    }
                                    if (tr.outcomeType.IndexOf("Provisional") == 0)
                                    {
                                        ProvisionalTotal += Convert.ToDouble(tr.bidValue);
                                    }
                                    xlWorkSheet.Cells[ii, 7] = tr.date;
                                }
                                else
                                {
                                    xlWorkSheet.Cells[ii, 4] = "";
                                    xlWorkSheet.Cells[ii, 5] = "";
                                    xlWorkSheet.Cells[ii, 6] = tr.outcomeType;
                                    xlWorkSheet.Cells[ii, 7] = tr.date;
                                }
                                ii++;
                            }
                        }
                        xlWorkSheet.Cells[ii, 1] = "Provisional";
                        xlWorkSheet.Cells[ii, 2] = "£" + ProvisionalTotal;
                        xlWorkSheet.Cells[ii + 1, 1] = "Sold";
                        xlWorkSheet.Cells[ii + 1, 2] = "£" + SoldTotal;

                        xlWorkSheet.get_Range("A5", "G" + (ii + 1)).Cells.Font.Size = 10;
                        xlWorkSheet.get_Range("A5", "G" + (ii - 1)).Cells.Borders.LineStyle = XlLineStyle.xlContinuous;
                        xlWorkSheet.get_Range("A5", "G" + (ii - 1)).Cells.Borders.Color = Color.Gray;
                        xlWorkSheet.get_Range("A1", "G" + (ii + 1)).Cells.HorizontalAlignment = XlHAlign.xlHAlignLeft;
                        xlWorkSheet.get_Range("E1", "E" + (ii + 1)).Cells.HorizontalAlignment = XlHAlign.xlHAlignRight;

                        xlApp.DisplayAlerts = false;

                        xlWorkBook.SaveAs(directory + "\\" + filename, XlFileFormat.xlWorkbookNormal, misValue, misValue, misValue, misValue, XlSaveAsAccessMode.xlExclusive, misValue, misValue, misValue, misValue, misValue);
                        xlWorkBook.Close(true, misValue, misValue);
                        xlApp.Quit();
                        xlWorkSheet = null;
                        xlWorkBook = null;
                        GC.Collect();
                    }
                    catch (Exception ee)
                    {
                        MessageBox.Show(ee.Message);
                        LogMsg(ee);
                    }
                    finally
                    {
                        xlApp.Quit();
                        xlWorkSheet = null;
                        xlWorkBook = null;
                        GC.Collect();
                    }

                    return;
                }

                public static void SaveVendorOnlineReport()
                {
                    var directory = new DirectoryInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                                    @"Humboldt\AuctionController\transactionlogs"));
                    string filename = SalePrefix + "_" + "sellerreport.xls";
                    int ss = 0;

                    Microsoft.Office.Interop.Excel.Application xlApp = new Microsoft.Office.Interop.Excel.Application();

                    if (xlApp == null)
                    {
                        MessageBox.Show("Excel is not properly installed!!");
                        LogMsg("Excel is not properly installed!!");
                        return;
                    }

                    Workbook xlWorkBook;
                    Worksheet[] xlWorkSheet = new Worksheet[numVendors];
                    Worksheet[] newxlWorkSheet = new Worksheet[numVendors];
                    object misValue = System.Reflection.Missing.Value;

                    try
                    {
                        xlWorkBook = xlApp.Workbooks.Add(misValue);

                        for (ss = 1; ss < numVendors; ss++)
                        {
                            newxlWorkSheet[ss] = xlApp.Worksheets.Add();
                        }

                        // One sheet per Vendor
                        ss = 0;
                        foreach (vendor thisvendor in Vendors)
                        {
                            xlWorkSheet[ss] = (Worksheet)xlWorkBook.Worksheets[ss + 1];
                            // Replace any dodgy characters in name
                            string pattern = "[\\~#%&*{}/:<>?|\"-]";
                            string replacement = " ";

                            Regex regEx = new Regex(pattern);
                            string accountnumber = thisvendor.vendorCode;
                            string sanitized = " ";
                            if (thisvendor.vendorName != null)
                            {
                                sanitized = accountnumber + " - " + Regex.Replace(regEx.Replace(thisvendor.vendorName, replacement), @"\s+", " ");
                            }
                            else
                            {
                                //sanitized = accountnumber;
                            }

                            if (sanitized.Length > 31)
                            {
                                xlWorkSheet[ss].Name = sanitized.Substring(0, 31);
                            }
                            else
                            {
                                try
                                {
                                    xlWorkSheet[ss].Name = sanitized;
                                }
                                catch (System.Runtime.InteropServices.COMException ce)
                                {
                                    LogMsg("Potentially matching Vendor names so obfusticating " + ce.Message);
                                    // Add ss to name and try again
                                    xlWorkSheet[ss].Name = sanitized + ss;
                                }
                            }

                            xlWorkSheet[ss].get_Range("A1", "A1").Cells.Font.Size = 20;
                            xlWorkSheet[ss].get_Range("A2", "A2").Cells.Font.Size = 16;
                            xlWorkSheet[ss].get_Range("A3", "A3").Cells.Font.Size = 12;

                            xlWorkSheet[ss].get_Range("A6", "D20").Cells.Borders.Weight = 3d;
                            xlWorkSheet[ss].get_Range("A1", "D20").Cells.WrapText = true;

                            xlWorkSheet[ss].get_Range("A1", "A1").EntireColumn.ColumnWidth = 40;
                            xlWorkSheet[ss].get_Range("B1", "B1").EntireColumn.ColumnWidth = 20;
                            xlWorkSheet[ss].get_Range("C1", "C1").EntireColumn.ColumnWidth = 40;
                            xlWorkSheet[ss].get_Range("D1", "D1").EntireColumn.ColumnWidth = 20;

                            xlWorkSheet[ss].PageSetup.Application.ActiveWindow.DisplayGridlines = false;

                            xlWorkSheet[ss].get_Range("A1", "A2").Cells.Font.Color = Color.ForestGreen;
                            xlWorkSheet[ss].get_Range("A4", "D5").Interior.Color = Color.ForestGreen;
                            xlWorkSheet[ss].get_Range("A6", "A20").Interior.Color = Color.LightGray;
                            xlWorkSheet[ss].get_Range("B6", "B20").Interior.Color = Color.DarkGray;
                            xlWorkSheet[ss].get_Range("C6", "C20").Interior.Color = Color.LightGray;
                            xlWorkSheet[ss].get_Range("D6", "D20").Interior.Color = Color.DarkGray;

                            xlWorkSheet[ss].get_Range("C7", "D7").Interior.Color = Color.White;
                            xlWorkSheet[ss].get_Range("C12", "D12").Interior.Color = Color.White;
                            xlWorkSheet[ss].get_Range("C15", "D15").Interior.Color = Color.White;
                            xlWorkSheet[ss].get_Range("C7", "D7").Cells.Font.Bold = true;
                            xlWorkSheet[ss].get_Range("C12", "D12").Cells.Font.Bold = true;
                            xlWorkSheet[ss].get_Range("C15", "D15").Cells.Font.Bold = true;

                            xlWorkSheet[ss].Cells[1, 1] = "Live Bid";
                            xlWorkSheet[ss].Cells[2, 1] = "Global Online Post Sale Report";
                            xlWorkSheet[ss].Cells[3, 1] = "Sale Number " + ThisSale.SaleNo;

                            xlWorkSheet[ss].Cells[4, 1] = "Vendor " + thisvendor.vendorName;
                            xlWorkSheet[ss].Cells[5, 1] = "Vendor Code " + thisvendor.vendorCode;

                            xlWorkSheet[ss].Cells[6, 1] = "Branch";
                            xlWorkSheet[ss].Cells[6, 2] = ThisSale.SiteName;
                            xlWorkSheet[ss].Cells[6, 3] = "No of Vehicles Entered";
                            xlWorkSheet[ss].Cells[6, 4] = thisvendor.numLots;

                            xlWorkSheet[ss].Cells[7, 1] = "Sale Date";
                            xlWorkSheet[ss].Cells[7, 2] = ThisSale.StartTime.ToLocalTime();
                            xlWorkSheet[ss].Cells[7, 3] = "Sold";
                            xlWorkSheet[ss].Cells[7, 4] = "";

                            xlWorkSheet[ss].Cells[8, 1] = "Sale Description";
                            xlWorkSheet[ss].Cells[8, 2] = ThisSale.Description;
                            xlWorkSheet[ss].Cells[8, 3] = "No of Vehicles Sold Online (excluding Provisional)";
                            xlWorkSheet[ss].Cells[8, 4] = thisvendor.numOnlineSales - thisvendor.numProvisionalOnlineSales;

                            xlWorkSheet[ss].Cells[9, 1] = "Number of Dealers Logged On During Sale";
                            xlWorkSheet[ss].Cells[9, 2] = ThisSale.ClientsOnline;
                            xlWorkSheet[ss].Cells[9, 3] = "No of Vehicles Sold Physically";
                            xlWorkSheet[ss].Cells[9, 4] = thisvendor.numHallSales;

                            xlWorkSheet[ss].Cells[10, 1] = "Number of Dealers who purchased (including Provisional)";
                            //                    xlWorkSheet[ss].Cells[10, 2] = thisvendor.numBuyers;
                            xlWorkSheet[ss].Cells[10, 2] = ThisSale.Buyers;
                            xlWorkSheet[ss].Cells[10, 3] = "% Sold Online";
                            if (thisvendor.numLots > 0)
                            {
                                xlWorkSheet[ss].Cells[10, 4] = thisvendor.numOnlineSales * 100 / thisvendor.numLots + "%";
                            }
                            else
                            {
                                xlWorkSheet[ss].Cells[10, 4] = "0%";
                            }

                            xlWorkSheet[ss].Cells[11, 1] = "Min Added Value";
                            xlWorkSheet[ss].Cells[11, 2] = "£" + thisvendor.minAddedValue;
                            xlWorkSheet[ss].Cells[11, 3] = "% Sold Physically";
                            if (thisvendor.numLots > 0)
                            {
                                xlWorkSheet[ss].Cells[11, 4] = thisvendor.numHallSales * 100 / thisvendor.numLots + "%";
                            }
                            else
                            {
                                xlWorkSheet[ss].Cells[11, 4] = "0%";
                            }

                            xlWorkSheet[ss].Cells[12, 1] = "Total Number of Online Bids";
                            //                    xlWorkSheet[ss].Cells[12, 2] = thisvendor.numOnlineBidders;
                            xlWorkSheet[ss].Cells[12, 2] = ThisSale.Bidders;
                            xlWorkSheet[ss].Cells[12, 3] = "Provisionally Sold";
                            xlWorkSheet[ss].Cells[12, 4] = "";

                            xlWorkSheet[ss].Cells[13, 1] = "Total Bid Value LOL";
                            xlWorkSheet[ss].Cells[13, 2] = thisvendor.TotalClosingPrice;
                            xlWorkSheet[ss].Cells[13, 3] = "No of Vehicles Provisionally Sold Online";
                            xlWorkSheet[ss].Cells[13, 4] = thisvendor.numProvisionalOnlineSales;

                            xlWorkSheet[ss].Cells[14, 1] = "Min Added Value Per Unit Entered";
                            xlWorkSheet[ss].Cells[14, 2] = "£" + (thisvendor.minAddedValue / thisvendor.numLots).ToString("0.00");
                            xlWorkSheet[ss].Cells[14, 3] = "No of Vehicles Provisionally Sold Physically";
                            xlWorkSheet[ss].Cells[14, 4] = thisvendor.numProvisionalHallSales;

                            xlWorkSheet[ss].Cells[15, 1] = "Min Added Value Per Unit with LOL Interest";
                            xlWorkSheet[ss].Cells[15, 2] = "";
                            xlWorkSheet[ss].Cells[15, 3] = "Not Sold";
                            xlWorkSheet[ss].Cells[15, 4] = "";

                            xlWorkSheet[ss].Cells[16, 1] = "LOL Bid Value per Unit Entered";
                            xlWorkSheet[ss].Cells[16, 2] = "";
                            xlWorkSheet[ss].Cells[16, 3] = "No of Vehicles Not Sold";
                            xlWorkSheet[ss].Cells[16, 4] = thisvendor.numLots - thisvendor.numHallSales - thisvendor.numOnlineSales;

                            xlWorkSheet[ss].Cells[17, 1] = "Total Vehicles Sold";
                            xlWorkSheet[ss].Cells[17, 2] = thisvendor.numHallSales + thisvendor.numOnlineSales;
                            xlWorkSheet[ss].Cells[17, 3] = "% Not Sold";
                            if (thisvendor.numLots > 0)
                            {
                                xlWorkSheet[ss].Cells[17, 4] = (thisvendor.numLots - thisvendor.numHallSales - thisvendor.numOnlineSales) * 100 / thisvendor.numLots + "%";
                            }
                            else
                            {
                                xlWorkSheet[ss].Cells[17, 4] = "0%";
                            }

                            xlWorkSheet[ss].Cells[18, 1] = "% of Units with Online Bid";
                            if (thisvendor.numLots > 0)
                            {
                                xlWorkSheet[ss].Cells[18, 2] = thisvendor.onlineBids * 100 / thisvendor.numLots + "%";
                            }
                            else
                            {
                                xlWorkSheet[ss].Cells[18, 2] = "0%";
                            }
                            xlWorkSheet[ss].Cells[18, 3] = "";
                            xlWorkSheet[ss].Cells[18, 4] = "";

                            xlWorkSheet[ss].Cells[19, 1] = "% of Bids Placed Online";
                            if ((thisvendor.onlineBids + thisvendor.hallBids) > 0)
                            {
                                xlWorkSheet[ss].Cells[19, 2] = thisvendor.onlineBids * 100 / (thisvendor.onlineBids + thisvendor.hallBids) + "%";
                            }
                            else
                            {
                                xlWorkSheet[ss].Cells[19, 2] = "0%";
                            }
                            xlWorkSheet[ss].Cells[19, 3] = "";
                            xlWorkSheet[ss].Cells[19, 4] = "";

                            xlWorkSheet[ss].Cells[20, 1] = "% of Online Sale Conversion";
                            if ((thisvendor.onlineBids) > 0)
                            {
                                xlWorkSheet[ss].Cells[20, 2] = thisvendor.numOnlineSales * 100 / (thisvendor.onlineBids) + "%";
                            }
                            else
                            {
                                xlWorkSheet[ss].Cells[20, 2] = "0%";
                            }
                            xlWorkSheet[ss].Cells[20, 3] = "";
                            xlWorkSheet[ss].Cells[20, 4] = "";

                            xlWorkSheet[ss].get_Range("A6", "D" + 20).Cells.Font.Size = 10;
                            xlWorkSheet[ss].get_Range("A6", "D" + 20).Cells.Borders.LineStyle = XlLineStyle.xlContinuous;
                            xlWorkSheet[ss].get_Range("A1", "D" + 20).Cells.Borders.Color = Color.White;
                            xlWorkSheet[ss].get_Range("A1", "D" + 20).Cells.HorizontalAlignment = XlHAlign.xlHAlignLeft;

                            ss++;
                        }

                        xlApp.DisplayAlerts = false;

                        xlWorkBook.SaveAs(directory + "\\" + filename, XlFileFormat.xlWorkbookNormal, misValue, misValue, misValue, misValue, XlSaveAsAccessMode.xlExclusive, misValue, misValue, misValue, misValue, misValue);
                        xlWorkBook.Close(true, misValue, misValue);
                        xlApp.Quit();
                        for (ss = 0; ss < numVendors; ss++)
                        {
                            newxlWorkSheet[ss] = null;
                            xlWorkSheet[ss] = null;
                        }
                        xlWorkBook = null;
                        GC.Collect();
                    }
                    catch (Exception ee)
                    {
                        MessageBox.Show(ee.Message);
                        LogMsg(ee);
                    }
                    finally
                    {
                        xlApp.Quit();
                        newxlWorkSheet = null;
                        xlWorkSheet = null;
                        xlWorkBook = null;
                        GC.Collect();
                    }

                    return;
                }

                private void SaveExcelLiveReports(int saleid)
                {
                    // Save Online Sales Report
                    SaveOnlineSalesReport(saleid);

                    // Save Clients Online Report
                    SaveClientsOnlineReport(saleid);

                    // Save Global Online Report
                    SaveGlobalOnlineReport();

                    // Save Vendor Online Report
                    SaveVendorOnlineReport();

                    // Save Sale Report
                    SaveSaleReport(saleid);

                    return;
                }
                */
    }
}
