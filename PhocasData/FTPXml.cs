using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using AlexPilotti.FTPS.Client;
using AlexPilotti.FTPS.Common;
using System.Net;
using System.Globalization;
using System.Net.Sockets;
using System.Xml;

namespace PhocasBidData
{

    public partial class FTPXML
    {

        private static readonly log4net.ILog log = log4net.LogManager.GetLogger
    (System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public struct TransactionLogs
        {
            public string SaleNo;
            public DateTime TimeRecorded;
            public string Filename;
            public string AMSSaleCode;
            public string AMSDescription;
            public string AMSSaleDate;
            public string AMSSite;
        }
        public static TransactionLogs[] tl;
        public static TransactionLogs[] alltl;
        public static int CurrentTransactionLog = -1;

        static public void LogMsg(string info)
        {
            String[] LogMsg = { info };

            log.Info(LogMsg);
            Console.WriteLine(info);

            return;
        }

        static public void LogMsg(Exception e)
        {
            String[] LogMsg = { e.Message };

            log.Fatal(LogMsg, e);
            Console.WriteLine(e.Message);

            return;
        }


        internal static long GetLatestSaleNo()
        {
            string Host = null;
            Int32 Port = 0;
            string Username = null;
            string Password = null;
            DateTime Latest = Convert.ToDateTime("1/1/1970");

            Host = "192.168.44.40";
            Username = "Humboldt";
            Password = "RyG5h9cb9B";
            Port = 990;

            long saleNo = 0;

            Console.WriteLine("Opening ftp connection");

            // Download the XML transcript and pop it into AMS...
            using (FTPSClient client = new FTPSClient())
            {
                try
                {
                    client.Connect(Host, Port,
                                   new NetworkCredential(Username,
                                                         Password),
                                   ESSLSupportMode.CredentialsRequired | ESSLSupportMode.DataChannelRequested | ESSLSupportMode.Implicit,
                                   new System.Net.Security.RemoteCertificateValidationCallback(AcceptAllCertifications),
                                   new System.Security.Cryptography.X509Certificates.X509Certificate(),
                                   0, 0, 0, 60000, true);
                    LogMsg("GetLatestSaleNo - FTP Connected ");
                    Console.WriteLine("FTP connected");
                }
                catch (FTPCommandException fe)
                {
                    LogMsg(fe);
                    return 0;
                }
                catch (IOException ie)
                {
                    LogMsg(ie);
                    return 0;
                }
                catch (SocketException ie)
                {
                    LogMsg(ie);
                    return 0;
                }

                try
                {
                    // Change to Transaction log directory
                    String pwd = client.GetCurrentDirectory();
                    client.SetCurrentDirectory("TransactionLogs");
                }
                catch (FTPCommandException fe)
                {
                    LogMsg(fe);
                    return 0;
                }

                try
                {
                    string directory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                @"Humboldt\AuctionController\transactionlogs\");
                    directory = "C:\\TransactionLogs\\";
                    // Make sure directory exists
                    if (Directory.Exists(directory))
                    {
                    }
                    else
                    {
                        Directory.CreateDirectory(directory);
                    }

                    {
                        Console.WriteLine("Find latest file");
                        // Find latest file
                        var lastFile = Directory.EnumerateFiles(directory).Max(filename => filename);
                        LogMsg("GetLatestSaleNo " + lastFile);
                        Console.WriteLine("GetLatestSaleNo " + lastFile);
                        if (lastFile != null)
                        {
                            saleNo = Convert.ToInt64(lastFile.Substring(19, 4));
                        }
                        LogMsg("GetLatestSaleNo " + saleNo);
                        Console.WriteLine("GetLatestSaleNo " + saleNo);
                    }

                    return (saleNo);
                }
                catch (FTPCommandException fe)
                {
                    LogMsg(fe);
                    return 0;
                }
                catch (IOException ie)
                {
                    LogMsg(ie);
                    return 0;
                }
                catch (IndexOutOfRangeException ie)
                {
                    LogMsg(ie);
                    return 0;
                }
            }
        }

        
        public static string GetXMLFile(int saleid)
        {
            string Host = null;
            Int32 Port = 0;
            string Username = null;
            string Password = null;
            string thisone = "";
            DateTime Latest = Convert.ToDateTime("1/1/1970");

            // Get FTP credentials from config
//            Auction.AuctionConfig config = Administration.ReadConfigFile();

//            LogMsg("GetXMLFile");

            try
            {
                // Ftp Server settings
/*                                Host = config.FtpServer;
                                Username = config.FtpUser;
                                Password = Auction.DecodeFrom64(config.FtpPassword);
                                Port = Int32.Parse(config.FtpPort);*/
//                Host = "54.154.210.120";
                Host = "192.168.44.40";
                Username = "Humboldt";
                Password = "RyG5h9cb9B";
                Port = 990;
            }
            catch (ArgumentNullException ne)
            {
                LogMsg(ne);
                return "";
            }
            catch (FormatException fe)
            {
                LogMsg(fe);
                return "";
            }

            string targetDirectory = "C:\\TransactionLogs";

            // Check if we've already got the file locally
            /*            string[] localItems = Directory.GetFiles(targetDirectory);
                        tl = new TransactionLogs[localItems.Length];
                        alltl = new TransactionLogs[localItems.Length];
                        int ii = 0;

                        foreach (string transactionFile in localItems)
                        {
                            if ((transactionFile.IndexOf("_") > -1) && (transactionFile.IndexOf("Transaction") > -1))
                            {
                                try
                                {
                                    if ((transactionFile.IndexOf("_") > -1) && (transactionFile.Length > 41))
                                    {
                                        // Format Date
                                        string pattern = "yyyy-MM-dd_HH-mm-ss_";
                                        DateTime parsedDate;

                                        if (DateTime.TryParseExact(transactionFile.Substring(transactionFile.IndexOf("_") + 1, 20), pattern, null,
                                                                    DateTimeStyles.None, out parsedDate))
                                        {
                                            // Check if this is today and if so skip and ftp
                                            if (parsedDate == DateTime.Today) {
                                                break;
                                            }
                                            alltl[ii].TimeRecorded = parsedDate;
                                        }

                                        alltl[ii].SaleNo = transactionFile.Substring(0, transactionFile.IndexOf("_"));
                                        alltl[ii].Filename = transactionFile;

                                        ii++;
                                    }
                                }
                                catch (IOException ie)
                                {
                                    // Skip it for now
                                    LogMsg(ie.Message);
                                }
                                catch (FormatException fe)
                                {
                                    // Skip it for now
                                    LogMsg(fe.Message);
                                }
                                catch (IndexOutOfRangeException fe)
                                {
                                    // Skip it for now
                                    LogMsg(fe.Message);
                                }
                                catch (Exception ee)
                                {
                                    // Skip it for now
                                    LogMsg(ee.Message);
                                }
                            }
                        }

                        ulong? largest = 0;
                        foreach (TransactionLogs transactionLog in alltl)
                        {
                            if (transactionLog.SaleNo != null)
                            {
                                if (transactionLog.SaleNo.IndexOf(saleid.ToString()) >= 18)
                                {
                                    ulong? size = (ulong?)new System.IO.FileInfo(transactionLog.Filename).Length;
                                    if (size > largest)
                                    {
                                        thisone = transactionLog.Filename;
                                        largest = size;
                                    }
                                }
                            }
                        }

                        if (thisone != "") return (thisone);
            */

            ulong? largest = 0;

            // Download the XML transcript and pop it into AMS...
            using (FTPSClient client = new FTPSClient())
            {
                try
                {
                    client.Connect(Host, Port,
                                   new NetworkCredential(Username,
                                                         Password),
                                   ESSLSupportMode.CredentialsRequired | ESSLSupportMode.DataChannelRequested | ESSLSupportMode.Implicit,
                                   new System.Net.Security.RemoteCertificateValidationCallback(AcceptAllCertifications),
                                   new System.Security.Cryptography.X509Certificates.X509Certificate(),
                                   0, 0, 0, 60000, true);
                    LogMsg("GetXMLFile - FTP Connected to sale " + saleid);
                }
                catch (FTPCommandException fe)
                {
                    LogMsg(fe);
                    return "";
                }
                catch (IOException ie)
                {
                    LogMsg(ie);
                    return "";
                }
                catch (SocketException ie)
                {
                    LogMsg(ie);
                    return "";
                }

                try
                {
                    // Change to Transaction log directory
                    String pwd = client.GetCurrentDirectory();
                    client.SetCurrentDirectory("TransactionLogs");
                }
                catch (FTPCommandException fe)
                {
                    LogMsg(fe);
                    return "";
                }

                try
                {
                    string directory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                                @"Humboldt\AuctionController\transactionlogs\");
                    directory = "C:\\TransactionLogs\\";
                    // Make sure directory exists
                    if (Directory.Exists(directory))
                    {
                    }
                    else
                    {
                        Directory.CreateDirectory(directory);
                    }

                    {
                        // Find latest file
                        List<DirectoryListItem> ftpItems = client.GetDirectoryList().ToList();
                        LogMsg("GetXMLFile - Current Sale");

                        largest = 0;
                        foreach (DirectoryListItem FtpItem in ftpItems)
                        {
                            if (FtpItem.Name.IndexOf(saleid.ToString()) == 0)
                            {
                                ulong? size = client.GetFileTransferSize(FtpItem.Name);
                                if (size > largest)
                                {
                                    thisone = FtpItem.Name;
                                    largest = size;
                                }
                            }
                        }
                    }

                    LogMsg("GetXMLFile - getting " + thisone + " into " + directory + thisone);

                    if (thisone.Length > 0)
                    {
                        client.GetFile(thisone, directory + thisone);
                    }
                    return (directory + thisone);
                }
                catch (FTPCommandException fe)
                {
                    LogMsg(fe);
                    return "";
                }
                catch (IOException ie)
                {
                    LogMsg(ie);
                    return "";
                }
                catch (IndexOutOfRangeException ie)
                {
                    LogMsg(ie);
                    return "";
                }
            }
        }


        public static TransactionLogs ExtractAMSXMLData(string tempFile, TransactionLogs tl)
        {
            TransactionLogs amstl = new TransactionLogs();
            amstl.TimeRecorded = tl.TimeRecorded;
            amstl.SaleNo = tl.SaleNo;
            amstl.Filename = tl.Filename;

            // Extract sale number and description
            XmlTextReader reader = new XmlTextReader(tempFile);
            try
            {
                string field1 = "";
                string field2 = "";
                string field3 = "";
                string field4 = "";
                bool Done = false;

                while ((reader.Read()) && (Done == false))
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
                                amstl.AMSSaleCode = reader["id"];
                                amstl.SaleNo = reader["id"];
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
                                field2 = "";
                            }
                            if (field3.IndexOf("date") > -1)
                            {
                                amstl.AMSSaleDate = Convert.ToDateTime(reader.Value).ToString("dd/MM/yyyy HH:mm");
                                amstl.AMSSaleDate = Convert.ToDateTime(reader.Value).ToString("MM/dd/yyyy HH:mm"); 
                                field3 = "";
                            }
                            if (field3.Length > 0)
                            {
                                if (field3.IndexOf("description") > -1)
                                {
                                    amstl.AMSDescription = reader.Value;
                                    field3 = "";
                                }
                                if (field3.IndexOf("code") > -1)
                                {
                                    amstl.AMSSaleCode = reader.Value;
                                    amstl.SaleNo = reader["id"];
                                    field3 = "";
                                }
                                if (field3.IndexOf("site") > -1)
                                {
                                    amstl.AMSSite = reader.Value;
                                    field3 = "";
                                }
                            }
                            break;

                        case XmlNodeType.EndElement: //Display the end of the element.
                            if (reader.Name.IndexOf("sale") > -1)
                            {
                                Done = true;
                            }
                            break;

                        default:
                            break;
                    }
                }
            }
            catch (IOException ie)
            {
                LogMsg(ie);
            }
            catch (XmlException xe)
            {
//                MessageBox.Show(xe.Message);
                LogMsg(xe);
            }

            reader.Close();
            return amstl;
        }

        public static TransactionLogs ExtractAMSData    (FTPSClient client, string directory, TransactionLogs tl)
        {
            TransactionLogs amstl = new TransactionLogs();
            amstl.TimeRecorded = tl.TimeRecorded;
            amstl.SaleNo = tl.SaleNo;
            amstl.Filename = tl.Filename;

            try
            {
                // Download file
                string tempFile = directory + "temp";
                client.GetFile(tl.Filename, tempFile);
                // Extract sale number and description
                XmlTextReader reader = new XmlTextReader(tempFile);

                amstl = ExtractAMSXMLData(tempFile, tl);

                // Delete temp file
                File.Delete(tempFile);

            }
            catch (FTPCommandException fce)
            {
                LogMsg("FTP Command problem " + fce.InnerException + " Error Code " + fce.ErrorCode + " File " + tl.Filename);
//                MessageBox.Show("FTP Command problem " + fce.InnerException);
            }
            catch (FTPException fe)
            {
                LogMsg("FTP problem " + fe.InnerException);
//                MessageBox.Show("FTP problem " + fe.InnerException);
            }


            catch (IOException ie)
            {
                LogMsg("IO problem " + ie.InnerException);
            }

            return amstl;
        }

        public static bool AcceptAllCertifications(object sender, System.Security.Cryptography.X509Certificates.X509Certificate certification, System.Security.Cryptography.X509Certificates.X509Chain chain, System.Net.Security.SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }


    }
}
