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
                string bidcsvFile = csvDataPath + "bidstats.csv";
                string lotcsvFile = csvDataPath + "lotstats.csv";
                string biddercsvFile = csvDataPath + "bidderstats.csv";
                string usercsvFile = csvDataPath + "userstats.csv";

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
                                lotHeaders.Append("Online Bids").Append("\n");
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
                                for (int saleNo = 2000; saleNo < 2700; saleNo++)
                                {
                                    try
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
                                            //lottext.Write("\n");
                                            biddertext.Write(bidderCSVData);
                                            //biddertext.Write("\n");
                                            usertext.Write(userCSVData);
                                            //usertext.Write("\n");
                                        }
//                                        File.Delete(thisFile);

                                    }
                                    catch (Exception ee)
                                    {
                                        LogMsg("Something went wrong" + ee.Message);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ee)
            {
                log.Warn("Couldn't get Mobile data " + ee);
            }
        }

                /**
         * Created on 06/07/2017.
         * Gets available for sale vehicles etc for Phocas
         * @author andy
         *
         */
        private static void GetAvailablesCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                sql += "SELECT DISTINCT on (date, site_id, vehicle_id, vehicle_entrydate)  ";
                sql += "d.date,    ";
                sql += "ca.id as vehicle_id,    ";
                sql += "ca.site_id as site_id,    ";
                sql += "ca.registration as registration,    ";
                sql += "(to_char(ca.entrydate, 'dd/mm/yyyy')) AS vehicle_entrydate,     ";
                sql += "(to_char(ca.exitdate, 'dd/mm/yyyy')) AS vehicle_exitdate,     ";
                sql += "(to_char(ca.withdrawnstamp, 'dd/mm/yyyy')) AS withdrawn_stamp,   ";
                sql += "(to_char(ca.onholdstamp, 'dd/mm/yyyy')) AS onhold_stamp,     ";
                sql += "(to_char(ca.clearonholdstamp, 'dd/mm/yyyy')) AS onholdcleared_stamp,     ";
                sql += "(to_char(ca.soldstamp, 'dd/mm/yyyy')) AS sold_stamp,     ";
                sql += "(ca.status),   ";
                sql += "(ca.onhold),   ";
                sql += "(ca.withdrawn),   ";
                sql += "(case when (ca.status is null and ca.entrydate < d.endofdays and (ca.exitdate > d.endofdays or ca.exitdate is null) and    ";
                sql += "(ca.withdrawn is false or (ca.withdrawn is true and ca.withdrawnstamp > d.endofdays)) and    ";
                sql += "(ca.onhold is false or (ca.onhold is true and ca.onholdstamp > d.endofdays))) or   ";
                sql += "(ca.soldstamp > d.endofdays) then 1 else 0 end) as isonsite,   ";
                sql += "(case when ca.status = 0 and (ca.soldstamp < d.endofdays or ca.soldstamp is null) then 1 else 0 end) as isentered,   ";
                sql += "(case when ca.status = 1 and ca.soldstamp < d.endofdays then 1 else 0 end) as issold,   ";
                sql += "(case when ca.status = 2 and ca.soldstamp < d.endofdays then 1 else 0 end) as isunsold,   ";
                sql += "(case when ca.status = 3 and ca.soldstamp < d.endofdays then 1 else 0 end) as isprovisional,  "; 
                sql += "(case when ca.withdrawn is true and (ca.withdrawnstamp < d.endofdays and ca.withdrawnstamp is null) then 1 else 0 end) as iswithdrawn,   ";
                sql += "(case when ca.onhold is true and ca.onholdstamp < d.endofdays and (ca.clearonholdstamp > d.endofdays or (ca.clearonholdstamp is null)) then 1 else 0 end) as isonhold,   ";
                sql += "(ca.text),   ";
                sql += "(case when ((ca.status is null and ca.entrydate < d.endofdays and (ca.exitdate > d.endofdays or ca.exitdate is null) and    ";
                sql += "(ca.withdrawn is false or (ca.withdrawn is true and ca.withdrawnstamp > d.endofdays)) and    ";
                sql += "(ca.onhold is false or (ca.onhold is true and ca.onholdstamp > d.endofdays)))) or   ";
                sql += "(ca.soldstamp > d.endofdays) or   ";
                sql += "(ca.status = 0 and (ca.soldstamp < d.endofdays or ca.soldstamp is null)) or   ";
                sql += "(ca.status = 2 and ca.soldstamp < d.endofdays) then 1 else 0 end) as availableforsale,    ";
                sql += "(case when (ca.status = 1 and ca.soldstamp < d.endofdays) or   ";
                sql += "(ca.status = 3 and ca.soldstamp < d.endofdays) or   ";
                sql += "(ca.withdrawn is true and (ca.withdrawnstamp < d.endofdays or ca.withdrawnstamp is null)) or   ";
                sql += "(ca.onhold is true and ca.onholdstamp < d.endofdays and (ca.clearonholdstamp > d.endofdays or ca.clearonholdstamp is null)) then 1 else 0 end) as awaitingdeparture    ";
                sql += "FROM (     ";
                sql += "select     ";
                sql += "to_char(date_trunc('day', ((current_date) - offs)), 'dd/mm/yyyy') AS date,    ";
                sql += "(current_date) - offs + 1 as endofdays  ";
                sql += "FROM generate_series(365, 0, -1)      ";
                sql += "AS offs     ";
                sql += ") d    ";
                sql += "LEFT OUTER JOIN (     ";
                sql += "SELECT     ";
                sql += "vehicle.id,    "; 
                sql += "site_id,    ";
                sql += "registration,   "; 
                sql += "entrydate,    ";
                sql += "exitdate,    ";
                sql += "saleresult.status as status,    ";
                sql += "saleresult.soldstamp as soldstamp,  "; 
                sql += "h.text as text,   ";
                sql += "case when h.text like '%Withdrawn%' then h.stamp else null end as withdrawnstamp,   ";
                sql += "case when h.text like '%Set on hold%' then h.stamp else null end as onholdstamp,   ";
                sql += "case when h.text like '%Clear on hold%' then h.stamp else null end as clearonholdstamp,   ";
                sql += "onhold,    ";
                sql += "withdrawn   ";
                sql += "FROM     ";
                sql += "vehicle vehicle LEFT OUTER JOIN saleresult saleresult ON saleresult.vehicle_id = vehicle.id and saleresult.sale_id = vehicle.lastresult_sale_id    ";
                sql += "LEFT OUTER JOIN history h ON vehicle.id = h.vehicle_id and (h.text like '%Withdrawn%' or h.text like '%Set on hold%' or h.text like '%Clear on hold%')   ";
                sql += ") ca    ";
                sql += "ON (entrydate is not null and entrydate < d.endofdays and (exitdate is null or exitdate > d.endofdays))    ";
                sql += "order by date, site_id, vehicle_id, vehicle_entrydate   ";



                LogMsg("Availables SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Availables Data");
                LogMsg("Extracted Availables Data");

                String fn = csvDataPath + "availables" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Availables Data");
                LogMsg("Written Availables Data");

            }

            catch (NpgsqlException ne)
            {
                Console.WriteLine("SQL Error {0}", ne.Message);
                LogMsg(ne);
            }

            catch (IOException ie)
            {
                Console.WriteLine("IOException Error {0}", ie.Message);
                LogMsg(ie);
            }
            catch (WebException we)
            {
                Console.WriteLine("Upload File Failed, status {0}", we.Message);
                LogMsg(we);
            }

            finally
            {
                conn.Close();
            }

        }

        /**
         * Created on 12/05/2017.
         * Gets vehicles on site for Phocas
         * @author andy
         *
         */
        private static void GetStockCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all vehicles on site

                sql += "select vehicle.id as id, registration, site_id, lastresult_sale_id,  ";
                sql += "case when capcoding.manufacturer is not null then capcoding.manufacturer else make end as make, ";
                sql += "case when capcoding.shortmodel is not null then capcoding.shortmodel else model end as model, ";
                sql += "to_char(vehicle.entrydate, 'dd/mm/yyyy') AS vehicle_entrydate,  ";
                sql += "((EXTRACT(epoch from age(NOW(), vehicle.entrydate)) / 86400)::int) AS daysonsite, ";
                sql += "case when saleresult.status is null then 1 else 0 end as awaitingcount,  ";
                sql += "case when saleresult.status = 0 then 1 else 0 end as enteredcount,  ";
                sql += "case when saleresult.status = 1 then 1 else 0 end as soldcount,  ";
                sql += "case when saleresult.status = 2 then 1 else 0 end as unsoldcount,  ";
                sql += "case when saleresult.status = 3 then 1 else 0 end as provisionalcount,  ";
                sql += "case when onhold is true then 1 else 0 end as onhold, ";
                sql += "case when withdrawn is true then 1 else 0 end as withdrawn, ";
                sql += "pricing_closingprice,  ";
                sql += "seller.accountnumber as selleraccountnumber,  ";
                sql += "buyer.accountnumber as buyeraccountnumber,  ";
                sql += "to_char(vehicle.exitdate, 'dd/mm/yyyy') AS vehicle_exitdate  ";
                sql += "FROM ";
                sql += "public.vehicle vehicle LEFT OUTER JOIN public.capcoding capcoding ON capcoding.vehiclecode = vehicle.capcode  ";
                sql += "LEFT OUTER JOIN public.saleresult saleresult ON saleresult.vehicle_id = vehicle.id and saleresult.sale_id = vehicle.lastresult_sale_id ";
                sql += "INNER JOIN public.client seller ON vehicle.seller_id = seller.id ";
                sql += "LEFT OUTER JOIN public.client buyer ON vehicle.buyer_id = buyer.id ";
                sql += "where exitdate is null and entrydate is not null order by entrydate  ";

                LogMsg("Stock SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Stock Data");
                LogMsg("Extracted Stock Data");


                String fn = csvDataPath + "stock" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Stock Data");
                LogMsg("Written Stock Data");

            }

            catch (NpgsqlException ne)
            {
                Console.WriteLine("SQL Error {0}", ne.Message);
                LogMsg(ne);
            }

            catch (IOException ie)
            {
                Console.WriteLine("IOException Error {0}", ie.Message);
                LogMsg(ie);
            }
            catch (WebException we)
            {
                Console.WriteLine("Upload File Failed, status {0}", we.Message);
                LogMsg(we);
            }

            finally
            {
                conn.Close();
            }

        }

        /**
         * Created on 10/11/2016.
         * Gets on site records for Phocas
         * @author andy
         *
         */
        private static void GetOnSiteRecordsCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Vehicle entry dates


                sql += "SELECT a.id, a.registration, a.site_id, 1 as onsite, to_char(d.as_of_date, 'dd/mm/yyyy') as stockdate ";
                sql += "FROM (  ";
                sql += "SELECT d::date AS as_of_date  ";
                sql += "FROM generate_series(date '2016-01-01', now(), interval '1 day') d  ";
                sql += ") d  ";
                sql += "JOIN vehicle a ON (d.as_of_date between a.entrydate and a.exitdate and a.entrydate is not null) or ";
                sql += "(d.as_of_date > a.entrydate and a.exitdate is null) ";
                sql += "JOIN LATERAL (  ";
                sql += "SELECT id  ";
                sql += "FROM vehicle  ";
                sql += "WHERE (d.as_of_date between a.entrydate and a.exitdate and a.entrydate is not null) or ";
                sql += "(d.as_of_date > a.entrydate and a.exitdate is null) ";
                sql += "ORDER BY as_of_date DESC  ";
                sql += "LIMIT 1  ";
                sql += ") b ON (d.as_of_date between a.entrydate and a.exitdate and a.entrydate is not null) or ";
                sql += "(d.as_of_date > a.entrydate and a.exitdate is null) ";
                sql += "ORDER BY a.entrydate, d.as_of_date  ";

                LogMsg("On Site Records SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted On Site Records Data");
                LogMsg("Extracted On Site Records Data");

                String fn = csvDataPath + "onsiterecords" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written On Site Records Data");
                LogMsg("Written On Site Records Data");
            }

            catch (NpgsqlException ne)
            {
                Console.WriteLine("SQL Error {0}", ne.Message);
                LogMsg(ne);
            }

            catch (IOException ie)
            {
                Console.WriteLine("IOException Error {0}", ie.Message);
                LogMsg(ie);
            }
            catch (WebException we)
            {
                Console.WriteLine("Upload File Failed, status {0}", we.Message);
                LogMsg(we);
            }

            finally
            {
                conn.Close();
            }

        }

        private static void GetGeoDataCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                sql += "select ";
                sql += "min(vehicle_id) as vehicle_id,  ";
                sql += "min(to_char(timestamp, 'dd/mm/yyyy')) as timestamp,  ";
                sql += "min(case when sale_id is null then 0 else sale_id end) as sale_id,   ";
                sql += "min(ipaddress),  ";
                sql += "min(username) as username,  ";
                sql += "count(vehicle_id) as vehicleviews, ";
                sql += "min((regexp_replace((case when position('country_code' IN geodata) > -1 then substring(geodata from (position('country_code' in geodata) + 15) for 2) else '' end), '[\"]', '', 'g'))) as countrycode,  ";
                sql += "min((regexp_replace((case when position('region_code' IN geodata) > -1 then substring(geodata from (position('region_code' in geodata) + 14) for 3) else '' end), '[\",]', '', 'g'))) as regioncode,  ";
                sql += "min((regexp_replace((case when position('zip_code' IN geodata) > -1 then substring(geodata from (position('zip_code' in geodata) + 11) for 6) else '' end), '[\",tim]', '', 'g'))) as postcode  ";
                sql += "from webanalytics   ";
                sql += "group by sale_id, ipaddress, vehicle_id ";
                sql += "order by sale_id ";

                LogMsg("Geodata SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Geo Data");
                LogMsg("Extracted Geo Data");

                String fn = csvDataPath + "geodata" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Geo Data");
                LogMsg("Written Geo Data");
            }

            catch (NpgsqlException ne)
            {
                Console.WriteLine("SQL Error {0}", ne.Message);
                LogMsg(ne);
            }

            catch (IOException ie)
            {
                Console.WriteLine("IOException Error {0}", ie.Message);
                LogMsg(ie);
            }
            catch (WebException we)
            {
                Console.WriteLine("Upload File Failed, status {0}", we.Message);
                LogMsg(we);
            }

            finally
            {
                conn.Close();
            }

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
