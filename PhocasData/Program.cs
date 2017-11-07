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

namespace PhocasData
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

            vehiclestats = " CASE  "; 
            vehiclestats += "WHEN mileage <= 1000 then 'Up to 1000' ";
            vehiclestats += "WHEN mileage > 1000 and mileage <= 5000 then 'Up to 5,000' ";
            vehiclestats += "WHEN mileage > 5000 and mileage <= 10000 then 'Up to 10,000' ";
            vehiclestats += "WHEN mileage > 10000 and mileage <= 20000 then 'Up to 20,000' ";
            vehiclestats += "WHEN mileage > 20000 and mileage <= 30000 then 'Up to 30,000' ";
            vehiclestats += "WHEN mileage > 30000 and mileage <= 40000 then 'Up to 40,000' ";
            vehiclestats += "WHEN mileage > 40000 and mileage <= 50000 then 'Up to 50,000' ";
            vehiclestats += "WHEN mileage > 50000 and mileage <= 60000 then 'Up to 60,000' ";
            vehiclestats += "WHEN mileage > 60000 and mileage <= 70000 then 'Up to 70,000' ";
            vehiclestats += "WHEN mileage > 70000 and mileage <= 80000 then 'Up to 80,000' ";
            vehiclestats += "WHEN mileage > 80000 and mileage <= 90000 then 'Up to 90,000' ";
            vehiclestats += "WHEN mileage > 90000 and mileage <= 100000 then 'Up to 100,000' ";
            vehiclestats += "ELSE 'Over 100,000' ";
            vehiclestats += "END as Mileage_Cat, ";
            vehiclestats += "CASE  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '1 Year' and NOW() then 'Up to 1 year'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '2 Year' and NOW() then 'Up to 2 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '3 Year' and NOW() then 'Up to 3 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '4 Year' and NOW() then 'Up to 4 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '5 Year' and NOW() then 'Up to 5 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '6 Year' and NOW() then 'Up to 6 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '7 Year' and NOW() then 'Up to 7 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '8 Year' and NOW() then 'Up to 8 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '9 Year' and NOW() then 'Up to 9 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '10 Year' and NOW() then 'Up to 10 years'  ";
            vehiclestats += "ELSE 'Over 10 year' ";
            vehiclestats += "End as Age_cat, ";
            vehiclestats += "CASE ";
            vehiclestats += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '30 Month' and saleresult.soldstamp then 'Late & Low' ";
            vehiclestats += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '54 Month' and saleresult.soldstamp then 'Fleet Profile'  ";
            vehiclestats += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '78 Month' and saleresult.soldstamp then 'PX Young'  ";
            vehiclestats += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '126 Month' and saleresult.soldstamp then 'PX Old'  ";
            vehiclestats += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '9999 Month' and saleresult.soldstamp then 'Budget' ";
            vehiclestats += "else 'Unsold' End as Industry_Age_cat, ";
            vehiclestats += "CASE ";
            vehiclestats += "WHEN lower(bodystyle) like '%cabriolet%' then 'Convertible' ";
            vehiclestats += "WHEN lower(bodystyle) like '%convertible%' then 'Convertible' ";
            vehiclestats += "WHEN lower(bodystyle) like '%soft-top%' then 'Convertible' ";
            vehiclestats += "WHEN lower(bodystyle) like '%coupe%' then 'Coupe' ";
            vehiclestats += "WHEN lower(bodystyle) like '%double cab pick-up%' or lower(bodystyle) like '%double cab dropside%' or " ;
            vehiclestats += "     lower(bodystyle) like '%double cab tipper%' or LOWER(bodystyle) LIKE '%double chassis cab%' then 'Double Cab Pick-up' ";
            vehiclestats += "WHEN lower(bodystyle) like '%estate%' or lower(bodystyle) like '%tourer%' then 'Estate' ";
            vehiclestats += "WHEN lower(bodystyle) like '%hardtop%' then 'Convertible' ";
            vehiclestats += "WHEN lower(bodystyle) like '%hatchback%' then 'Hatchback' ";
            vehiclestats += "WHEN lower(bodystyle) like '%high volume/high roof van%' then 'High Roof Van' ";
            vehiclestats += "WHEN lower(bodystyle) like '%extra high roof%' then 'High Roof Van' ";
            vehiclestats += "WHEN lower(bodystyle) like '%medium roof van%' then 'Medium Roof Van' ";
            vehiclestats += "WHEN lower(bodystyle) like '%roadster%' then 'Convertible' ";
            vehiclestats += "WHEN lower(bodystyle) like '%saloon%' then 'Saloon' ";
            vehiclestats += "WHEN lower(bodystyle) like '%station wagon%' then 'Station Wagon' ";
            vehiclestats += "WHEN lower(bodystyle) like '%standard roof minibus%' then 'Minibus' ";
            vehiclestats += "WHEN lower(bodystyle) like '%minibus%' then 'Minibus' ";
            vehiclestats += "WHEN lower(bodystyle) like '%combi van%' then 'Combi Van' ";
            vehiclestats += "WHEN lower(bodystyle) like '%crew bus%' then 'Crew Bus' ";
            vehiclestats += "WHEN lower(bodystyle) like '%chassis cab%' then 'Drop Side' ";
            vehiclestats += "WHEN lower(bodystyle) like '%dropside%' then 'Drop Side' ";
            vehiclestats += "WHEN lower(bodystyle) like '%pick-up%' then 'Pick-up' ";
            vehiclestats += "WHEN lower(bodystyle) like '%mpv%' then 'Estate' ";
            vehiclestats += "WHEN lower(bodystyle) like '%van%' then 'Van' ";
            vehiclestats += "ELSE 'Other' ";
            vehiclestats += "END AS BodyStyle, ";
            vehiclestats += "CASE ";
            vehiclestats += "WHEN enginesizecc is null then 'Unknown' ";
            vehiclestats += "WHEN enginesizecc between 0 and 999 then 'Less than 1.0L' ";
            vehiclestats += "WHEN enginesizecc between 1000 and 1399 then '1.0L - 1.3L' ";
            vehiclestats += "WHEN enginesizecc between 1400 and 1699 then '1.4L - 1.6L' ";
            vehiclestats += "WHEN enginesizecc between 1700 and 1999 then '1.7L - 1.9L' ";
            vehiclestats += "WHEN enginesizecc between 2000 and 2599 then '2.0L - 2.5L' ";
            vehiclestats += "WHEN enginesizecc between 2600 and 2999 then '2.6L - 2.9L' ";
            vehiclestats += "WHEN enginesizecc between 3000 and 3999 then '3.0L - 3.9L' ";
            vehiclestats += "WHEN enginesizecc between 4000 and 4999 then '4.0L - 4.9L' ";
            vehiclestats += "ELSE '5.0L' ";
            vehiclestats += "END AS Engine_Size_cat, ";
            // DVLA Colours
            vehiclestats += "CASE 		 ";
            vehiclestats += "when lower(vehicle.colour) like '%beige%' then 'Bronze'  ";
            vehiclestats += "when lower(vehicle.colour) like '%buff%' then 'Bronze'  ";
            vehiclestats += "when lower(vehicle.colour) like '%black%' then 'Black'  ";
            vehiclestats += "when lower(vehicle.colour) like '%blue%' then 'Blue'  ";
            vehiclestats += "when lower(vehicle.colour) like '%bronze%' then 'Bronze' ";
            vehiclestats += "when lower(vehicle.colour) like '%brown%' then 'Bronze'  ";
            vehiclestats += "when lower(vehicle.colour) like '%cream%' then 'Cream'  ";
            vehiclestats += "when lower(vehicle.colour) like '%gold%' then 'Gold'  ";
            vehiclestats += "when lower(vehicle.colour) like '%green%' then 'Green'  ";
            vehiclestats += "when lower(vehicle.colour) like '%grey%' then 'Grey'  ";
            vehiclestats += "when lower(vehicle.colour) like '%maroon%' then 'Maroon'  ";
            vehiclestats += "when lower(vehicle.colour) like '%purple%' then 'Purple'  ";
            vehiclestats += "when lower(vehicle.colour) like '%violet%' then 'Purple'  ";
            vehiclestats += "when lower(vehicle.colour) like '%mauve%' then 'Purple'  ";
            vehiclestats += "when lower(vehicle.colour) like '%orange%' then 'Orange'  ";
            vehiclestats += "when lower(vehicle.colour) like '%pink%' then 'Pink' ";
            vehiclestats += "when lower(vehicle.colour) like '%red%' then 'Red'  ";
            vehiclestats += "when lower(vehicle.colour) like '%silver%' then 'Silver'  ";
            vehiclestats += "when lower(vehicle.colour) like '%turquoise%' then 'Turquoise'  ";
            vehiclestats += "when lower(vehicle.colour) like '%white%' then 'White'  ";
            vehiclestats += "when lower(vehicle.colour) like '%yellow%' then 'Yellow'  ";
            // Extra Mercedes plus colours
            vehiclestats += "when lower(vehicle.colour) like '%ivory%' then 'Cream'  ";
            vehiclestats += "when lower(vehicle.colour) like '%fire%' then 'Red'  ";
            vehiclestats += "when lower(vehicle.colour) like '%anthracite%' then 'Silver'  ";
            vehiclestats += "when lower(vehicle.colour) like '%platinum%' then 'Silver'  ";
            vehiclestats += "when lower(vehicle.colour) like '%graphite%' then 'Grey'   ";
            vehiclestats += "when lower(vehicle.colour) like '%venetian%' then 'Red'   ";
            vehiclestats += "when lower(vehicle.colour) like '%ruby%' then 'Red'   ";
            vehiclestats += "when lower(vehicle.colour) like '%aluminium%' then 'Silver'  ";
            vehiclestats += "when lower(vehicle.colour) like '%multi%' then 'Multi-Coloured'   ";
            vehiclestats += "when vehicle.colour is null then 'Not Specified'  else 'Other'  ";
            vehiclestats += "END AS vehicle_standard_colour,  ";
            //            vehiclestats += "when lower(vehicle.colour) like '%magenta%' then 'Magenta'  ";

            vehiclestats += "CASE  ";
            vehiclestats += "WHEN sale.site_id = 1 THEN 'AB Chelmsford' ";
            vehiclestats += "WHEN sale.site_id = 659779 THEN 'AB Press Heath' ";
            vehiclestats += "WHEN sale.site_id = 659780 THEN 'AB Westbury' ";
            vehiclestats += "WHEN sale.site_id = 2360542 THEN 'AB Donington' ";
            vehiclestats += "ELSE 'AB Leeds' ";
            vehiclestats += "END AS Site_Location, ";

            // Generate Mobile Stats
//            GetMobileStatsCSV();

            // Generate Availables CSV
/*            GetAvailablesCSV();

            // Generate Arrivals CSV
            GetArrivalsCSV();

            // Generate Departures CSV
            GetDeparturesCSV();

            // Generate Stock CSV
            GetStockCSV();

            // Generate Site CSV
            GetSiteCSV();

            // Generate Sale CSV
            GetSaleCSV();

            // Generate Client CSV
            GetClientCSV();

            // Generate Client CSV
            GetClientGroupCSV();

            // Generate Client CSV
            GetGroupCSV();
*/
            // Generate Client CSV
            GetClientWithGroupCSV();

            // Generate Vehicle CSV
/*            GetVehicleCSV();

            // Generate SaleResult CSV
            //GetSaleResultCSV();

            // Generate Detailed SaleResult CSV
            GetDetailedSaleResultCSV();

            // Generate Transaction CSV
            //GetTransactionCSV();

            // Generate Extended Transaction CSV
            //GetExtendedTransactionCSV();
            GetExtendedTransactionSellerCSV();
            GetExtendedTransactionBuyerCSV();

            // Generate Supplier CSV
            GetSuppliersCSV();

            // Generate Transport Jobs CSV
            GetTransportJobCSV();

            // Generate Transport Records CSV
            GetTransportRecordsCSV();

            // Generate Transport Records CSV
            GetOnSiteRecordsCSV();
            
            // Generate Geo Data CSV
            GetGeoDataCSV();

            // Generate Naughty Vehicle CSV
            GetNaughtyVehiclesCSV();*/

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
         * Created on 02/11/2016.
         * Gets vehicle data for Phocas
         * @author andy
         *
         */
        private static void GetVehicleCSV()
        {
            try
            {
                conn.Open();
                string sql = null;
                string imgixurl = "https://abimg002.imgix.net/";

                // Find all vehicles
                sql = "SELECT DISTINCT on (vehicle.\"id\") ";
                sql += "vehicle.\"id\" AS vehicle_id,";
                sql += "vehicle.\"bodystyle\" AS vehicle_bodystyle,";
                sql += "CASE ";
                sql += "WHEN lower(vehicle.bodystyle) like '%cabriolet%' then 'Convertible' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%convertible%' then 'Convertible' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%soft-top%' then 'Convertible' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%coupe%' then 'Coupe' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%double cab pick-up%' or lower(vehicle.bodystyle) like '%double cab dropside%' or ";
                sql += "     lower(vehicle.bodystyle) like '%double cab tipper%' or LOWER(vehicle.bodystyle) LIKE '%double chassis cab%' then 'Double Cab Pick-up' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%estate%' or lower(vehicle.bodystyle) like '%tourer%' then 'Estate' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%hardtop%' then 'Convertible' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%hatchback%' then 'Hatchback' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%high volume/high roof van%' then 'High Roof Van' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%extra high roof%' then 'High Roof Van' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%medium roof van%' then 'Medium Roof Van' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%roadster%' then 'Convertible' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%saloon%' then 'Saloon' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%station wagon%' then 'Station Wagon' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%standard roof minibus%' then 'Minibus' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%minibus%' then 'Minibus' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%combi van%' then 'Combi Van' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%crew bus%' then 'Crew Bus' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%chassis cab%' then 'Drop Side' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%dropside%' then 'Drop Side' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%pick-up%' then 'Pick-up' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%mpv%' then 'Estate' ";
                sql += "WHEN lower(vehicle.bodystyle) like '%van%' then 'Van' ";
                sql += "ELSE 'Other' ";
                sql += "END AS vehicle_standard_bodystyle,";
/*                sql += "CASE ";
                sql += "WHEN bodystyle like '%Cabriolet%' or bodystyle like '%CABRIOLET%' then 'Cabriolet' ";
                sql += "WHEN bodystyle like '%Convertible%' or bodystyle like '%CONVERTIBLE%' then 'Convertible' ";
                sql += "WHEN bodystyle like '%Coupe%' or bodystyle like '%COUPE%' then 'Coupe' ";
                sql += "WHEN bodystyle = 'Double Cab Pick-up' or bodystyle = 'Double Cab Dropside' or bodystyle = 'Double Cab Tipper' or bodystyle = 'Double Chassis Cab' then 'Double Cab Pick-up' ";
                sql += "WHEN bodystyle like '%Estate%' or bodystyle like '%ESTATE%' then 'Estate' ";
                sql += "WHEN bodystyle like '%Hardtop%' then 'Cabriolet' ";
                sql += "WHEN bodystyle like '%Hatchback%' or bodystyle like '%HATCHBACK%' then 'Hatchback' ";
                sql += "WHEN bodystyle like '%High Volume/High Roof Van%' then 'High Volume/High Roof Van' ";
                sql += "WHEN bodystyle = 'Medium Roof Van' then 'Medium Roof Van' ";
                sql += "WHEN bodystyle like '%Roadster%' then 'Roadster' ";
                sql += "WHEN bodystyle like '%Saloon%' or bodystyle like '%SALOON%' then 'Saloon' ";
                sql += "WHEN bodystyle like '%Station Wagon%' then 'Station Wagon' ";
                sql += "WHEN bodystyle like '%Van%' then 'Van' ";
                sql += "ELSE 'Others' ";
                sql += "END AS vehicle_standard_bodystyle,";
 * */
                sql += "vehicle.\"colour\" AS vehicle_colour,";
                sql += "case when vehicle.\"doors\" is null then 0 else vehicle.\"doors\" end AS vehicle_doors,";
                sql += "CASE ";
                sql += "WHEN doors = 2 then '2 doors' ";
                sql += "WHEN doors = 3 then '3 doors' ";
                sql += "WHEN doors = 4 then '4 doors' ";
                sql += "WHEN doors = 5 then '5 doors' ";
                sql += "ELSE 'Other' ";
                sql += "END AS vehicle_doors_band, ";
                sql += "to_char(vehicle.\"entrydate\", 'dd/mm/yyyy') AS vehicle_entrydate,";
                sql += "to_char(vehicle.\"firstregistration\", 'dd/mm/yyyy') AS vehicle_firstregistration,";
                sql += "CASE ";
                sql += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '30 Month' and saleresult.soldstamp then 'Late & Low' ";
                sql += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '54 Month' and saleresult.soldstamp then 'Fleet Profile'  ";
                sql += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '78 Month' and saleresult.soldstamp then 'PX Young'  ";
                sql += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '126 Month' and saleresult.soldstamp then 'PX Old'  ";
                sql += "WHEN saleresult.status=1 and firstregistration between saleresult.soldstamp - INTERVAL '9999 Month' and saleresult.soldstamp then 'Budget' ";
                sql += "else 'Unsold' end AS vehicle_age,  "; 
                sql += "vehicle.\"fuel\" AS vehicle_fuel, ";
                sql += "CASE ";
                sql += "WHEN lower(vehicle.fuel) like '%hyb%' or lower(vehicle.fuel) like '%gas bi%' then 'Hybrid/Electric' ";
                sql += "WHEN lower(vehicle.fuel) = 'diesel' then 'Diesel' ";
                sql += "WHEN lower(vehicle.fuel) = '%electric%' then 'Hybrid/Electric' ";
                sql += "WHEN lower(vehicle.fuel) = 'petrol' then 'Petrol' ";
                sql += "WHEN lower(vehicle.fuel) = 'petrol/ele' or lower(vehicle.fuel) = 'petrol/gas' or lower(vehicle.fuel) = 'petrol/lpg' or lower(vehicle.fuel) like '%petrol/bio-ethanol%' then 'Hybrid/Electric'	 ";
                sql += "ELSE 'Hybrid/Electric' ";
                sql += "END AS FuelType, ";
                sql += "case when capcoding.\"manufacturer\" is not null then capcoding.\"manufacturer\" else vehicle.make end AS vehicle_make,";
//                sql += "vehicle.make AS vehicle_make,";
                sql += "case when vehicle.\"mileage\" is null then 1 else vehicle.\"mileage\" end AS vehicle_mileage,";
                sql += "CASE ";
                sql += "WHEN vehicle.mileage is null then 'Unknown'";
                sql += "WHEN vehicle.mileage <= 1000 then 'Up to 1,000'";
                sql += "WHEN vehicle.mileage > 1000 and vehicle.mileage <= 5000 then 'Up to 5,000'";
                sql += "WHEN vehicle.mileage > 5000 and vehicle.mileage <= 10000 then 'Up to 10,000'";
                sql += "WHEN vehicle.mileage > 10000 and vehicle.mileage <= 20000 then 'Up to 20,000'";
                sql += "WHEN vehicle.mileage > 20000 and vehicle.mileage <= 30000 then 'Up to 30,000'";
                sql += "WHEN vehicle.mileage > 30000 and vehicle.mileage <= 40000 then 'Up to 40,000'";
                sql += "WHEN vehicle.mileage > 40000 and vehicle.mileage <= 50000 then 'Up to 50,000'";
                sql += "WHEN vehicle.mileage > 50000 and vehicle.mileage <= 60000 then 'Up to 60,000'";
                sql += "WHEN vehicle.mileage > 60000 and vehicle.mileage <= 70000 then 'Up to 70,000'";
                sql += "WHEN vehicle.mileage > 70000 and vehicle.mileage <= 80000 then 'Up to 80,000'";
                sql += "WHEN vehicle.mileage > 80000 and vehicle.mileage <= 90000 then 'Up to 90,000'";
                sql += "WHEN vehicle.mileage > 90000 and vehicle.mileage <= 100000 then 'Up to 100,000'";
                sql += "ELSE 'Over 100,000'";
                sql += "END as Mileage_Band,";
                sql += "case when capcoding.\"longmodel\" is not null then capcoding.\"longmodel\" else vehicle.model end AS vehicle_model, ";
//                sql += "vehicle.model AS vehicle_model, ";
                sql += "to_char(vehicle.\"motexpiry\", 'dd/mm/yyyy') AS vehicle_motexpiry,";
                sql += "vehicle.\"previouskeepers\" AS vehicle_previouskeepers,";
                sql += "vehicle.\"previousregistration\" AS vehicle_previousregistration,";
                sql += "vehicle.\"registration\" AS vehicle_registration,";
                sql += "to_char(vehicle.\"taxexpiry\", 'dd/mm/yyyy') AS vehicle_taxexpiry,";
                sql += "vehicle.\"v5heldstate\" AS vehicle_v5heldstate,";
                sql += "vehicle.\"version\" AS vehicle_version,";
                sql += "vehicle.\"vin\" AS vehicle_vin,";
                sql += "vehicle.\"capcode\" AS vehicle_capcode,";
                sql += "vehicle.\"calculatedpricing_average\" AS vehicle_calculatedpricing_average,";
                sql += "vehicle.\"calculatedpricing_belowaverage\" AS vehicle_calculatedpricing_belowaverage,";
                sql += "vehicle.\"calculatedpricing_clean\" AS vehicle_calculatedpricing_clean,";
                sql += "vehicle.\"calculatedpricing_retail\" AS vehicle_calculatedpricing_retail,";
                sql += "vehicle.\"pricing_closingprice\" AS vehicle_pricing_closingprice,";
                sql += "vehicle.\"pricing_finalprice\" AS vehicle_pricing_finalprice,";
                sql += "vehicle.\"pricing_reserveprice\" AS vehicle_pricing_reserveprice,";
                sql += "vehicle.\"autoreserve\" AS vehicle_autoreserve,";
                sql += "vehicle.\"lastresult_sale_id\" AS vehicle_lastresult_sale_id,";
                sql += "vehicle.\"lastresult_vehicle_id\" AS vehicle_lastresult_vehicle_id,";
                sql += "vehicle.\"longderivative\" AS vehicle_longderivative,";
                sql += "vehicle.\"mileagewarranty\" AS vehicle_mileagewarranty,";
                sql += "vehicle.\"servicehistory\" AS vehicle_servicehistory,";
                sql += "vehicle.\"taxexpired\" AS vehicle_taxexpired,";
                sql += "vehicle.\"soldasseen\" AS vehicle_soldasseen,";
                sql += "case when vehicle.\"transmission\" is null then 'N/A' else vehicle.\"transmission\" end AS vehicle_transmission,";
                sql += "vehicle.\"vatstatus\" AS vehicle_vatstatus,";
                sql += "CASE ";
                sql += "WHEN vatstatus = 0 then 'Qualifying' ";
                sql += "WHEN vatstatus = 1 then 'Margin' ";
                sql += "WHEN vatstatus = 2 then 'Commerical subj. to VAT' ";
                sql += "WHEN vatstatus = 3 then 'Commercial no VAT' ";
                sql += "END AS Vat, ";
                sql += "vehicle.\"remarks\" AS vehicle_remarks,";
                sql += "vehicle.\"experiantotalloss\" AS vehicle_experiantotalloss,";
                sql += "vehicle.\"glasstradeprice\" AS vehicle_glasstradeprice,";
                sql += "vehicle.\"lastserviced\" AS vehicle_lastserviced,";
                sql += "vehicle.\"extraspec\" AS vehicle_extraspec,";
                sql += "case when vehicle.\"co2emission\" is null then 0 else vehicle.\"co2emission\" end AS vehicle_co2emission,";
                sql += "vehicle.\"yearofmanufacture\" AS vehicle_yearofmanufacture,";
                sql += "vehicle.\"damagecost\" AS vehicle_damagecost,";
                sql += "vehicle.\"buyitnow\" AS vehicle_buyitnow,";
                sql += "vehicle.\"excludefromlivebid\" AS vehicle_excludefromlivebid,";
                sql += "vehicle.\"excludefromwebsite\" AS vehicle_excludefromwebsite,";
                sql += "vehicle.\"websupression\" AS vehicle_websupression,";
                sql += "vehicle.\"deltapoint_retail\" AS vehicle_deltapoint_retail,";
                sql += "vehicle.\"deltapoint_trade\" AS vehicle_deltapoint_trade, ";
                sql += "vehicle.\"enginesizecc\" AS vehicle_enginesizecc, ";
                sql += "CASE ";
                sql += "WHEN enginesizecc is null then 'Unknown'";
                sql += "WHEN enginesizecc between 0 and 999 then 'Less than 1.0L' ";
                sql += "WHEN enginesizecc between 1000 and 1399 then '1.0L - 1.3L' ";
                sql += "WHEN enginesizecc between 1400 and 1699 then '1.4L - 1.6L' ";
                sql += "WHEN enginesizecc between 1700 and 1999 then '1.7L - 1.9L' ";
                sql += "WHEN enginesizecc between 2000 and 2599 then '2.0L - 2.5L' ";
                sql += "WHEN enginesizecc between 2600 and 2999 then '2.6L - 2.9L' ";
                sql += "WHEN enginesizecc between 3000 and 3999 then '3.0L - 3.9L' ";
                sql += "WHEN enginesizecc between 4000 and 4999 then '4.0L - 4.9L' ";
                sql += "ELSE 'Over 5.0L' ";
                sql += "END AS vehicle_enginesize_band, ";
                sql += "vehicle.\"plant\" AS vehicle_plant, ";
                sql += "to_char(vehicle.\"exitdate\", 'dd/mm/yyyy') AS vehicle_exitdate, ";
                sql += "vehicle.\"site_id\" AS vehicle_site_id, ";
                sql += "sales_per_vehicle.\"count\" AS sales_per_vehicle_count, ";
                sql += "case when inspection.\"grade\" is null or LENGTH(inspection.grade) < 1 then  'N/A' else inspection.\"grade\" end AS inspection_grade, ";
                sql += "case when inspection.\"result\" is null or LENGTH(inspection.result) < 1 then 'N/A' else inspection.\"result\" end AS inspection_result, ";
                sql += "case when inspection.\"totaldamage\" is null then 0 else inspection.\"totaldamage\" end AS inspection_totaldamage, ";
                sql += "case when inspection.\"nama\" is null or LENGTH(inspection.nama) < 1 then 'N/A' else inspection.\"nama\" end AS inspection_nama, ";
                sql += "case when vehicle.exitdate is not null then ((EXTRACT(epoch from age(vehicle.exitdate, vehicle.entrydate)) / 86400)::int)else ((EXTRACT(epoch from age(NOW(), vehicle.entrydate)) / 86400)::int) end AS daysonsite, ";
                sql += "case when vehicle.assured_id is null then 'Not Assured' else 'Assured' end AS vehicle_assured, ";
                sql += "case when vehicle.colour is null then 'Not Specified' ";
                sql += "when lower(vehicle.colour) like '%black%' then 'Black' ";
                sql += "when lower(vehicle.colour) like '%white%' then 'White' ";
                sql += "when lower(vehicle.colour) like '%silver%' then 'Silver' ";
                sql += "when lower(vehicle.colour) like '%red%' then 'Red' ";
                sql += "when lower(vehicle.colour) like '%blue%' then 'Blue' ";
                sql += "when lower(vehicle.colour) like '%green%' then 'Green' ";
                sql += "when lower(vehicle.colour) like '%yellow%' then 'Yellow' ";
                sql += "when lower(vehicle.colour) like '%gold%' then 'Gold' ";
                sql += "when lower(vehicle.colour) like '%bronze%' then 'Bronze' ";
                sql += "when lower(vehicle.colour) like '%purple%' then 'Purple' ";
                sql += "when lower(vehicle.colour) like '%magenta%' then 'Magenta' ";
                sql += "when lower(vehicle.colour) like '%grey%' then 'Grey' ";
                sql += "when lower(vehicle.colour) like '%gray%' then 'Grey' ";
                sql += "when lower(vehicle.colour) like '%brown%' then 'Brown' ";
                sql += "when lower(vehicle.colour) like '%beige%' then 'Beige' ";
                sql += "when lower(vehicle.colour) like '%fire%' then 'Red' ";
                sql += "when lower(vehicle.colour) like '%anthracite%' then 'Silver' ";
                sql += "when lower(vehicle.colour) like '%cream%' then 'Cream' ";
                sql += "when lower(vehicle.colour) like '%maroon%' then 'Maroon' ";
                sql += "when lower(vehicle.colour) like '%violet%' then 'Violet' ";
                sql += "when lower(vehicle.colour) like '%mauve%' then 'Mauve' ";
                sql += "when lower(vehicle.colour) like '%orange%' then 'Orange' ";
                sql += "when lower(vehicle.colour) like '%turquoise%' then 'Turquoise' ";
                sql += "when lower(vehicle.colour) like '%platinum%' then 'Silver' ";
                sql += "when lower(vehicle.colour) like '%graphite%' then 'Grey'  ";
                sql += "when lower(vehicle.colour) like '%venetian%' then 'Red'  ";
                sql += "when lower(vehicle.colour) like '%ruby%' then 'Red'  ";
                sql += "when lower(vehicle.colour) like '%multi%' then 'Multi-Coloured'  ";
                sql += "when lower(vehicle.colour) like '%pink%' then 'Pink' else 'Other'  ";
                sql += "end AS vehicle_standard_colour, ";
                sql += "case when inspection.grade is null or LENGTH(inspection.grade) < 1 or inspection.result is null or LENGTH(inspection.result) < 1 then 'N/A' ";
                sql += " else (concat(inspection.grade,left(inspection.result,1))) end AS combined_grade, ";
                sql += "case when inspection.costedreport_id is null then '' else '" + imgixurl + "' || image.externalpath end as costedpdfurl, ";
                sql += "case when vehicle.withdrawn is true then 1 else 0 end as withdrawn, ";
                sql += "case when (inspection.date is not null and vehicle.entrydate is not null) then ((EXTRACT(epoch from age(inspection.date, vehicle.entrydate)) / 60)::int) else 0 end AS time_to_inspect, ";
                sql += "inspection.inspector as inspection_inspector, ";
                sql += "inspection.provider as inspection_provider, ";
                sql += "case when transportrecord.inspectedoffsite is true then 1 else 0 end as inspectedoffsite, ";
                sql += "case when vehicle.onhold is true then 1 else 0 end as onholdcount,   ";
                sql += "case when vehicle.exitdate is null and vehicle.entrydate is not null and   ";
                sql += "((saleresult.status <> 1 and sales_per_vehicle.count > 5) or   ";
                sql += "(vehicle.onhold is true and ((NOW()::date - vehicle.entrydate::date) > 14)) or   ";
                sql += "(saleresult.status = 1 and ((NOW()::date - saleresult.soldstamp::date) > 14)) or   ";
                sql += "(vehicle.withdrawn is true) or   ";
                sql += "(vehicle.entrydate is not null and vehicle.lastresult_sale_id is null and ((NOW()::date - vehicle.entrydate::date) > 14)))  then 1 else 0 end as naughty,  ";
                sql += "inspection.inspectiontype as inspectiontype  ";
                sql += "FROM ";
                sql += "\"public\".\"vehicle\" vehicle INNER JOIN \"public\".\"sales_per_vehicle\" sales_per_vehicle ON vehicle.\"id\" = sales_per_vehicle.\"vehicle_id\" ";
                sql += "LEFT OUTER JOIN \"public\".\"inspection\" inspection ON vehicle.\"primaryinspection_id\" = inspection.\"id\"   ";
                sql += "LEFT OUTER JOIN public.image image ON inspection.costedreport_id = image.id   ";
                sql += "LEFT OUTER JOIN public.transportrecord transportrecord ON transportrecord.vehicle_id = vehicle.id   ";
                sql += "LEFT OUTER JOIN public.capcoding capcoding ON capcoding.vehiclecode = vehicle.capcode ";
                sql += "LEFT OUTER JOIN public.saleresult saleresult ON saleresult.sale_id = vehicle.lastresult_sale_id and status = 1";
                sql += " WHERE";
                sql += " vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                
                String order = " ORDER BY vehicle.\"id\"";
                String limit = " and vehicle.\"entrydate\" < '2016-06-01' ";
                String query = sql + limit + order;

                LogMsg("Vehicle SQL " + query);

                NpgsqlCommand command = new NpgsqlCommand(query, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Vehicle Data 1");
                LogMsg("Extracted Vehicle Data 1");

                String fn = csvDataPath + "vehicles" + ".csv";

                StreamWriter sr = WriteCSV(fn, dr);

                Console.WriteLine("Written Vehicle Data 1");
                LogMsg("Written Vehicle Data 1");

                limit = " and vehicle.\"entrydate\" >= '2016-06-01' ";
                query = sql + limit + order;

                LogMsg("Vehicle SQL 2 " + query);

                conn.Close();
                conn.Open();

                NpgsqlCommand command2 = new NpgsqlCommand(query, conn);
                NpgsqlDataReader dr2 = command2.ExecuteReader();

                Console.WriteLine("Extracted Vehicle Data 2");
                LogMsg("Extracted Vehicle Data 2");

                AppendCSV(fn, dr2);

                Console.WriteLine("Written Vehicle Data 2");
                LogMsg("Written Vehicle Data 2");

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
         * Created on 02/11/2016.
         * Gets site data for Phocas
         * @author andy
         *
         */
        private static void GetSiteCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all sites
                sql = "SELECT ";
                sql += "site.\"id\" AS site_id,";
                sql += "site.\"name\" AS site_name,";
                sql += "site.\"shortname\" AS site_shortname,";
                sql += "site.\"address_postcode\" AS site_address_postcode";
                sql += " FROM ";
                sql += "site";
                sql += " ORDER BY site.\"id\"";

                LogMsg("Site SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Site Data");
                LogMsg("Extracted Site Data");


                String fn = csvDataPath + "sites" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Site Data");
                LogMsg("Written Site Data");

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
         * Created on 02/11/2016.
         * Gets sale data for Phocas
         * @author andy
         *
         */
        private static void GetSaleCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Sales
                sql = "SELECT ";
                sql += "sale.id AS Sale_id, ";
                sql += "sale.description AS sale_description, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') AS sale_start, ";
                sql += "sale.site_id AS sale_site_id, ";
                sql += "sale.hall_hall AS sale_hall, ";
                sql += "DATE(start) as dstart, ";
                sql += "case when EXTRACT(HOUR from start) < 12 then 'AM' else (case when EXTRACT(HOUR from start) < 16 then 'PM' else 'EVENING' end) end as hstart, ";
                sql += "initcap(to_char(start, 'dy')) as day, ";
                sql += "count(distinct buyer_id) as uniquebuyers ";
                sql += "FROM  ";
                sql += "sale ";
                sql += "INNER JOIN saleresult saleresult ON sale.id = saleresult.sale_id   ";
                sql += "GROUP by sale.id ";
                sql += "ORDER BY sale.id ";

                LogMsg("Sale SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Sale Data");
                LogMsg("Extracted Sale Data");


                String fn = csvDataPath + "sales" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Sale Data");
                LogMsg("Written Sale Data");

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
         * Created on 02/11/2016.
         * Gets client data for Phocas
         * @author andy
         *
         */
        private static void GetClientCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Clients
                sql = "SELECT DISTINCT on (client_id)";
                sql += " client.\"id\" AS client_id,";
                sql += " client.\"accountnumber\" AS client_accountnumber,";
                sql += " client.\"name\" AS client_name,";
                sql += " client.\"primarycontact_streetone\" AS client_primarycontact_streetone,";
                sql += " client.\"primarycontact_streettwo\" AS client_primarycontact_streettwo,";
                sql += " client.\"primarycontact_town\" AS client_primarycontact_town,";
                sql += " client.\"primarycontact_county\" AS client_primarycontact_county,";
                sql += " client.\"primarycontact_postcode\" AS client_primarycontact_postcode,";
                sql += " client.\"primarycontact_mainphone\" AS client_primarycontact_mainphone,";
                sql += " client.\"primarycontact_fax\" AS client_primarycontact_fax,";
                sql += " client.\"creditlimit\" AS client_creditlimit, ";
                sql += " client.\"lastactivity\" AS client_lastactivity ";
                sql += "FROM ";
                sql += "  client ";
                sql += "ORDER BY client.\"id\"";

                LogMsg("Client SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted client Data");
                LogMsg("Extracted client Data");


                String fn = csvDataPath + "clients" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written client Data");
                LogMsg("Written client Data");

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
         * Created on 03/11/2016.
         * Gets client data with group info for Phocas
         * @author andy
         *
         */
        private static void GetClientWithGroupCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Clients
                sql += "SELECT ";
                sql += " max(client.id) AS client_id, ";
                sql += " max(client.accountnumber) AS client_accountnumber, ";
                sql += " max(client.name) AS client_name, ";
                sql += " max(client.primarycontact_streetone) AS client_primarycontact_streetone, ";
                sql += " max(client.primarycontact_streettwo) AS client_primarycontact_streettwo, ";
                sql += " max(client.primarycontact_town) AS client_primarycontact_town, ";
                sql += " max(client.primarycontact_county) AS client_primarycontact_county, ";
                sql += " max(client.primarycontact_postcode) AS client_primarycontact_postcode, ";
                sql += " max(client.primarycontact_mainphone) AS client_primarycontact_mainphone, ";
                sql += " max(client.primarycontact_fax) AS client_primarycontact_fax, ";
                sql += " max(client.creditlimit) AS client_creditlimit, ";
                sql += " max(client_grouptag.client_id) as clientgroup_client_id, ";
                sql += " max(client_grouptag.groups_id) as clientgroup_group_id, ";
                sql += " string_agg(grouptag.description, ',') as grouptag_description, ";
                sql += " max(client.seller_type) AS client_seller_type, ";
                sql += " max(client.buyer_type) AS client_buyer_type, ";
                // turn into a date
                sql += " max(client.lastactivity) AS client_lastactivity, ";
                sql += " max(client.accountmanager_id) AS client_accountmanager_id ";
                sql += "FROM ";
                sql += " client LEFT OUTER JOIN client_grouptag client_grouptag ON client.id = client_grouptag.client_id ";
                sql += " LEFT OUTER JOIN grouptag grouptag ON client_grouptag.groups_id = grouptag.id ";
                sql += "WHERE  ";
                sql += "lastactivity is not null and  ";
                sql += "accountnumber not like 'TR' and accountnumber not like 'PR' and  accountnumber not like 'NB' and ";
                sql += "accountnumber not like 'tr' and accountnumber not like 'pr' and accountnumber not like 'nb' ";
                sql += "GROUP by client.id ";
                sql += "ORDER BY client.id ";

                LogMsg("Client with Group SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted client Data");
                LogMsg("Extracted client Data");


                String fn = csvDataPath + "clientswithgroup" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written client with group Data");
                LogMsg("Written client with group Data");

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
         * Created on 02/11/2016.
         * Gets group data for Phocas
         * @author andy
         *
         */
        private static void GetGroupCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Groups
                sql = "SELECT ";
                sql += "grouptag.\"id\" AS grouptag_id,";
                sql += "grouptag.\"description\" AS grouptag_description";
                sql += " FROM ";
                sql += " grouptag ";
                sql += " ORDER BY grouptag.\"id\"";

                LogMsg("Group SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted group Data");
                LogMsg("Extracted group Data");


                String fn = csvDataPath + "groups" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written group Data");
                LogMsg("Written group Data");

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
         * Created on 02/11/2016.
         * Gets client / group data for Phocas
         * @author andy
         *
         */
        private static void GetClientGroupCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Groups
                sql = "SELECT ";
                sql += "client_grouptag.\"client_id\" AS client_grouptag_client_id,";
                sql += "client_grouptag.\"groups_id\" AS client_grouptag_groups_id";
                sql += " FROM ";
                sql += " client_grouptag ";
                sql += " ORDER BY client_grouptag.\"client_id\"";

                LogMsg("ClientGroup SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted client group Data");
                LogMsg("Extracted client group Data");


                String fn = csvDataPath + "clientgroups" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written client group Data");
                LogMsg("Written client group Data");

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
         * Created on 02/11/2016.
         * Gets transaction data for Phocas
         * @author andy
         *
         */
        private static void GetTransactionCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Transactions for vehicle
                sql = "select vehicle_id, seller_id as client_id, (commission * (100 + sellerinvoicevehicleentry.vatrate) / 100 )::numeric(20,2) as grosscost, commission as netcost, 'commission' as tag ";
                sql += "from sellerinvoicevehicleentry sive ";
                sql += "where commission > 0 and rescinded = false ";
                sql += "union all ";
                sql += "select vehicle_id, seller_id as client_id, (entryfee * (100 + sellerinvoicevehicleentry.vatrate) / 100 )::numeric(20,2) as grosscost, entryfee as netcost, 'entry fee' as tag ";
                sql += "from sellerinvoicevehicleentry sive ";
                sql += "where entryfee > 0  and rescinded = false ";
                sql += "union all ";
                sql += "select vehicle_id, seller_id as client_id, ben as grosscost, ben as netcost, 'ben' as tag ";
                sql += "from sellerinvoicevehicleentry sive ";
                sql += "where ben > 0  and rescinded = false ";
                sql += "union all ";
                sql += "select vehicle_id, seller_id as client_id, (collection * (100 + sellerinvoicevehicleentry.vatrate) / 100 )::numeric(20,2) as grosscost, collection as netcost, 'collection' as tag ";
                sql += "from sellerinvoicevehicleentry sive ";
                sql += "where collection > 0  and rescinded = false ";
                sql += "union all ";
                sql += "select vehicle_id, buyer_id as client_id, (indemnity * (100 + vatrate) / 100 )::numeric(20,2) as grosscost, indemnity as netcost, 'indemnity' as tag ";
                sql += "from buyerinvoicevehicleentry bive ";
                sql += "where indemnity > 0 and rescinded = false ";
                sql += "union all ";
                sql += "select vehicle_id, buyer_id as client_id, (delivery * (100 + vatrate) / 100 )::numeric(20,2) as grosscost, delivery as netcost, 'delivery' as tag ";
                sql += "from buyerinvoicevehicleentry bive ";
                sql += "where delivery > 0  and rescinded = false ";
                sql += "union all ";
                sql += "select vehicle_id, buyer_id as client_id, case when buyerinvoicevehicleentry_fees.fees_vatexempt = true then buyerinvoicevehicleentry_fees.fees_amount else (buyerinvoicevehicleentry_fees.fees_amount * ((100 + vatrate) / 100 ))::numeric(20,2) end as grosscost, buyerinvoicevehicleentry_fees.fees_amount as netcost, buyerinvoicevehicleentry_fees.fees_code as tag ";
                sql += "from public.buyerinvoicevehicleentry buyerinvoicevehicleentry INNER JOIN public.buyerinvoicevehicleentry_fees buyerinvoicevehicleentry_fees ON buyerinvoicevehicleentry_fees.buyerinvoicevehicleentry_id = buyerinvoicevehicleentry.id ";
                sql += "where rescinded = false ";
                sql += "union all ";
                sql += "select vehicle_id, seller_id as client_id, case when sellerinvoicevehicleentry_charges.charges_vatexempt = true then sellerinvoicevehicleentry_charges.charges_amount else (sellerinvoicevehicleentry_charges.charges_amount * ((100 + sellerinvoicevehicleentry.vatrate) / 100 ))::numeric(20,2) end as grosscost, sellerinvoicevehicleentry_charges.charges_amount as netcost, sellerinvoicevehicleentry_charges.charges_code as tag ";
                sql += "from public.sellerinvoicevehicleentry sellerinvoicevehicleentry INNER JOIN public.sellerinvoicevehicleentry_charges sellerinvoicevehicleentry_charges ON sellerinvoicevehicleentry_charges.sellerinvoicevehicleentry_id = sellerinvoicevehicleentry.id ";
                sql += "where rescinded = false ";
                sql += "order by vehicle_id, client_id";

                LogMsg("Transaction SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted transaction Data");
                LogMsg("Extracted transaction Data");


                String fn = csvDataPath + "transactions" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written transaction Data");
                LogMsg("Written transaction Data");

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
         * Created on 03/11/2016.
         * Gets extended transaction data for Phocas
         * @author andy
         *
         */
        private static void GetExtendedTransactionSellerCSV()
        {
            try
            {
                conn.Open();
                string sql = null;


                sql = "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "case when saleresult.lot is null then 0 else saleresult.lot end as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(commission * (100 + sellerinvoicevehicleentry.vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "commission as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += vehiclestats;
                sql += "CASE ";
                sql += "WHEN (seller.accountnumber = 'PR' or seller.accountnumber = 'pr') THEN 'Private Seller' ";
                sql += "WHEN (seller.accountnumber = 'TR' or seller.accountnumber = 'tr') THEN 'Trade Seller' ";
                sql += "ELSE 'Commercial Seller' ";
                sql += "END AS SellerGroup, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "'commission' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "commission > 0 and rescinded = false ";
                sql += " and vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "case when saleresult.lot is null then 0 else saleresult.lot end as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(entryfee * (100 + sellerinvoicevehicleentry.vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "entryfee as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += vehiclestats;
                sql += "CASE ";
                sql += "WHEN (seller.accountnumber = 'PR' or seller.accountnumber = 'pr') THEN 'Private Seller' ";
                sql += "WHEN (seller.accountnumber = 'TR' or seller.accountnumber = 'tr') THEN 'Trade Seller' ";
                sql += "ELSE 'Commercial Seller' ";
                sql += "END AS SellerGroup, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "'entry fee' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "entryfee > 0 and rescinded = false ";
                sql += " and vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "case when saleresult.lot is null then 0 else saleresult.lot end as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(collection * (100 + sellerinvoicevehicleentry.vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "collection as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += vehiclestats;
                sql += "CASE ";
                sql += "WHEN (seller.accountnumber = 'PR' or seller.accountnumber = 'pr') THEN 'Private Seller' ";
                sql += "WHEN (seller.accountnumber = 'TR' or seller.accountnumber = 'tr') THEN 'Trade Seller' ";
                sql += "ELSE 'Commercial Seller' ";
                sql += "END AS SellerGroup, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "'collection' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "collection > 0 and rescinded = false ";
                sql += " and vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "case when saleresult.lot is null then 0 else saleresult.lot end as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "ben as netcost, ";
                sql += "ben as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += vehiclestats;
                sql += "CASE ";
                sql += "WHEN (seller.accountnumber = 'PR' or seller.accountnumber = 'pr') THEN 'Private Seller' ";
                sql += "WHEN (seller.accountnumber = 'TR' or seller.accountnumber = 'tr') THEN 'Trade Seller' ";
                sql += "ELSE 'Commercial Seller' ";
                sql += "END AS SellerGroup, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "'entry fee' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "ben > 0 and rescinded = false ";
                sql += " and vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "case when saleresult.lot is null then 0 else saleresult.lot end as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "case when sellerinvoicevehicleentry_charges.charges_vatexempt = true then sellerinvoicevehicleentry_charges.charges_amount else (sellerinvoicevehicleentry_charges.charges_amount * ((100 + sellerinvoicevehicleentry.vatrate) / 100 ))::numeric(20,2) end as grosscost, ";
                sql += "sellerinvoicevehicleentry_charges.charges_amount as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += vehiclestats;
                sql += "CASE ";
                sql += "WHEN (seller.accountnumber = 'PR' or seller.accountnumber = 'pr') THEN 'Private Seller' ";
                sql += "WHEN (seller.accountnumber = 'TR' or seller.accountnumber = 'tr') THEN 'Trade Seller' ";
                sql += "ELSE 'Commercial Seller' ";
                sql += "END AS SellerGroup, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "sellerinvoicevehicleentry_charges.charges_code as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry_charges sellerinvoicevehicleentry_charges ON sellerinvoicevehicleentry_charges.sellerinvoicevehicleentry_id = sellerinvoicevehicleentry.id ";
                sql += "WHERE rescinded = false ";
                sql += " and vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += "ORDER by sale_id, saleresult_lot, client_id ";




                LogMsg("Extended Seller Transaction SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted extended seller transaction Data");
                LogMsg("Extracted extended seller transaction Data");


                String fn = csvDataPath + "extendedtransactionsseller" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written extended seller transaction Data");
                LogMsg("Written extended seller transaction Data");

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
         * Created on 03/11/2016.
         * Gets extended transaction data for Phocas
         * @author andy
         *
         */
        private static void GetExtendedTransactionBuyerCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "case when saleresult.lot is null then 0 else saleresult.lot end as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "buyerinvoicevehicleentry.vehiclegross as grosscost, ";
                sql += "buyerinvoicevehicleentry.vehiclenet as netcost, ";
                sql += "bive_extended.total_charges_gross as feesgross, ";
                sql += "bive_extended.total_charges_net as feesnet, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += vehiclestats;
                sql += "CASE ";
                sql += "WHEN (buyer.accountnumber = 'PR' or buyer.accountnumber = 'pr') THEN 'Private buyer' ";
                sql += "WHEN (buyer.accountnumber = 'TR' or buyer.accountnumber = 'tr') THEN 'Trade buyer' ";
                sql += "ELSE 'Commercial buyer' ";
                sql += "END AS buyerGroup, ";
                sql += "buyer.id as client_id, ";
                sql += "buyer.accountnumber as client_accountnumber, ";
                sql += "'buyer' as clienttype, ";
                sql += "'sale price' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.buyerinvoicevehicleentry buyerinvoicevehicleentry ON vehicle.id = buyerinvoicevehicleentry.vehicle_id ";
                sql += "INNER JOIN public.bive_extended bive_extended ON bive_extended.id = buyerinvoicevehicleentry.id ";
                sql += "WHERE   ";
                sql += "saleresult.status = 1 ";
                sql += " and vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += "UNION ALL   ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "case when saleresult.lot is null then 0 else saleresult.lot end as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(indemnity * (100 + vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "indemnity as netcost, ";
                sql += "bive_extended.total_charges_gross as feesgross, ";
                sql += "bive_extended.total_charges_net as feesnet, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += vehiclestats;
                sql += "CASE ";
                sql += "WHEN (buyer.accountnumber = 'PR' or buyer.accountnumber = 'pr') THEN 'Private buyer' ";
                sql += "WHEN (buyer.accountnumber = 'TR' or buyer.accountnumber = 'tr') THEN 'Trade buyer' ";
                sql += "ELSE 'Commercial buyer' ";
                sql += "END AS buyerGroup, ";
                sql += "buyer.id as client_id, ";
                sql += "buyer.accountnumber as client_accountnumber, ";
                sql += "'buyer' as clienttype, ";
                sql += "'indemnity' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.buyerinvoicevehicleentry buyerinvoicevehicleentry ON vehicle.id = buyerinvoicevehicleentry.vehicle_id ";
                sql += "INNER JOIN public.bive_extended bive_extended ON bive_extended.id = buyerinvoicevehicleentry.id ";
                sql += "WHERE ";
                sql += "indemnity > 0 and rescinded = false ";
                sql += " and vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "case when saleresult.lot is null then 0 else saleresult.lot end as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(delivery * (100 + vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "delivery as netcost, ";
                sql += "bive_extended.total_charges_gross as feesgross, ";
                sql += "bive_extended.total_charges_net as feesnet, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += vehiclestats;
                sql += "CASE ";
                sql += "WHEN (buyer.accountnumber = 'PR' or buyer.accountnumber = 'pr') THEN 'Private buyer' ";
                sql += "WHEN (buyer.accountnumber = 'TR' or buyer.accountnumber = 'tr') THEN 'Trade buyer' ";
                sql += "ELSE 'Commercial buyer' ";
                sql += "END AS buyerGroup, ";
                sql += "buyer.id as client_id, ";
                sql += "buyer.accountnumber as client_accountnumber, ";
                sql += "'buyer' as clienttype, ";
                sql += "'delivery' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.buyerinvoicevehicleentry buyerinvoicevehicleentry ON vehicle.id = buyerinvoicevehicleentry.vehicle_id ";
                sql += "INNER JOIN public.bive_extended bive_extended ON bive_extended.id = buyerinvoicevehicleentry.id ";
                sql += "WHERE ";
                sql += "delivery > 0 and rescinded = false ";
                sql += " and vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "case when saleresult.lot is null then 0 else saleresult.lot end as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "case when buyerinvoicevehicleentry_fees.fees_vatexempt = true then buyerinvoicevehicleentry_fees.fees_amount else (buyerinvoicevehicleentry_fees.fees_amount * ((100 + vatrate) / 100 ))::numeric(20,2) end as grosscost, ";
                sql += "buyerinvoicevehicleentry_fees.fees_amount as netcost, ";
                sql += "bive_extended.total_charges_gross as feesgross, ";
                sql += "bive_extended.total_charges_net as feesnet, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += vehiclestats;
                sql += "CASE ";
                sql += "WHEN (buyer.accountnumber = 'PR' or buyer.accountnumber = 'pr') THEN 'Private buyer' ";
                sql += "WHEN (buyer.accountnumber = 'TR' or buyer.accountnumber = 'tr') THEN 'Trade buyer' ";
                sql += "ELSE 'Commercial buyer' ";
                sql += "END AS buyerGroup, ";
                sql += "buyer.id as client_id, ";
                sql += "buyer.accountnumber as client_accountnumber, ";
                sql += "'buyer' as clienttype, ";
                sql += "buyerinvoicevehicleentry_fees.fees_code as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.buyerinvoicevehicleentry buyerinvoicevehicleentry ON vehicle.id = buyerinvoicevehicleentry.vehicle_id ";
                sql += "INNER JOIN public.buyerinvoicevehicleentry_fees buyerinvoicevehicleentry_fees ON buyerinvoicevehicleentry_fees.buyerinvoicevehicleentry_id = buyerinvoicevehicleentry.id ";
                sql += "INNER JOIN public.bive_extended bive_extended ON bive_extended.id = buyerinvoicevehicleentry.id ";
                sql += "WHERE rescinded = false ";
                sql += " and vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += "ORDER by sale_id, saleresult_lot, client_id ";

                LogMsg("Extended Transaction SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted extended buyer transaction Data");
                LogMsg("Extracted extended buyer transaction Data");


                String fn = csvDataPath + "extendedtransactionsbuyer" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written extended buyer transaction Data");
                LogMsg("Written extended buyer transaction Data");

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
         * Created on 03/11/2016.
         * Gets extended transaction data for Phocas
         * @author andy
         *
         */
        private static void GetExtendedTransactionCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "saleresult.lot as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(commission * (100 + sellerinvoicevehicleentry.vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "commission as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "'commission' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "commission > 0 and rescinded = false ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "saleresult.lot as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(entryfee * (100 + sellerinvoicevehicleentry.vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "entryfee as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "'entry fee' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "entryfee > 0 and rescinded = false ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "saleresult.lot as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(collection * (100 + sellerinvoicevehicleentry.vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "collection as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "'collection' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "collection > 0 and rescinded = false ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "saleresult.lot as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "ben as netcost, ";
                sql += "ben as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "'entry fee' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "ben > 0 and rescinded = false ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "saleresult.lot as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "case when sellerinvoicevehicleentry_charges.charges_vatexempt = true then sellerinvoicevehicleentry_charges.charges_amount else (sellerinvoicevehicleentry_charges.charges_amount * ((100 + sellerinvoicevehicleentry.vatrate) / 100 ))::numeric(20,2) end as grosscost, ";
                sql += "sellerinvoicevehicleentry_charges.charges_amount as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += "seller.id as client_id, ";
                sql += "seller.accountnumber as client_accountnumber, ";
                sql += "'seller' as clienttype, ";
                sql += "sellerinvoicevehicleentry_charges.charges_code as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry sellerinvoicevehicleentry ON vehicle.id = sellerinvoicevehicleentry.vehicle_id ";
                sql += "INNER JOIN public.sellerinvoicevehicleentry_charges sellerinvoicevehicleentry_charges ON sellerinvoicevehicleentry_charges.sellerinvoicevehicleentry_id = sellerinvoicevehicleentry.id ";
                sql += "WHERE rescinded = false ";
                sql += "UNION ALL   ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "saleresult.lot as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "saleresult.closingprice as grosscost, ";
                sql += "saleresult.closingprice as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += "buyer.id as client_id, ";
                sql += "buyer.accountnumber as client_accountnumber, ";
                sql += "'buyer' as clienttype, ";
                sql += "'sale price' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "WHERE   ";
                sql += "saleresult.status = 1 ";
                sql += "UNION ALL   ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "saleresult.lot as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(indemnity * (100 + vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "indemnity as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += "buyer.id as client_id, ";
                sql += "buyer.accountnumber as client_accountnumber, ";
                sql += "'buyer' as clienttype, ";
                sql += "'indemnity' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.buyerinvoicevehicleentry buyerinvoicevehicleentry ON vehicle.id = buyerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "indemnity > 0 and rescinded = false ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "saleresult.lot as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "(delivery * (100 + vatrate) / 100 )::numeric(20,2) as grosscost, ";
                sql += "delivery as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += "buyer.id as client_id, ";
                sql += "buyer.accountnumber as client_accountnumber, ";
                sql += "'buyer' as clienttype, ";
                sql += "'delivery' as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.buyerinvoicevehicleentry buyerinvoicevehicleentry ON vehicle.id = buyerinvoicevehicleentry.vehicle_id ";
                sql += "WHERE ";
                sql += "delivery > 0 and rescinded = false ";
                sql += "UNION ALL ";
                sql += "SELECT ";
                sql += "sale.site_id as sale_site_id, ";
                sql += "sale.id as sale_id, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') as sale_start, ";
                sql += "sale.description as sale_description, ";
                sql += "saleresult.lot as saleresult_lot, ";
                sql += "saleresult.status as saleresult_status, ";
                sql += "case when buyerinvoicevehicleentry_fees.fees_vatexempt = true then buyerinvoicevehicleentry_fees.fees_amount else (buyerinvoicevehicleentry_fees.fees_amount * ((100 + vatrate) / 100 ))::numeric(20,2) end as grosscost, ";
                sql += "buyerinvoicevehicleentry_fees.fees_amount as netcost, ";
                sql += "vehicle.id as vehicle_id, ";
                sql += "buyer.id as client_id, ";
                sql += "buyer.accountnumber as client_accountnumber, ";
                sql += "'buyer' as clienttype, ";
                sql += "buyerinvoicevehicleentry_fees.fees_code as tag ";
                sql += "FROM ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id ";
                sql += "INNER JOIN public.client buyer ON saleresult.buyer_id = buyer.id ";
                sql += "INNER JOIN public.buyerinvoicevehicleentry buyerinvoicevehicleentry ON vehicle.id = buyerinvoicevehicleentry.vehicle_id ";
                sql += "INNER JOIN public.buyerinvoicevehicleentry_fees buyerinvoicevehicleentry_fees ON buyerinvoicevehicleentry_fees.buyerinvoicevehicleentry_id = buyerinvoicevehicleentry.id ";
                sql += "WHERE rescinded = false ";
                sql += "ORDER by sale_id, saleresult_lot, client_id ";

                LogMsg("Extended Transaction SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted extended  transaction Data");
                LogMsg("Extracted extended  transaction Data");


                String fn = csvDataPath + "extendedtransactions" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written extended  transaction Data");
                LogMsg("Written extended transaction Data");

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
         * Created on 02/11/2016.
         * Gets sale result data for Phocas
         * @author andy
         *
         */
        private static void GetSaleResultCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Sale Results
                sql = "SELECT ";
                sql += "saleresult.\"sale_id\" AS saleresult_sale_id,";
                sql += "saleresult.\"lot\" AS saleresult_lot,";
                sql += "saleresult.\"vehicle_id\" AS saleresult_vehicle_id,";
                sql += "saleresult.\"closingprice\" AS saleresult_closingprice,";
                sql += "saleresult.\"status\" AS saleresult_status,";
                sql += "saleresult.\"buyer_id\" AS saleresult_buyer_id,";
                sql += "saleresult.\"soldstamp\" AS saleresult_soldstamp,";
                sql += "saleresult.\"bestbid\" AS saleresult_bestbid,";
                sql += "saleresult.\"salemethod\" AS saleresult_salemethod";
                sql += " FROM ";
                sql += "saleresult";
                sql += " ORDER BY saleresult.\"sale_id\", \"lot\"";

                LogMsg("SaleResult SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted saleresult Data");
                LogMsg("Extracted saleresult Data");


                String fn = csvDataPath + "saleresults" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written saleresult Data");
                LogMsg("Written saleresult Data");

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
         * Created on 03/11/2016.
         * Gets enhanced sale result data for Phocas
         * @author andy
         *
         */
        private static void GetDetailedSaleResultCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Detailed Sale Results
                sql = "SELECT ";
                sql += "min(site.id) as site_id,  ";
                sql += "min(sale.id) as sale_id,    ";
                sql += "min(to_char(sale.start, 'dd/mm/yyyy')) as sale_start,    ";
                sql += "min(sale.description) as sale_description,    ";
                sql += "min(case when saleresult.lot is null then 0 else saleresult.lot end) as saleresult_lot,    ";
                sql += "min(saleresult.status) as saleresult_status,    ";
                sql += "min(case when saleresult.status = 1 then saleresult.closingprice else 0 end) as saleresult_closingprice,    ";
                sql += "min(case when saleresult.status = 1 then saleresult.salemethod else '' end) as saleresult_method,    ";
                sql += "min(vehicle.registration) as vehicle_registration,    ";
                sql += "min(vehicle.id) as vehicle_id,    ";
                sql += "min(vehicle.calculatedpricing_clean) as vehicle_calculatedpricing_clean,    ";
                sql += "min(vehicle.pricing_reserveprice) as vehicle_pricing_reserveprice,    ";
                sql += "min(seller.accountnumber) as seller_accountnumber,    ";
                sql += "min(seller.name) as seller_name,    ";
                sql += "min(buyer.accountnumber) as buyer_accountnumber,    ";
                sql += "min(buyer.name) as buyer_name,    ";
                sql += "min(case when vehicle.calculatedpricing_clean is not null and saleresult.status = 1 then saleresult.closingprice else 0.0 end) as soldcapclean,    ";
                sql += "min(case when vehicle.pricing_reserveprice is not null  and saleresult.status = 1 then saleresult.closingprice else 0.0 end) as soldreserve,    ";
                sql += "min(case when saleresult.closingprice is not null and saleresult.status = 1 then vehicle.pricing_reserveprice else 0.0 end) as reserveclosing,    ";
                sql += "min(case when saleresult.closingprice is not null and saleresult.status = 1 then vehicle.calculatedpricing_clean else 0.0 end) as soldclosing,    ";
                sql += "min(case when vehicle.calculatedpricing_average is not null and saleresult.status = 1 then saleresult.closingprice else 0.0 end) as soldcapaverage,   ";
                sql += "min(case when saleresult.closingprice is not null and saleresult.status = 1 then vehicle.calculatedpricing_average else 0.0 end) as capaveragesold,    ";
                sql += "min(seller.id) as seller_id,    ";
                sql += "min(buyer.id) as buyer_id,    ";
                sql += "min(to_char(saleresult.soldstamp, 'dd/mm/yyyy')) as saleresult_soldstamp,    ";
                sql += "min(case when saleresult.webviews is null then 0 else saleresult.webviews end) as saleresult_webviews,    ";
                sql += "min(case when saleresult.uniquewebviews is null then 0 else saleresult.uniquewebviews end) as saleresult_uniquewebviews,    ";
                sql += "min(case when vehicle.damagecost is not null and saleresult.status = 1 then vehicle.damagecost else case when inspection.totaldamage is not null then inspection.totaldamage else 0 end end) as vehicledamage,   ";
                sql += "min(case when vehicle.damagecost is not null and saleresult.status = 1 then 1 else case when inspection.totaldamage is not null then 1 else 0 end end) as vehicledamagecount,   ";
                sql += "min(case when vehicle.mileage is not null and saleresult.status = 1 then vehicle.mileage else 0 end) as soldvehiclemileage,   ";
                sql += "min(case when vehicle.mileage is not null and saleresult.status = 1 then 1 else 0 end) as soldvehiclemileagecount,   ";
                sql += "min(sales_per_vehicle.count) as sales_per_vehicle,   ";
                sql += "min(extract('days' from (saleresult.soldstamp - vehicle.firstregistration))) as age,   ";
                sql += "min(inspection.grade) as grade,   ";
                sql += "min(case when saleresult.status = 0 then 1 else 0 end) as enteredcount,   ";
                sql += "min(case when saleresult.status = 1 then 1 else 0 end) as soldcount,   ";
                sql += "min(case when saleresult.status = 2 then 1 else 0 end) as unsoldcount,   ";
                sql += "min(case when saleresult.status = 3 then 1 else 0 end) as provisionalcount,  ";
                sql += "max(case when saleresult.status = 1 and sales_per_vehicle.count = 1 then 1 else 0 end) as firsttimesale,   ";
                sql += "min(case when vehicle.onhold is true then 1 else 0 end) as onholdcount,   ";
                sql += "min(case when vehicle.withdrawn is true then 1 else 0 end) as withdrawncount,   ";
                sql += "min(case when vehicle.exitdate is not null then ((EXTRACT(epoch from age(vehicle.exitdate, vehicle.entrydate)) / 86400)::int) else ((EXTRACT(epoch from age(NOW(), vehicle.entrydate)) / 86400)::int) end) AS daysonsite, ";
                sql += "min(case when saleresult.salemethod = 'Physical' then 1 else 0 end) as physicalcount,   ";
                sql += "min(case when saleresult.salemethod = 'Online' then 1 else 0 end) as onlinecount,   ";
                sql += "min(case when saleresult.salemethod = 'BidBuyNow' then 1 else 0 end) as bidbuynowcount,   ";
                sql += "min(abs(EXTRACT(EPOCH from (stamp - entrydate)) / 60))::integer as timetoweb,      ";
                sql += "min(case when inspection.grade is null and vehicle.exitdate is null and vehicle.withdrawn is false and vehicle.onhold is false then 0 else 1 end) as inspected,    "; 
                sql += "min(bive_ext.total_charges_net) as buyerchargesnet,  ";
                sql += "min(bive_ext.total_charges_gross) as buyerchargesgross,  ";
                sql += "min(case when saleresult.status = 1 and vehicle.mot is not null then 1 else 0 end) as motcount,   ";
                sql += "min(case when saleresult.status = 1 and vehicle.v5heldstate is not null and vehicle.v5heldstate != 'None' then 1 else 0 end) as v5count,   ";
                sql += "min(case when saleresult.status = 1 and vehicle.servicehistory is not null and vehicle.servicehistory != 'None' then 1 else 0 end) as shcount,   ";
                sql += "min(inspection.bodyplan) as inspectionbodyplan,  ";
                sql += "(sum(case when lower(extraspec) like '%sat nav operational - yes%' or lower(extraspec) like '%sat nav working%' then 1 else 0 end)) as satnavworking, ";
                sql += "(sum(case when lower(extraspec) like '%sat nav operational - no%' or lower(extraspec) like '%sat nav not working%' then 1 else 0 end)) as satnavnotworking, ";
                sql += "(sum(case when lower(extraspec) like '%alloys%' then 1 else 0 end)) as alloys, ";
                sql += "(sum(case when lower(colour) like '%metallic%' then 1 else 0 end )) as metallic, ";
                sql += "(sum(case when lower(extraspec) like '%air con%' then 1 else 0 end)) as aircon,     ";
                sql += "max(case when saleresult.status = 1 then extract('days' from (saleresult.soldstamp - firstregistration)) else 0 end) as soldvehicleage,   ";
                sql += "min(case when saleresult.status = 1 then 1 else 0 end) as soldvehicleagecount   ";
                sql += "FROM    ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id    ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id    ";
                sql += "LEFT OUTER JOIN public.sale sale ON saleresult.sale_id = sale.id    ";
                sql += "LEFT OUTER JOIN history on vehicle.id = history.vehicle_id and history.text like '%Edited vehicle%'   ";
                sql += "LEFT OUTER JOIN public.client buyer ON saleresult.buyer_id = buyer.id    ";
                sql += "LEFT OUTER JOIN public.inspection inspection ON vehicle.primaryinspection_id = inspection.id    ";
                sql += "LEFT OUTER JOIN public.sales_per_vehicle sales_per_vehicle ON vehicle.id = sales_per_vehicle.vehicle_id    ";
                sql += "LEFT OUTER JOIN public.buyerinvoicevehicleentry bive ON bive.vehicle_id = vehicle.id and bive.rescinded = false and saleresult.status = 1 ";
                sql += "LEFT OUTER JOIN public.bive_extended bive_ext on bive.id = bive_ext.id ";
                sql += "INNER JOIN public.site site ON vehicle.site_id = site.id    ";
                sql += "WHERE   ";
                sql += "vehicle.make is not null  ";
                sql += "and vehicle.entrydate is not null   ";
                sql += "group by sale.id, saleresult.lot  ";
                sql += "order by sale.id, saleresult.lot  ";

                LogMsg("Detailed SaleResult SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted detailed saleresult Data");
                LogMsg("Extracted detailed saleresult Data");

                String fn = csvDataPath + "detailedsaleresults" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written detailed saleresult Data");
                LogMsg("Written detailed saleresult Data");
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
         * Gets transport suppliers data for Phocas
         * @author andy
         *
         */
        private static void GetSuppliersCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Transport Suppliers
                sql = "select  ";
                sql += " id, ";
                sql += " name, ";
                sql += " contact_buildingname, ";
                sql += " contact_streetone, ";
                sql += " contact_streettwo, ";
                sql += " contact_town, ";
                sql += " contact_county, ";
                sql += " contact_postcode, ";
                sql += " contact_email, ";
                sql += " contact_mainphone ";
                sql += " from transportsupplier ";

                LogMsg("Transport Suppliers SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Supplier Data");
                LogMsg("Extracted Supplier Data");

                String fn = csvDataPath + "suppliers" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Supplier Data");
                LogMsg("Written Supplier Data");

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
         * Gets transport jobs for Phocas
         * @author andy
         *
         */
        private static void GetTransportJobCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Transport Jobs
                sql = "select  ";
                sql += " id, ";
                sql += " address_buildingname, ";
                sql += " address_streetone, ";
                sql += " address_streettwo, ";
                sql += " address_town, ";
                sql += " address_county, ";
                sql += " address_postcode, ";
                sql += " address_email, ";
                sql += " address_mainphone, ";
                sql += " destination_buildingname, ";
                sql += " destination_streetone, ";
                sql += " destination_streettwo, ";
                sql += " destination_town, ";
                sql += " destination_county, ";
                sql += " destination_postcode, ";
                sql += " destination_email, ";
                sql += " destination_mainphone, ";
                sql += " client_id, ";
                sql += " supplier_id, ";
                sql += " direction, ";
                sql += " miles, ";
                sql += " site_id, ";
                sql += " to_char(timestamp, 'dd/mm/yyyy') as timestamp, ";
                sql += " amountgross, ";
                sql += " amountnet ";
                sql += " from transportjob ";

                LogMsg("Transport Job SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Transport Job Data");
                LogMsg("Extracted Transport Job Data");

                String fn = csvDataPath + "transportjobs" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Transport Job Data");
                LogMsg("Written Transport Job Data");

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
         * Gets transport records for Phocas
         * @author andy
         *
         */
        private static void GetTransportRecordsCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Transport Records
                sql = "select  ";
                sql += " id, ";
                sql += " abortcode, ";
                sql += " charge, ";
                sql += " cost, ";
                sql += " fuel, ";
                sql += " movementid, ";
                sql += " notes, ";
                sql += " state, ";
                sql += " job_id, ";
                sql += " vehicle_id, ";
                sql += " inspectedoffsite ";
                sql += " from transportrecord ";

                LogMsg("Transport Records SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Transport Records Data");
                LogMsg("Extracted Transport Records Data");

                String fn = csvDataPath + "transportrecords" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Transport Records Data");
                LogMsg("Written Transport Records Data");
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
         * Created on 20/04/20167
         * Gets "Naughty" vehicles for Phocas
         * @author andy
         *
         */
        private static void GetNaughtyVehiclesCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all naughty vehicles
                sql = "select  ";
                sql += "id as vehicle_id, ";
                sql += "registration, ";
                sql += "count as sales_per_vehicle, ";
                sql += "to_char(vehicle.\"entrydate\", 'dd/mm/yyyy') AS vehicle_entrydate,";
                sql += "to_char(vehicle.\"exitdate\", 'dd/mm/yyyy') AS vehicle_exitdate,";
                sql += "saleresult.status, ";
                sql += "case when onhold is true then 1 else 0 end, ";
                sql += "lastresult_sale_id ";
                sql += "from  ";
                sql += "vehicle inner join sales_per_vehicle on vehicle.id = sales_per_vehicle.vehicle_id ";
                sql += "left outer join saleresult on saleresult.vehicle_id = vehicle.id and vehicle.lastresult_sale_id = saleresult.sale_id ";
                sql += "where  ";
                sql += "vehicle.exitdate is null and vehicle.entrydate is not null and ";
                sql += "((saleresult.status <> 1 and sales_per_vehicle.count > 5) or ";
                sql += "(vehicle.onhold is true and ((NOW()::date - vehicle.entrydate::date) > 14)) or ";
                sql += "(saleresult.status = 1 and ((NOW()::date - saleresult.soldstamp::date) > 14)) or ";
                sql += "(vehicle.withdrawn is true) or ";
                sql += "(vehicle.entrydate is not null and vehicle.lastresult_sale_id is null and ((NOW()::date - vehicle.entrydate::date) > 14))) ";
                sql += "order by sales_per_vehicle desc, entrydate ";

                LogMsg("Naughty Vehicles SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Naughty Vehicles Data");
                LogMsg("Extracted Transport Records Data");

                String fn = csvDataPath + "naughty" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Naughty Vehicles Data");
                LogMsg("Written Naughty Vehicles Data");
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
         * Gets arrival dates for Phocas
         * @author andy
         *
         */
        private static void GetArrivalsCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all vehicles on site

                sql += "SELECT d.date, ";
                sql += "count(distinct ca.id) as charrivals, ";
                sql += "count(distinct pa.id) as pharrivals, ";
                sql += "count(distinct wa.id) as wearrivals,  ";
                sql += "count(distinct la.id) as learrivals ";
                sql += "FROM ( ";
                sql += "    select to_char(date_trunc('day', (current_date - offs)), 'YYYY-MM-DD') ";
                sql += "AS date  ";
                sql += "FROM generate_series(0, 28, 7)  ";
                sql += "AS offs ";
                sql += ") d  ";
                sql += "LEFT OUTER JOIN ( ";
                sql += "SELECT id, entrydate FROM vehicle  ";
                sql += "WHERE site_id=1 ";
                sql += ") ca ";
                sql += "ON (d.date=to_char(date_trunc('day', ca.entrydate), 'YYYY-MM-DD'))  ";
                sql += "LEFT OUTER JOIN ( ";
                sql += "SELECT id, entrydate FROM vehicle  ";
                sql += "WHERE site_id=659780 ";
                sql += ") wa ";
                sql += "ON (d.date=to_char(date_trunc('day', wa.entrydate), 'YYYY-MM-DD'))  ";
                sql += "LEFT OUTER JOIN ( ";
                sql += "SELECT id, entrydate FROM vehicle  ";
                sql += "WHERE site_id=659779 ";
                sql += ") pa ";
                sql += "ON (d.date=to_char(date_trunc('day', pa.entrydate), 'YYYY-MM-DD'))  ";
                sql += "LEFT OUTER JOIN ( ";
                sql += "SELECT id, entrydate FROM vehicle  ";
                sql += "WHERE site_id=2360542 ";
                sql += ") la ";
                sql += "ON (d.date=to_char(date_trunc('day', la.entrydate), 'YYYY-MM-DD'))  ";
                sql += "GROUP BY d.date ";

                sql = "select vehicle.id as id, registration, site_id, lastresult_sale_id,  ";
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
                sql += "where entrydate is not null order by entrydate  ";



                LogMsg("Arrivals SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Arrivals Data");
                LogMsg("Extracted Arrivals Data");


                String fn = csvDataPath + "arrivals" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Arrivals Data");
                LogMsg("Written Arrivals Data");

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
         * Gets departure dates for Phocas
         * @author andy
         *
         */
        private static void GetDeparturesCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                sql = "select vehicle.id as id, registration, site_id, lastresult_sale_id,  ";
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
                sql += "where entrydate is not null and exitdate is not null order by entrydate  ";

                LogMsg("Departures SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Departures Data");
                LogMsg("Extracted Departures Data");

                String fn = csvDataPath + "departures" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Departures Data");
                LogMsg("Written Departures Data");

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
         * Created on 31/10/2017.
         * Gets Mobile / On;ine Bid stats etc for Phocas
         * @author andy
         *
         */
        private static void GetMobileStatsCSV()
        {
            try
            {
                string bidcsvFile = "c:\\temp\\salestats.csv";
                string lotcsvFile = "c:\\temp\\lotstats.csv";

                using (StreamWriter bidtext = new StreamWriter(bidcsvFile))
                {
                    using (StreamWriter lottext = new StreamWriter(lotcsvFile))
                    {
                        StringBuilder bidHeaders = new StringBuilder();
                        bidHeaders.Append("SaleNo").Append(",");

                        bidHeaders.Append("Site").Append(",");
                        bidHeaders.Append("Start").Append(",");
                        bidHeaders.Append("Lots").Append(",");
                        bidHeaders.Append("HallBidders").Append(",");
                        bidHeaders.Append("OnlineBidders").Append(",");
                        bidHeaders.Append("OnlineBids").Append(",");
                        bidHeaders.Append("MobileBidders").Append(",");
                        bidHeaders.Append("MobileBids").Append(",");
                        bidHeaders.Append("OnlineClients").Append(",");
                        bidHeaders.Append("MobileClients").Append("\n");
                        bidtext.Write(bidHeaders);

                        StringBuilder lotHeaders = new StringBuilder();
                        lotHeaders.Append("SaleNo").Append(",");

                        lotHeaders.Append("Lot").Append(",");
                        lotHeaders.Append("Registration").Append(",");
                        lotHeaders.Append("Make").Append(",");
                        lotHeaders.Append("Model").Append(",");
                        lotHeaders.Append("Name").Append(",");
                        lotHeaders.Append("Company").Append(",");
                        lotHeaders.Append("Type").Append(",");
                        lotHeaders.Append("Outcome").Append(",");
                        lotHeaders.Append("Time").Append("\n");
                        lottext.Write(lotHeaders);

                        // FTP all the transaction logs we might need
                        for (int saleNo = 1100; saleNo < 3000; saleNo++)
                        {
                            try
                            {
                                FTPXML.GetXMLFile(saleNo);

                                ShowTranscriptOfSale.ProcessXml(saleNo);

                                ShowTranscriptOfSale.ProcessTransactionLog();

//                                string bidCSVData = ShowTranscriptOfSale.SaveBidData(ShowTranscriptOfSale.ThisSale.SaleNo);
                                string lotCSVData = ShowTranscriptOfSale.SaveLotData(ShowTranscriptOfSale.ThisSale.SaleNo);

//                                LogMsg("Bid CSV " + bidCSVData);
                                LogMsg("Lot CSV " + lotCSVData);
                                if (saleNo == ShowTranscriptOfSale.ThisSale.SaleNo)
                                {
//                                    bidtext.Write(bidCSVData);
//                                    bidtext.Write("\n");
                                    lottext.Write(lotCSVData);
                                    lottext.Write("\n");
                                }
                            }
                            catch (Exception ee)
                            {
                                LogMsg("Something went wrong" + ee.Message);
                            }
                        }
                    }
                }

            }
            catch (Exception ee)
            {
                log.Warn("Couldn't get Mobile data");
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
