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
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '4 Year' and NOW() then 'U to 4 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '5 Year' and NOW() then 'Up to 5 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '6 Year' and NOW() then 'Up to 6 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '7 Year' and NOW() then 'Up to 7 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '8 Year' and NOW() then 'Up to 8 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '9 Year' and NOW() then 'Up to 9 years'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '10 Year' and NOW() then 'Up to 10 years'  ";
            vehiclestats += "ELSE 'Over 10 year' ";
            vehiclestats += "End as Age_cat, ";
            vehiclestats += "CASE  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '2.5 Year' and NOW() then 'Late & Low'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '4.5 Year' and NOW() then 'Fleet Profile'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '6.5 Year' and NOW() then 'PX Young'  ";
            vehiclestats += "WHEN firstregistration between NOW() - INTERVAL '10.5 Year' and NOW() then 'PX old'  ";
            vehiclestats += "ELSE 'Budget' ";
            vehiclestats += "End as Industry_Age_cat, ";
            vehiclestats += "CASE ";
            vehiclestats += "WHEN bodystyle like '%Cabriolet%' or bodystyle like '%CABRIOLET%' then 'Cabriolet' ";
            vehiclestats += "WHEN bodystyle like '%Convertible%' or bodystyle like '%CONVERTIBLE%' then 'Convertible' ";
            vehiclestats += "WHEN bodystyle like '%Coupe%' or bodystyle like '%CABRIOLET%' then 'Coupe' ";
            vehiclestats += "WHEN bodystyle = 'Double Cab Pick-up' or bodystyle = 'Double Cab Dropside' or bodystyle = 'Double Cab Tipper' or bodystyle = 'Double Chassis Cab' then 'Double Cab Pick-up' ";
            vehiclestats += "WHEN bodystyle like '%Estate%' or bodystyle like '%ESTATE%' then 'Estate' ";
            vehiclestats += "WHEN bodystyle like '%Hardtop%' then 'Cabriolet' ";
            vehiclestats += "WHEN bodystyle like '%Hatchback%' or bodystyle like '%HATCHBACK%' then 'Hatchback' ";
            vehiclestats += "WHEN bodystyle like '%High Volume/High Roof Van%' then 'High Volume/High Roof Van' ";
            vehiclestats += "WHEN bodystyle = 'Medium Roof Van' then 'Medium Roof Van' ";
            vehiclestats += "WHEN bodystyle like '%Roadster%' then 'Roadster' ";
            vehiclestats += "WHEN bodystyle like '%Saloon%' or bodystyle like '%SALOON%' then 'Saloon' ";
            vehiclestats += "WHEN bodystyle like '%Station Wagon%' then 'Station Wagon' ";
            vehiclestats += "WHEN bodystyle like '%Van%' then 'Van' ";
            vehiclestats += "ELSE 'Others' ";
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
            vehiclestats += "when lower(vehicle.colour) like '%beige%' then 'Beige'  ";
            vehiclestats += "when lower(vehicle.colour) like '%buff%' then 'Beige'  ";
            vehiclestats += "when lower(vehicle.colour) like '%black%' then 'Black'  ";
            vehiclestats += "when lower(vehicle.colour) like '%blue%' then 'Blue'  ";
            vehiclestats += "when lower(vehicle.colour) like '%bronze%' then 'Bronze' ";
            vehiclestats += "when lower(vehicle.colour) like '%brown%' then 'Brown'  ";
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
            vehiclestats += "WHEN sale.site_id= 659780 THEN 'AB Westbury' ";
            vehiclestats += "ELSE 'AB Leeds' ";
            vehiclestats += "END AS Site_Location, ";

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

            // Generate Client CSV
            GetClientWithGroupCSV();

            // Generate Vehicle CSV
            GetVehicleCSV();

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
            GetNaughtyVehiclesCSV();

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

            sqlconnection = "Server=" + lines[0] + ";Port=" + lines[1] + ";Database=" + lines[2] + ";User Id=" + lines[3] + ";Password=" + password + ";" + "CommandTimeout = 720;";

            conn = new NpgsqlConnection(sqlconnection);

            return;
        }

        /**
         * Created on 02/11/2016.
         * Writes the contents of a DataReader to a .csv file line by line for Phocas
         * @author andy
         *
         */
        private static void WriteCSV(string fn, NpgsqlDataReader dr)
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
                sql += "WHEN bodystyle like '%Cabriolet%' or bodystyle like '%CABRIOLET%' then 'Cabriolet' ";
                sql += "WHEN bodystyle like '%Convertible%' or bodystyle like '%CONVERTIBLE%' then 'Convertible' ";
                sql += "WHEN bodystyle like '%Coupe%' or bodystyle like '%CABRIOLET%' then 'Coupe' ";
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
                sql += "WHEN firstregistration between NOW() - INTERVAL '30 Month' and NOW() then 'Late & Low' ";
                sql += "WHEN firstregistration between NOW() - INTERVAL '54 Month' and NOW() then 'Fleet Profile' ";
                sql += "WHEN firstregistration between NOW() - INTERVAL '78 Month' and NOW() then 'PX Young' ";
                sql += "WHEN firstregistration between NOW() - INTERVAL '126 Month' and NOW() then 'PX Old' ";
                sql += "else 'Budget' end AS vehicle_age,";
                sql += "vehicle.\"fuel\" AS vehicle_fuel, ";
                sql += "CASE ";
                sql += "WHEN fuel like '%Hybrid%' or fuel like '%HYB%' then 'Hybrid' ";
                sql += "WHEN fuel = 'Diesel' or fuel = 'DIESEL' then 'Diesel' ";
                sql += "WHEN fuel = 'Electric' or fuel = 'ELECTRIC' then 'Electric' ";
                sql += "WHEN fuel = 'petrol' or fuel = 'Petrol' or fuel = 'PETROL' or fuel like '%Petrol/Bio-Ethanol%'then 'Petrol' ";
                sql += "WHEN fuel = 'Petrol/ELE' or fuel = 'Petrol/Gas' or fuel = 'PETROL/GAS' or fuel = 'Petrol/LPG' or fuel = 'PETROL/ELE' then 'Petrol'	 ";
                sql += "ELSE 'Others' ";
                sql += "END AS FuelType, ";
                sql += "vehicle.\"make\" AS vehicle_make,";
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
                sql += "vehicle.\"model\" AS vehicle_model,";
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
                sql += "inspection.\"totaldamage\" AS inspection_totaldamage, ";
                sql += "case when inspection.\"nama\" is null or LENGTH(inspection.nama) < 1 then 'N/A' else inspection.\"nama\" end AS inspection_nama, ";
                sql += "case when vehicle.exitdate is null then (date_part('day',age(NOW(), vehicle.entrydate))) else (date_part('day',age(vehicle.exitdate, vehicle.entrydate))) end as daysonsite, ";
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
                sql += "case when (inspection.date is not null and vehicle.entrydate is not null) then DATE_PART('day', inspection.date - vehicle.entrydate) * 24 * 60 + ";
                sql += "DATE_PART('hour', inspection.date - vehicle.entrydate) * 60   + ";
                sql += "DATE_PART('minute', inspection.date - vehicle.entrydate)end AS time_to_inspect ";
                sql += "FROM ";
                sql += "\"public\".\"vehicle\" vehicle INNER JOIN \"public\".\"sales_per_vehicle\" sales_per_vehicle ON vehicle.\"id\" = sales_per_vehicle.\"vehicle_id\" ";
                sql += "LEFT OUTER JOIN \"public\".\"inspection\" inspection ON vehicle.\"primaryinspection_id\" = inspection.\"id\"   ";
                sql += "LEFT OUTER JOIN public.image image ON inspection.costedreport_id = image.id   ";
                sql += " WHERE";
                sql += " vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += " ORDER BY vehicle.\"id\"";

                LogMsg("Vehicle SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Vehicle Data");
                LogMsg("Extracted Vehicle Data");


                String fn = csvDataPath + "vehicles" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Vehicle Data");
                LogMsg("Written Vehicle Data");

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
                sql += " client.\"creditlimit\" AS client_creditlimit ";
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
                sql += " max(client.buyer_type) AS client_buyer_type ";
                sql += "FROM ";
                sql += " client LEFT OUTER JOIN client_grouptag client_grouptag ON client.id = client_grouptag.client_id ";
                sql += " LEFT OUTER JOIN grouptag grouptag ON client_grouptag.groups_id = grouptag.id ";
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
                sql += "min(sale.site_id) as sale_site_id,  ";
                sql += "min(sale.id) as sale_id,    ";
                sql += "min(to_char(sale.start, 'dd/mm/yyyy')) as sale_start,    ";
                sql += "min(sale.description) as sale_description,    ";
                sql += "min(case when saleresult.lot is null then 0 else saleresult.lot end) as saleresult_lot,    ";
                sql += "min(saleresult.status) as saleresult_status,    ";
                sql += "min(saleresult.closingprice) as saleresult_closingprice,    ";
                sql += "min(saleresult.salemethod) as saleresult_method,    ";
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
                sql += "min(extract('days' from (NOW() - vehicle.firstregistration))) as age,   ";
                sql += "min(inspection.grade) as grade,   ";
                sql += "min(case when saleresult.status = 0 then 1 else 0 end) as enteredcount,   ";
                sql += "min(case when saleresult.status = 1 then 1 else 0 end) as soldcount,   ";
                sql += "min(case when saleresult.status = 2 then 1 else 0 end) as unsoldcount,   ";
                sql += "min(case when saleresult.status = 3 then 1 else 0 end) as provisionalcount,  ";
                sql += "min(case when saleresult.status = 1 and sales_per_vehicle.count = 1 then 1 else 0 end) as firsttimesale,   ";
                sql += "min(case when vehicle.onhold is true then 1 else 0 end) as onholdcount,   ";
                sql += "min(case when vehicle.withdrawn is true then 1 else 0 end) as withdrawncount,   ";
                sql += "min(case when vehicle.exitdate is null then (date_part('day',age(NOW(), vehicle.entrydate))) else (date_part('day',age(vehicle.exitdate, vehicle.entrydate))) end) as daysonsite,   ";
                sql += "min(case when saleresult.salemethod = 'Physical' then 1 else 0 end) as physicalcount,   ";
                sql += "min(case when saleresult.salemethod = 'Online' then 1 else 0 end) as onlinecount,   ";
                sql += "min(case when saleresult.salemethod = 'BidBuyNow' then 1 else 0 end) as bidbuynowcount,   ";
                sql += "min(EXTRACT(EPOCH from (stamp - entrydate)) / 60)::integer as timetoweb,      ";
                sql += "min(case when inspection.grade is null and vehicle.exitdate is null and vehicle.withdrawn is false and vehicle.onhold is false then 0 else 1 end) as inspected    "; 
                sql += "FROM    ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id    ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id    ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id    ";
                sql += "INNER JOIN history on vehicle.id = history.vehicle_id and history.text like '%Edited vehicle (\"v5HeldState%' and history.stamp > vehicle.entrydate   ";
                sql += "LEFT OUTER JOIN public.client buyer ON saleresult.buyer_id = buyer.id    ";
                sql += "LEFT OUTER JOIN public.inspection inspection ON vehicle.primaryinspection_id = inspection.id    ";
                sql += "LEFT OUTER JOIN public.sales_per_vehicle sales_per_vehicle ON vehicle.id = sales_per_vehicle.vehicle_id    ";
                sql += "WHERE   ";
                sql += "vehicle.vatstatus is not null and vehicle.make is not null  ";
                sql += "and vehicle.entrydate is not null   ";
                sql += "GROUP by vehicle.entrydate  ";
                sql += "ORDER by vehicle.entrydate  ";

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
                sql += "registration, ";
                sql += "count as sales_per_vehicle, ";
                sql += "entrydate, ";
                sql += "exitdate, ";
                sql += "saleresult.status, ";
                sql += "onhold, ";
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

                String fn = csvDataPath + "transportrecords" + ".csv";

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

                sql += "select vehicle_id, case when sale_id is null then 0 else sale_id end as sale_id, ipaddress,";
                sql += "case when position('country_code' IN geodata) > -1 then substring(geodata from (position('country_code' in geodata) + 15) for 2) else '' end as countrycode,";
                sql += "case when position('region_code' in geodata) > -1 then substring(geodata from (position('region_code' in geodata) + 14) for 3) else '' end as regioncode,";
                sql += "case when position('zip_code' in geodata) > -1 then substring(geodata from (position('zip_code' in geodata) + 11) for 4) else '' end as postcode ";
                sql += "from webanalytics ";
                sql += "group by sale_id, ipaddress, vehicle_id, geodata order by sale_id, ipaddress, vehicle_id";

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
            String[] logmsg = { info };

            log.Info(logmsg);

            return;
        }

        static public void LogMsg(Exception e)
        {
            String[] logmsg = { e.Message };

            log.Fatal(logmsg, e);

            return;
        }

    }

}
