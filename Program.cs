using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Research.Science.Data.Imperative;
using sds = Microsoft.Research.Science.Data;
namespace NcToText
{
    class Program
    {
        private static string fileAdd = @"";
        private static string outAdd = @"";
        private static string fileName = @"";
        static void Main(string[] args)
        {
            Console.WriteLine("Enter nc(with mdb) folder address:");
            fileAdd = Console.ReadLine();
            Console.WriteLine("Enter output folder address");
            outAdd = Console.ReadLine();

            Console.WriteLine("Start Latitude From?");
            var fromLat = Convert.ToDouble(Console.ReadLine());
            Console.WriteLine("End Latitude To?");
            var toLat = Convert.ToDouble(Console.ReadLine());
            Console.WriteLine("Start Longitude From?");
            var fromLon = Convert.ToDouble(Console.ReadLine());
            Console.WriteLine("End Longitude To?");
            var toLon = Convert.ToDouble(Console.ReadLine());
            Console.WriteLine("pcp or tmp?");
            var isPcp =Console.ReadLine().ToLower()=="pcp"?true:false;

            var pcpFiles = Directory.GetFiles(fileAdd).Where(x => x.Contains("PCP")).OrderBy(x => x.ToString()).ToList();
            //var posRecs = context.PositionRecords.Where(x => !string.IsNullOrEmpty(x.CRU_Name)).ToList();
            var posRecs = FindFileNamesCru(fromLat, fromLon, toLat, toLon);
            RunAllNc(pcpFiles, posRecs, isPcp);

            Console.WriteLine("end at " + DateTime.Now);
            Console.ReadKey();
        }
        private static void RunAllNc(List<string> pcpFileAdd, List<Position> posRecs,bool isPcp)
        {
            fileName = pcpFileAdd[0];
           // var days = 0;

            //var fromDays = 0;// int.Parse(FromDays.Text);
            var details = fileName.Replace(".nc", "").Split('_');
            var model = details[1];
            var scenario = details[2];

            var startDate = new DateTime(int.Parse(details[3]), 1, 1);


            var pcp0 = sds.DataSet.Open(pcpFileAdd[0] + "?openMode=readOnly&include=prAdjust");
            //var datasetmn = sds.DataSet.Open(pcpFileAdd.Replace("PCP", "TMN") + "?openMode=readOnly");
            //var datasetmx = sds.DataSet.Open(pcpFileAdd.Replace("PCP", "TMX") + "?openMode=readOnly");

           // var time = pcp0.GetData<double[]>("time").ToList();
            var mis = pcp0.GetAttr<float>("prAdjust", "missing_value");

            //if (ToDays.Text == "-1")
            //{
            //days = time.Count - 1;
            //}
            //time = null;
            var lon = pcp0.GetData<double[]>("lon").Select((value, index) => new NcLoc() { Val = value, Id = index }).ToList();
            var lat = pcp0.GetData<double[]>("lat").Select((value, index) => new NcLoc() { Val = value, Id = index }).ToList();


           // var insertInd = 0;

            //var histoicPos = posRecs.Where(x => !string.IsNullOrEmpty(x.CRU_Name));
            var stringDate = startDate.Year + startDate.Month.ToString("00") + startDate.Day.ToString("00");
            var pcpDir = Directory.CreateDirectory(outAdd+@"/" + model + "/" + scenario + "/pcp/");
            var tmpDir = Directory.CreateDirectory(outAdd+@"/" + model + "/" + scenario + "/tmp/");
            Console.WriteLine(posRecs.Count + " files will be created");
            var itemIndex = 0;
            //Parallel.ForEach(posRecs, positionRecord =>
            //{
            //    //Your stuff
            //});
            var validpcp = new List<NcModel>();

            for (int i = 0; i < pcpFileAdd.Count; i++)
            {
          
                var datasetPCPi = sds.DataSet.Open(pcpFileAdd[i] + "?openMode=readOnly&include=prAdjust");
                //    var pcpss = datasetPCP.GetData<float[, ,]>("prAdjust", sds.DataSet.FromToEnd(0),
           
                var datasetmn = sds.DataSet.Open(pcpFileAdd[i].Replace("PCP", "TMN") + "?openMode=readOnly&include=tasminAdjust");
                var datasetmx = sds.DataSet.Open(pcpFileAdd[i].Replace("PCP", "TMX") + "?openMode=readOnly&include=tasmaxAdjust");
               
                var time = datasetPCPi.GetData<double[]>("time").ToList();
                var finishdDay = 0;
                for (int t = 0; t < time.Count; t=t+365)
                {
                    var endIndex = Math.Min(t + 364, time.Count - 1);
                    finishdDay = finishdDay + endIndex;
                    itemIndex = 0;
                    if (isPcp) { 
                    var pcpss = datasetPCPi.GetData<float[, ,]>("prAdjust", sds.DataSet.Range(t, endIndex),
                   sds.DataSet.FromToEnd(0), sds.DataSet.FromToEnd(0));
                    //foreach (var positionRecord in posRecs)
                    //{
                        Parallel.ForEach(posRecs, positionRecord =>
                        {
                            itemIndex++;
                            var latitude = Convert.ToDouble(positionRecord.Latitude);
                            var longitude = Convert.ToDouble(positionRecord.Longitude);

                            var theLat = lat.SingleOrDefault(x => x.Val == latitude);
                            if (theLat != null)
                            {
                                int localI = (int) theLat.Id;
                                var theLon = lon.SingleOrDefault(x => x.Val == longitude);
                                if (theLon != null)
                                {
                                    int localJ = (int) theLon.Id;
                                    int currentIndex = (localJ*lon.Count) + (localI*lat.Count);
                                    GeneratePcpTxt(pcpFileAdd, localI, localJ, positionRecord, stringDate, mis, pcpDir,
                                        pcpss, currentIndex);
                                }
                            }


                            Console.WriteLine(itemIndex + " : " + positionRecord.Cru);

                        });

                    }
                    else
                    {
                           var tmnss = datasetmn.GetData<float[, ,]>("tasminAdjust", sds.DataSet.Range(t, endIndex),
                     sds.DataSet.FromToEnd(0), sds.DataSet.FromToEnd(0));
                    var tmxss = datasetmx.GetData<float[, ,]>("tasmaxAdjust", sds.DataSet.Range(t, endIndex),
                        sds.DataSet.FromToEnd(0), sds.DataSet.FromToEnd(0));
                    //foreach (var positionRecord in posRecs)
                    //{
                        Parallel.ForEach(posRecs, positionRecord =>
                        {
                            itemIndex++;
                            var latitude = Convert.ToDouble(positionRecord.Latitude);
                            var longitude = Convert.ToDouble(positionRecord.Longitude);

                            var theLat = lat.SingleOrDefault(x => x.Val == latitude);
                            if (theLat != null)
                            {
                                int localI = (int) theLat.Id;
                                var theLon = lon.SingleOrDefault(x => x.Val == longitude);
                                if (theLon != null)
                                {
                                    int localJ = (int) theLon.Id;
                                    int currentIndex = (localJ*lon.Count) + (localI*lat.Count);
                                    GenerateTmpTxt(pcpFileAdd, localI, localJ, positionRecord, stringDate, mis, tmpDir,
                                        tmnss, tmxss, currentIndex);
                                }
                            }


                            Console.WriteLine(itemIndex + " : " + positionRecord.Cru);

                        });
                    }
                 
                  
                  
                    Console.ForegroundColor = System.ConsoleColor.Green;
                    Console.WriteLine(finishdDay + " days complete ");
                    Console.ResetColor();
                }
               

                
                
            }
        }

        private static void GenerateTmpTxt(List<string> pcpFileAdd, int localI, int localJ, Position positionRecord, string stringDate, float mis, DirectoryInfo dir, float[, ,] tmnss, float[, ,] tmxss, int currentIndex)
        {
            var validtmn = new List<NcModel>();
            var validtmx = new List<NcModel>();
            //for (int i = 0; i < pcpFileAdd.Count; i++)
            //{
                

                //var tmnList = UsingBlockCopy(tmnss);
                validtmn.AddRange(
                   UsingBlockCopy(tmnss , localI, localJ)
                       .Select((value, index) => new NcModel() { Val = (float) (value == mis ? -99 : value - 273.15), Id = index })
                       .ToList());

                // var tmxList = UsingBlockCopy(tmxss);
                validtmx.AddRange(
                   UsingBlockCopy(tmxss, localI, localJ)
                       .Select((value, index) => new NcModel() { Val = (float) (value == mis ? -99 : value - 273.15), Id = index })
                       .ToList());
            //}

            var tmp = validtmx.Join(validtmn, x => x.Id, y => y.Id, (x, y) => new { tmx = x.Val, tmn = y.Val });


            var tmpFile = dir.FullName + positionRecord.Cru + "t.txt";
            var tmpDatas = tmp.Select(x => x.tmx.ToString("F2") + "," + x.tmn.ToString("F2")).ToList();
            if (!File.Exists(tmpFile))
            tmpDatas.Insert(0, stringDate);

            File.AppendAllLines(tmpFile, tmpDatas);
        }

        private static void GeneratePcpTxt(List<string> pcpFileAdd, int localI, int localJ, Position positionRecord, string stringDate, float mis, DirectoryInfo dir, float[,,] pcpss, int currentIndex)
        {
            var validpcp = new List<NcModel>();

            //for (int i = 0; i < pcpFileAdd.Count; i++)
            //{
            //    var datasetPCP = sds.DataSet.Open(pcpFileAdd[i] + "?openMode=readOnly&include=prAdjust");
            ////    var pcpss = datasetPCP.GetData<float[, ,]>("prAdjust", sds.DataSet.FromToEnd(0),
            ////sds.DataSet.Range(localI, localI), sds.DataSet.Range(localJ, localJ));
        //        var pcpss = datasetPCP.GetData<float[, ,]>("prAdjust", sds.DataSet.FromToEnd(0),
        //sds.DataSet.FromToEnd(0), sds.DataSet.FromToEnd(0));
           
                validpcp.AddRange(
             UsingBlockCopy(pcpss, localI, localJ)
             .Select((value, index) => new NcModel() { Val = value == mis ? -99 : (value * 86400), Id = index })
                 .ToList());
            //}
                var pcpFile = dir.FullName + positionRecord.Cru + "p.txt";
                var pcpDatas = validpcp.Select(x => x.Val.ToString("F2")).ToList();
                if (!File.Exists(pcpFile))
                pcpDatas.Insert(0, stringDate);

                File.AppendAllLines(pcpFile, pcpDatas);  
        }

        private static List<float> UsingBlockCopy(float[, ,] array, int localI, int localJ)
        {
            
            List<float> list = new List<float>();
            for (int i = 0; i < array.GetLength(0); i++)
            {
                list.Add(array[i, localI, localJ]);
            }
            return list;
        }

        private static List<float> UsingBlockCopy(float[, ,] array, int currentIndex)
        {
            float[] tmp = new float[array.GetLength(0)];
            Buffer.BlockCopy(array, currentIndex, tmp, 0, tmp.Length * sizeof(float));
            List<float> list = new List<float>(tmp);
            return list;
        }


        static List<float> UsingBlockCopy(float[, ,] array)
        {
            float[] tmp = new float[array.GetLength(0) * array.GetLength(1) * array.GetLength(2)];
            Buffer.BlockCopy(array, 0, tmp, 0, tmp.Length * sizeof(float));
            List<float> list = new List<float>(tmp);
            return list;
        }
        static List<Position> FindFileNamesCru(double LatitudeFrom, double LongitudeFrom, double LatitudeTo, double LongitudeTo)
        {

            OleDbConnection con = new OleDbConnection();
            string cs = @"Provider=Microsoft.ACE.OLEDB.16.0;Data Source=" + fileAdd + @"\positionDB.mdb";
            con.ConnectionString = cs;
            string query = "SELECT distinct cruPosition.File_Name as cru,cruPosition.Longitude,cruPosition.Latitude from cruPosition where  (cruPosition.Latitude>=" + LatitudeFrom +
                           " and cruPosition.Longitude>=" + LongitudeFrom + " and  cruPosition.Latitude<=" + LatitudeTo +
                           " and cruPosition.Longitude<=" + LongitudeTo + ")";
            OleDbDataAdapter da = new OleDbDataAdapter(query, con);
            con.Open();
            DataTable dt = new DataTable();
            da.Fill(dt);
            var list = (from DataRow row in dt.Rows
                        select new Position()
                        {
                            Cru = row["cru"].ToString(),
                            Latitude = row["Latitude"].ToString(),
                            Longitude = row["Longitude"].ToString()
                        }).ToList();

            con.Close();
            return list;
        }


    }
    internal class Position
    {
        public string Cru { get; set; }

        public string Latitude { get; set; }
        public string Longitude { get; set; }
    }
    internal class MainDataDay
    {
        public string model { get; set; }
        public string Scenario { get; set; }
        public float Pcp { get; set; }
        public double Tmn { get; set; }
        public double Tmx { get; set; }
        public DateTime InsertDate { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public class NcModel
    {
        [Key]
        public long Id { get; set; }
        public float Val { get; set; }
    }
    public class NcLoc
    {
        [Key]
        public long Id { get; set; }
        public double Val { get; set; }
    }

}
