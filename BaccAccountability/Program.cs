using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TermLogic;

namespace BaccAccountability
{
    class Program
    {
        static void Main(string[] args)
        {
            StreamWriter output = new StreamWriter("..\\..\\..\\UpperLevelEnrollmentHoursByProgram.csv");
            
            output.WriteLine("Program,In-State Program Hours,Out-of-State Program Hours");

            SqlConnection conn = new SqlConnection("server=vulcan;Trusted_Connection=true;database=StateSubmission");
            SqlCommand comm;
            SqlDataReader reader;
           
            List<String> inStateStudents = new List<string>();
            List<String> programs = new List<string>();
            Dictionary<String, int> inStateProgramHours = new Dictionary<string, int>();
            Dictionary<String, int> outOfStateProgramHours = new Dictionary<string, int>();

            try
            {
                conn.Open();
            }
            catch (Exception)
            {
                throw;
            }
            
            for (int j = 2; j < 6; j++)
            {
                StateReportingYear year = new StateReportingTermShort("11" + j).getStateReportingYear();

                output.WriteLine("State Reporting Year," + year);

                comm = new SqlCommand(@"SELECT
	                                        r6.DE1021
	                                        ,SUM(CASE WHEN r1.[DE1004-RESIDENCE-FEE] <> 'F' THEN 1 ELSE 0 END) AS [Residencies]
                                        FROM
	                                        StateSubmission.SDB.RecordType6 r6
	                                        INNER JOIN MIS.dbo.vwTermYearXwalk xwalk ON xwalk.StateReportingTerm = r6.DE1028
	                                        INNER JOIN StateSubmission.SDB.RecordType1 r1 ON r1.[STUDENT-ID] = r6.DE1021
												                                          AND r1.[TERM-ID] = r6.DE1028
												                                          AND r1.SubmissionType = r6.SubmissionType
                                        WHERE
	                                        SUBSTRING(r6.DE3008, 4, 1) IN ('3','4')
	                                        AND xwalk.StateReportingYear = '" + year + @"'
	                                        AND r6.SubmissionType = 'E'
                                        GROUP BY
	                                        r6.DE1021", conn);

                reader = comm.ExecuteReader();

                while (reader.Read())
                {
                    String studentID = reader["DE1021"].ToString();

                    int residenciesCount = int.Parse(reader["Residencies"].ToString());

                    if (residenciesCount == 0)
                    {
                        inStateStudents.Add(studentID);
                    }
                }

                reader.Close();

                for (StateReportingTermShort i = year.getNthTerm(1); i < year.getNthTerm(3); i++)
                {
                        comm = new SqlCommand(@"IF OBJECT_ID('tempdb..#upperLevelEnrollments') IS NOT NULL
	                                                DROP TABLE #upperLevelEnrollments

                                                SELECT
	                                                r6.DE1021
	                                                ,SUM(CAST(LEFT(r6.DE3012, 4) AS INT)) AS [Hours]
                                                INTO
	                                                #upperLevelEnrollments
                                                FROM
	                                                StateSubmission.SDB.RecordType6 r6
                                                WHERE
	                                                SUBSTRING(r6.DE3008, 4, 1) IN ('3','4')
	                                                AND r6.DE1028 = '" + i + @"'
	                                                AND r6.SubmissionType = 'E'
                                                    AND r6.DE3018 = 'Z'
                                                    AND r6.DE3006 NOT IN ('C','D','G')
                                                    AND r6.DE3010 = 'N'
                                                GROUP BY
	                                                r6.DE1021

                                                SELECT
	                                                u.DE1021
	                                                ,[1],[2],[3],[4],[5],[6],[9],[A],[B],[C],[D],[F],[G],[H],[I],[P],[T],[Z]
	                                                ,u.Hours
	                                                ,CASE
		                                                WHEN SRC.[C] > 0 THEN Adhoc.dbo.RAND_SELECT(upperProg.PGM_CD)
		                                                WHEN SRC.[H] > 0 THEN Adhoc.dbo.RAND_SELECT(upperProgNonDegree.PGM_CD)
		                                                WHEN SRC.[1] > 0 OR SRC.[2] > 0 OR SRC.[3] > 0 OR SRC.[9] > 0 OR SRC.[4] > 0 OR SRC.[5] > 0 
                                                          OR SRC.[A] > 0 OR SRC.[B] > 0 OR SRC.[D] > 0 OR SRC.[G] > 0 OR SRC.[I] > 0 OR SRC.[P] > 0 
                                                          OR SRC.[T] > 0 OR SRC.[Z] > 0 THEN 'Non-Bacc'
	                                                END AS [Program]
                                                FROM
	                                                #upperLevelEnrollments u
	                                                LEFT JOIN (
		                                                SELECT 
			                                                DE1021 AS Student,
			                                                [1],[2],[3],[4],[5],[6],[9],[A],[B],[C],[D],[F],[G],[H],[I],[P],[T],[Z]
		                                                FROM
			                                                (SELECT 
				                                                DE1021
				                                                ,DE2001
			                                                FROM 
				                                                StateSubmission.SDB.RecordType4
			                                                WHERE 
				                                                DE1028 = '" + i + @"'
				                                                AND SubmissionType = 'E'
				                                                AND DE1021 IN (SELECT DE1021 FROM #upperLevelEnrollments)) AS SourceTable
			                                                PIVOT
			                                                (COUNT(DE2001)
			                                                FOR DE2001 IN ([1],[2],[3],[4],[5],[6],[9],[A],[B],[C],[D],[F],[G],[H],[I],[P],[T],[Z])) AS PivotTable
			                                                ) SRC ON SRC.Student = u.DE1021
	                                                LEFT JOIN StateSubmission.SDB.RecordType4 r4 ON r4.DE1021 = u.DE1021
												                                                    AND r4.DE1028 = '" + i + @"'
												                                                    AND r4.SubmissionType = 'E'
	                                                LEFT JOIN MIS.dbo.ST_PROGRAMS_A_136 upperProg ON upperProg.CIP_CD = r4.DE2002
												                                                    AND LEFT(upperProg.AWD_TY, 1) = 'B'
												                                                    AND upperProg.EFF_TRM_D <> ''
													                                                AND SUBSTRING(upperProg.PGM_CD, 2, 2) <> '00'
	                                                LEFT JOIN MIS.dbo.ST_PROGRAMS_A_136 upperProgNonDegree ON upperProgNonDegree.CIP_CD = r4.DE2002
														                                            AND LEFT(upperProgNonDegree.AWD_TY, 1) = 'B'
														                                            AND upperProgNonDegree.EFF_TRM_D <> ''
														                                            AND SUBSTRING(upperProgNonDegree.PGM_CD, 2, 2) = '00'
                                                GROUP BY
	                                                u.DE1021
	                                                ,[1],[2],[3],[4],[5],[6],[9],[A],[B],[C],[D],[F],[G],[H],[I],[P],[T],[Z]
	                                                ,u.Hours
                                                ORDER BY
	                                                u.DE1021", conn);
                
                    reader = comm.ExecuteReader();

                    while (reader.Read())
                    {
                        String studentID = reader["DE1021"].ToString();
                        String program = reader["Program"].ToString();

                        int hours = int.Parse(reader["Hours"].ToString());

                        if (!inStateProgramHours.ContainsKey(program))
                        {
                            inStateProgramHours.Add(program, 0);
                            outOfStateProgramHours.Add(program, 0);
                            programs.Add(program);
                        }

                        if (inStateStudents.Contains(studentID))
                        {
                            inStateProgramHours[program] += hours;
                        }
                        else
                        {
                            outOfStateProgramHours[program] += hours;
                        }
                    }

                    reader.Close();
                }
                
                foreach (String program in programs)
                {
                    String prog = program == "" ? "No Program" : program;

                    output.WriteLine(String.Join(",", prog, inStateProgramHours[program], outOfStateProgramHours[program]));
                }

                for (int i = 0; i < 3; i++)
                {
                    output.WriteLine();
                }
            }
            
            output.Close();

            conn.Close();
        }
    }
}
