import os
import sys
import duckdb # https://duckdb.org
import pygrametl # https://pygrametl.org
from pygrametl.tables import CachedDimension, FactTable


duckdb_filename = 'dw.duckdb'


class DW:
    def __init__(self, create=False):
        if create and os.path.exists(duckdb_filename):
            os.remove(duckdb_filename)
        try:
            self.conn_duckdb = duckdb.connect(duckdb_filename)
            print("Connection to the DW created successfully")
        except duckdb.Error as e:
            print(f"Unable to connect to DuckDB database '{duckdb_filename}':", e)
            sys.exit(1)

        if create:
            try:
                # TODO: Create the tables in the DW
                self.conn_duckdb.execute('''
                    CREATE TABLE Date (
                        DateKey INT PRIMARY KEY,
                        FullDate VARCHAR(10),
                        Day INT,
                        Month INT,
                        Year INT
                    );

                    CREATE TABLE Month (
                        MonthKey INT PRIMARY KEY,
                        Month INT,
                        Year INT
                    );

                    CREATE TABLE Aircraft (
                        AircraftKey INT PRIMARY KEY,
                        AircraftRegistrationCode VARCHAR(10),
                        AircraftModel VARCHAR(30),
                        AircraftManufacturer VARCHAR(30)
                    );

                    CREATE TABLE DailyUtilization (
                        DateKey INT,
                        AircraftKey INT,
                        FlightHours DECIMAL(10, 2),
                        FlightCycles INT,
                        NumberOfDelays INT,
                        NumberOfCancellations INT,
                        SumOfDelayDuration INT,
                        PRIMARY KEY (DateKey, AircraftKey),
                        FOREIGN KEY (DateKey) REFERENCES Date(DateKey),
                        FOREIGN KEY (AircraftKey) REFERENCES Aircraft(AircraftKey)
                    );

                    CREATE TABLE MonthlyAircraftSummary (
                        MonthKey INT,
                        AircraftKey INT,
                        ADIS DECIMAL(10, 2),
                        ADOSS DECIMAL(10, 2),
                        ADOSU DECIMAL(10, 2),
                        PilotReportCount INT,
                        PRIMARY KEY (MonthKey, AircraftKey),
                        FOREIGN KEY (MonthKey) REFERENCES Month(MonthKey),
                        FOREIGN KEY (AircraftKey) REFERENCES Aircraft(AircraftKey)
                    );

                    CREATE TABLE MonthlyMaintenanceReports (
                        MonthKey INT,
                        AircraftKey INT,
                        AirportCode VARCHAR(4),
                        MaintenanceReportCount INT,
                        PRIMARY KEY (MonthKey, AircraftKey, AirportCode),
                        FOREIGN KEY (MonthKey) REFERENCES Month(MonthKey),
                        FOREIGN KEY (AircraftKey) REFERENCES Aircraft(AircraftKey)
                    );
                    ''')
                print("[dw.py] S'han creat les taules correctament")
            except duckdb.Error as e:
                print("[dw.py] Error creant les taules:", e)
                sys.exit(2)

        # Link DuckDB and pygrametl
        self.conn_pygrametl = pygrametl.ConnectionWrapper(self.conn_duckdb)

        # ======================================================================================================= Dimension and fact table objects
        # TODO: Declare the dimensions and facts for pygrametl
        self.date_dim = CachedDimension(
            name='Date',
            key='DateKey',
            attributes=('FullDate', 'Day', 'Month', 'Year'),
            lookupatts=('DateKey',)
        )

        self.month_dim = CachedDimension(
            name='Month',
            key='MonthKey',
            attributes=('Month', 'Year'),
            lookupatts=('MonthKey',)
        )

        self.aircraft_dim = CachedDimension(
            name='Aircraft',
            key='AircraftKey',
            attributes=('AircraftRegistrationCode', 'AircraftModel', 'AircraftManufacturer'),
            lookupatts=('AircraftRegistrationCode',)
        )

        self.daily_utilization_fact = FactTable(
            name='DailyUtilization',
            keyrefs=('DateKey', 'AircraftKey'),
            measures=('FlightHours', 'FlightCycles', 'NumberOfDelays', 'NumberOfCancellations', 'SumOfDelayDuration')
        )

        self.monthly_summary_fact = FactTable(
            name='MonthlyAircraftSummary',
            keyrefs=('MonthKey', 'AircraftKey'),
            measures=('ADIS', 'ADOSS', 'ADOSU', 'PilotReportCount')
        )

        self.monthly_maintenance_reports_fact = FactTable(
            name='MonthlyMaintenanceReports',
            keyrefs=('MonthKey', 'AircraftKey', 'AirportCode'),
            measures=('MaintenanceReportCount',)
        )

    # TODO: Rewrite the queries exemplified in "extract.py"
    def query_utilization(self):
        result = self.conn_duckdb.execute("""
            WITH atomic_data AS (
                SELECT
                    a.AircraftManufacturer,
                    d.Year,
                    a.AircraftKey,
                    du.FlightHours,
                    du.FlightCycles,
                    du.NumberOfCancellations,
                    du.NumberOfDelays,
                    du.SumOfDelayDuration,
                    CAST(0 AS DECIMAL(10,2)) AS scheduledOutOfService,
                    CAST(0 AS DECIMAL(10,2)) AS unScheduledOutOfService
                FROM DailyUtilization du
                JOIN Date d ON du.DateKey = d.DateKey
                JOIN Aircraft a ON du.AircraftKey = a.AircraftKey
                
                UNION ALL

                SELECT
                    a.AircraftManufacturer,
                    m.Year,
                    a.AircraftKey,
                    0 AS FlightHours,
                    0 AS FlightCycles,
                    0 AS NumberOfCancellations,
                    0 AS NumberOfDelays,
                    0 AS SumOfDelayDuration,
                    ms.ADOSS AS scheduledOutOfService,
                    ms.ADOSU AS unScheduledOutOfService
                FROM MonthlyAircraftSummary ms
                JOIN Month m ON ms.MonthKey = m.MonthKey
                JOIN Aircraft a ON ms.AircraftKey = a.AircraftKey
            )
            SELECT
                a.AircraftManufacturer,
                a.Year,
                ROUND(SUM(a.FlightHours)/COUNT(DISTINCT a.AircraftKey), 2) AS FH,
                ROUND(SUM(a.FlightCycles)/COUNT(DISTINCT a.AircraftKey), 2) AS TakeOff,
                ROUND(SUM(a.scheduledOutOfService)/COUNT(DISTINCT a.AircraftKey), 2) AS ADOSS,
                ROUND(SUM(a.unScheduledOutOfService)/COUNT(DISTINCT a.AircraftKey), 2) AS ADOSU,
                ROUND((SUM(a.scheduledOutOfService)+SUM(a.unScheduledOutOfService))/COUNT(DISTINCT a.AircraftKey), 2) AS ADOS,
                365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unScheduledOutOfService))/COUNT(DISTINCT a.AircraftKey), 2) AS ADIS,
                ROUND( (ROUND(SUM(a.FlightHours)/COUNT(DISTINCT a.AircraftKey), 2)) / ((365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unScheduledOutOfService))/COUNT(DISTINCT a.AircraftKey), 2)) * 24), 2) AS DU,
                ROUND( (ROUND(SUM(a.FlightCycles)/COUNT(DISTINCT a.AircraftKey), 2)) / (365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unScheduledOutOfService))/COUNT(DISTINCT a.AircraftKey), 2)), 2) AS DC,
                100*ROUND(SUM(a.NumberOfDelays)/SUM(a.FlightCycles), 4) AS DYR,
                100*ROUND(SUM(a.NumberOfCancellations)/SUM(a.FlightCycles), 4) AS CNR,
                100-ROUND(100*(SUM(a.NumberOfDelays)+SUM(a.NumberOfCancellations))/SUM(a.FlightCycles), 2) AS TDR,
                100*ROUND(SUM(a.SumOfDelayDuration)/SUM(a.NumberOfDelays),2) AS ADD
            FROM atomic_data a
            GROUP BY a.AircraftManufacturer, a.Year
            ORDER BY a.AircraftManufacturer, a.Year;
            """).fetchall()
        return result

    def query_reporting(self):
        result = self.conn_duckdb.execute("""
            WITH 
                UtilizationData AS (
                    SELECT
                        d.Year,
                        a.AircraftManufacturer,
                        SUM(du.FlightHours) AS flightHours,
                        SUM(du.FlightCycles) AS flightCycles
                    FROM DailyUtilization du
                    JOIN Date d ON du.DateKey = d.DateKey
                    JOIN Aircraft a ON du.AircraftKey = a.AircraftKey
                    GROUP BY d.Year, a.AircraftManufacturer
                ),
                MaintReports AS (
                    SELECT
                        m.Year,
                        a.AircraftManufacturer,
                        SUM(mmr.MaintenanceReportCount) as MaintCount
                    FROM MonthlyMaintenanceReports mmr
                    JOIN Month m ON mmr.MonthKey = m.MonthKey
                    JOIN Aircraft a ON mmr.AircraftKey = a.AircraftKey
                    GROUP BY m.Year, a.AircraftManufacturer
                ),
                PilotReports AS (
                    SELECT
                        m.Year,
                        a.AircraftManufacturer,
                        SUM(ms.PilotReportCount) as PilotCount
                    FROM MonthlyAircraftSummary ms
                    JOIN Month m ON ms.MonthKey = m.MonthKey
                    JOIN Aircraft a ON ms.AircraftKey = a.AircraftKey
                    GROUP BY m.Year, a.AircraftManufacturer
                ),
                TotalReports AS (
                    SELECT 
                        p.Year,
                        p.AircraftManufacturer,
                        p.PilotCount + m.MaintCount AS TotalCounter
                    FROM PilotReports p
                    JOIN MaintReports m ON p.Year = m.Year AND p.AircraftManufacturer = m.AircraftManufacturer
                )
            SELECT 
                tr.AircraftManufacturer as manufacturer, 
                tr.Year as year,
                1000 * ROUND(CAST(tr.TotalCounter AS REAL) / u.flightHours, 3) AS RRh,
                100 * ROUND(CAST(tr.TotalCounter AS REAL) / u.flightCycles, 2) AS RRc               
            FROM TotalReports tr
            JOIN UtilizationData u ON tr.AircraftManufacturer = u.AircraftManufacturer AND tr.Year = u.Year
            ORDER BY tr.AircraftManufacturer, tr.Year;
            """).fetchall()
        return result

    def query_reporting_per_role(self):
        result = self.conn_duckdb.execute("""
            WITH 
                UtilizationData AS (
                    SELECT
                        d.Year,
                        a.AircraftManufacturer,
                        SUM(du.FlightHours) AS flightHours,
                        SUM(du.FlightCycles) AS flightCycles
                    FROM DailyUtilization du
                    JOIN Date d ON du.DateKey = d.DateKey
                    JOIN Aircraft a ON du.AircraftKey = a.AircraftKey
                    GROUP BY d.Year, a.AircraftManufacturer
                ),
                PilotReports AS (
                    SELECT
                        m.Year,
                        a.AircraftManufacturer,
                        'PIREP' as role,
                        SUM(ms.PilotReportCount) as counter
                    FROM MonthlyAircraftSummary ms
                    JOIN Month m ON ms.MonthKey = m.MonthKey
                    JOIN Aircraft a ON ms.AircraftKey = a.AircraftKey
                    GROUP BY m.Year, a.AircraftManufacturer
                ),
                MaintReports AS (
                    SELECT
                        m.Year,
                        a.AircraftManufacturer,
                        'MAREP' as role,
                        SUM(mmr.MaintenanceReportCount) as counter
                    FROM MonthlyMaintenanceReports mmr
                    JOIN Month m ON mmr.MonthKey = m.MonthKey
                    JOIN Aircraft a ON mmr.AircraftKey = a.AircraftKey
                    GROUP BY m.Year, a.AircraftManufacturer
                ),
                CombinedReports AS (
                    SELECT * FROM PilotReports
                    UNION ALL
                    SELECT * FROM MaintReports
                )
            SELECT 
                cr.AircraftManufacturer as manufacturer, 
                cr.Year as year, 
                cr.role,
                1000 * ROUND(CAST(cr.counter AS REAL) / u.flightHours, 3) AS RRh,
                100 * ROUND(CAST(cr.counter AS REAL) / u.flightCycles, 2) AS RRc              
            FROM CombinedReports cr
            JOIN UtilizationData u ON u.AircraftManufacturer = cr.AircraftManufacturer AND u.Year = cr.Year
            ORDER BY cr.AircraftManufacturer, cr.Year, cr.role;
            """).fetchall()
        return result

    def close(self):
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()