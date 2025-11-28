from tqdm import tqdm

def load_dimension(transformed_data_source, dimension_object):
    """
    Carregar dades a qualsevol dimensió
    """
    for row in transformed_data_source:
        dimension_object.ensure(row)

def load_daily_utilization(dw, transformed_data_source):
    """
    Carrega les dades transformades a la taula de fets DailyUtilization
    """
    for row in transformed_data_source:
        # Buscar DateKey
        date_key_int = int(f"{row['date'].year}{str(row['date'].month).zfill(2)}{str(row['date'].day).zfill(2)}") # YYYYMMDD
        date_key = dw.date_dim.lookup({'DateKey': date_key_int})
        
        # Buscar AircraftKey
        aircraft_key = dw.aircraft_dim.lookup({'AircraftRegistrationCode': row['aircraftregistration']})

        fact_row = {
            'DateKey': date_key,
            'AircraftKey': aircraft_key,
            'FlightHours': row['FlightHours'],
            'FlightCycles': row['FlightCycles'],
            'NumberOfDelays': row['NumberOfDelays'],
            'NumberOfCancellations': row['NumberOfCancellations'],
            'SumOfDelayDuration': row['SumOfDelayDuration']
        }
        dw.daily_utilization_fact.insert(fact_row)

def load_monthly_summary(dw, transformed_data_source):
    """
    Carrega les dades transformades a la taula de fets MonthlyAircraftSummary
    """
    for row in transformed_data_source:
        # Buscar MonthKey
        month_key = dw.month_dim.lookup({'MonthKey': int(row['MonthKey'])})
        
        # Buscar AircraftKey
        aircraft_key = dw.aircraft_dim.lookup({'AircraftRegistrationCode': row['aircraftregistration']})

        fact_row = {
            'MonthKey': month_key,
            'AircraftKey': aircraft_key,
            'ADIS': row['ADIS'],
            'ADOSS': row['ADOSS'],
            'ADOSU': row['ADOSU'],
            'PilotReportCount': row['PilotReportCount']
        }
        dw.monthly_summary_fact.insert(fact_row)

def load_monthly_maintenance_reports(dw, transformed_data_source):
    """
    Carrega les dades transformades a la taula MonthlyMaintenanceReports
    """
    for row in transformed_data_source:
        # Buscar MonthKey
        month_key = dw.month_dim.lookup({'MonthKey': int(row['MonthKey'])})
        
        # Buscar AircraftKey
        aircraft_key = dw.aircraft_dim.lookup({'AircraftRegistrationCode': row['aircraftregistration']})
        
        fact_row = {
            'MonthKey': month_key,
            'AircraftKey': aircraft_key,
            'AirportCode': row['AirportCode'],
            'MaintenanceReportCount': row['MaintenanceReportCount']
        }
        dw.monthly_maintenance_reports_fact.insert(fact_row)