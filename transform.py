from tqdm import tqdm
import logging
import pandas as pd
import datetime 
from itertools import tee # Clonar iteradors fonts de dades

# Configure logging
logging.basicConfig(
    filename='cleaning.log', # Log file name
    level=logging.INFO, # Logging level
    format='%(message)s' # Log message format
)

def build_dateCode(date) -> str:
    return f"{date.year}-{date.month}-{date.day}"

# BR ValidAircraftRegistration
def clean_invalid_aircraft(data_source, dw):
    """
    Filtra els aircraftregistration invàlids (lookup a Aircraft)
    """
    for row in data_source:
        aircraft_code = row.get('aircraftregistration')
        if aircraft_code and dw.aircraft_dim.lookup({'AircraftRegistrationCode': aircraft_code}):
            yield row
        else:
            logging.info(f"S'ignora el registre per codi d'aeronau incorrecte: {row}")

# TODO: Implement here all transforming functions

def transform_aircraft_dimension(aircraft_info_source):
    """
    Ajusta les dades d'Aircraft a l'estructura de la dimensió Aircraft
    """
    source_list = list(aircraft_info_source)

    for row in tqdm(source_list, desc="Transformant i Carregant Dimensió Aircraft"):
        # Justificació: per evitar carregar tot a memoria
        yield {
            'AircraftRegistrationCode': row['aircraft_reg_code'],
            'AircraftModel': row['aircraft_model'],
            'AircraftManufacturer': row['aircraft_manufacturer']
        }
    

def transform_date_dimensions(flights_source, maintenance_source, reports_source):
    """
    Obtenir dades de Date i Month a partir de les dates d'AIMS i AMOS
    Filtrem les dades fora de rang d'AMOS
    """
    flights1, flights2 = tee(flights_source)
    maint1, maint2 = tee(maintenance_source)
    
    aims_dates = set()
    min_date = datetime.date.max
    max_date = datetime.date.min

    # Extreure dates d'AIMS
    for source, date_col in [(flights1, 'scheduleddeparture'), (maint1, 'scheduleddeparture')]:
        for row in source:
            current_date_obj = pd.to_datetime(row[date_col]).date()
            aims_dates.add(current_date_obj)
            if current_date_obj < min_date:
                min_date = current_date_obj
            if current_date_obj > max_date:
                max_date = current_date_obj

    # Filtrar AMOS (dates que surten del rang d'AIMS p. ex 2100)
    df_reports = pd.DataFrame(list(reports_source))
    if not df_reports.empty:
        df_reports['reportingdate'] = pd.to_datetime(df_reports['reportingdate']).dt.date
        df_reports_filtered = df_reports[
            (df_reports['reportingdate'] >= min_date) & (df_reports['reportingdate'] <= max_date)
        ]
        amos_dates = set(df_reports_filtered['reportingdate'])
    else:
        df_reports_filtered = pd.DataFrame(columns=df_reports.columns)
        amos_dates = set()

    all_unique_dates = sorted(list(aims_dates.union(amos_dates)))

    date_data = []
    for current_date in tqdm(all_unique_dates, desc="Transformant i Carregant Dimensions Date i Month"):
        date_data.append({
            'DateKey': int(f"{current_date.year}{str(current_date.month).zfill(2)}{str(current_date.day).zfill(2)}"), # YYYYMMDD
            'FullDate': build_dateCode(current_date),
            'Day': current_date.day,
            'Month': current_date.month,
            'Year': current_date.year
        })

    month_data = []
    seen_months = set()
    for date_row in date_data:
        month_key = date_row['Year'] * 100 + date_row['Month'] # YYYYMM
        if month_key not in seen_months:
            month_data.append({
                'MonthKey': month_key,
                'Month': date_row['Month'],
                'Year': date_row['Year']
            })
            seen_months.add(month_key)

    reports_filtered_iter = iter(df_reports_filtered.to_dict('records'))
    # Apart de dates, retorna iteradors de vols, manteniment i informes filtrats
    return date_data, month_data, flights2, maint2, reports_filtered_iter

def transform_daily_utilization(flights_source, apply_cleaning=False):
    """
    Transforma les dades de vols en format diari per aeronau
    apply_cleaning: si és True, s'apliquen les BR-21 i BR-23
    """
    df = pd.DataFrame(flights_source)

    for col in ['scheduleddeparture', 'scheduledarrival', 'actualdeparture', 'actualarrival']:

        df[col] = pd.to_datetime(df[col])

    # Business Rules
    if apply_cleaning:

        # BR-23
        inverted_dates_mask = df['actualarrival'] < df['actualdeparture']
        df.loc[inverted_dates_mask, ['actualdeparture', 'actualarrival']] = \
            df.loc[inverted_dates_mask, ['actualarrival', 'actualdeparture']].values # Intercanviem valors

        # BR-21
        df.sort_values(by=['aircraftregistration', 'actualdeparture'], inplace=True)
        """
        Màscara booleana amb condicions:
        (1) Vol actual i el següent són de la mateixa aeronau
        (2) Vols no cancel·lats
        (3) L'arribada real del vol actual és posterior a la sortida real del següent vol
        """
        while True:
            next_ac = df['aircraftregistration'].shift(-1) # Comparar actual amb la següent
            next_actualdep = df['actualdeparture'].shift(-1)
            next_cancelled = df['cancelled'].astype('boolean').shift(-1) # Mirar si el següent cancel·lat

            overlaps_mask = ( # Vol actual no cancel·lat, seguënt tampoc i arribada actual > sortida següent
                (df['aircraftregistration'] == next_ac) & (~df['cancelled']) & (~next_cancelled) & (df['actualarrival'] > next_actualdep)
            )

            if not overlaps_mask.any():
                break # Cap solapament

            # Log files eliminades
            first_overlap = df[overlaps_mask].index[0]
            logging.info(f"Violacio de BR-21: Vols solapats per l'aeronau {df.loc[first_overlap, 'aircraftregistration']}. S'ignora el vol: {df.loc[first_overlap].to_dict()}")

            df.drop(first_overlap, inplace=True)
            df.reset_index(drop=True, inplace=True)

            df.sort_values(by=['aircraftregistration', 'actualdeparture'], inplace=True)
        
        print(f"BR-21, BR-23 i BR-ValidAircraftRegistration aplicades correctament")
    
    df['date'] = df['scheduleddeparture'].dt.date

    # Càlcul de mètriques
    df['FlightCycles'] = 1
    df.loc[df['cancelled'], 'FlightCycles'] = 0 # Cancelat no compta
    df['NumberOfCancellations'] = df['cancelled'].astype(int)

    df['FlightHours'] = (df['actualarrival'] - df['actualdeparture']).dt.total_seconds() / 3600 # Duració real vol en hores
    df.loc[df['cancelled'], 'FlightHours'] = 0 

    # 15 minuts o més, atrasat
    is_delayed = ((df['actualarrival'] - df['scheduledarrival']).dt.total_seconds() / 60) > 15 
    df['NumberOfDelays'] = (is_delayed & ~df['cancelled']).astype(int) # Cancelat no compta

    df['SumOfDelayDuration'] = (df['actualarrival'] - df['scheduledarrival']).dt.total_seconds() / 60 # Retràs en minuts

    df.loc[~is_delayed | df['cancelled'], 'SumOfDelayDuration'] = 0

    # Agregació diaria per aeronau
    daily_summary = df.groupby(['date', 'aircraftregistration']).agg(
        FlightHours=('FlightHours', 'sum'),
        FlightCycles=('FlightCycles', 'sum'),
        NumberOfDelays=('NumberOfDelays', 'sum'),
        NumberOfCancellations=('NumberOfCancellations', 'sum'),
        SumOfDelayDuration=('SumOfDelayDuration', 'sum')
    ).reset_index()

    records = daily_summary.to_dict('records')

    for row in tqdm(records, desc="Transformant i Carregant DailyUtilization"):
        # Justificació: per evitar carregar tot a memoria
        yield row

def transform_monthly_summary(maintenance_source, reports_source):
    """
    Transformar dades de manteniment i reports de pilots per agrupar mensualment per aeronau
    """
    df_maint = pd.DataFrame(maintenance_source)
    df_reports = pd.DataFrame(reports_source)
    
    df_maint['scheduleddeparture'] = pd.to_datetime(df_maint['scheduleddeparture'])
    df_maint['scheduledarrival'] = pd.to_datetime(df_maint['scheduledarrival'])

    df_maint['month_key'] = df_maint['scheduleddeparture'].dt.year * 100 + df_maint['scheduleddeparture'].dt.month # YYYYMM
    duration_days = (df_maint['scheduledarrival'] - df_maint['scheduleddeparture']).dt.total_seconds() / (24 * 3600) # Duració en dies

    df_maint['ADOSS'] = duration_days.where(df_maint['programmed'], 0) # Veure si és programat per ADOSS/ADOSU
    df_maint['ADOSU'] = duration_days.where(~df_maint['programmed'], 0)
    
    maint_summary = df_maint.groupby(['month_key', 'aircraftregistration']).agg(
        ADOSS=('ADOSS', 'sum'), ADOSU=('ADOSU', 'sum')
    ).reset_index()
    
    maint_summary['ADIS'] = 30.44 - (maint_summary['ADOSS'] + maint_summary['ADOSU']) # Dies mitjans per mes = 365.25 / 12 aprox 30.44
    
    df_reports['reportingdate'] = pd.to_datetime(df_reports['reportingdate'])
    
    df_pilot_reports = df_reports[df_reports['reporteurclass'] == 'PIREP'].copy() # Només pilots
    
    df_pilot_reports['month_key'] = df_pilot_reports['reportingdate'].dt.year * 100 + df_pilot_reports['reportingdate'].dt.month # YYYYMM

    pilot_summary = df_pilot_reports.groupby(['month_key', 'aircraftregistration']).agg(
        PilotReportCount=('reporteurclass', 'count')
    ).reset_index()

    # Garantir que estiguin tots els mesos i totes les aeronaus encara que falti informació en alguna de les dues fonts
    final_summary = pd.merge(maint_summary, pilot_summary, on=['month_key', 'aircraftregistration'], how='outer')
    final_summary.fillna(0, inplace=True)

    final_summary.rename(columns={'month_key': 'MonthKey'}, inplace=True)

    records = final_summary.to_dict('records')

    for row in tqdm(records, desc="Transformant i Carregant MonthlyAircraftSummary"):
        # Justificació: per evitar carregar tot a memoria
        yield row

def transform_monthly_maintenance_reports(reports_source, personnel_source):
    """
    Transforma els reports de manteniment, afegeix informació del aeroport del personal i els agrega per mes, aeronau i aeroport
    """
    
    df_reports = pd.DataFrame(reports_source)
    df_personnel = pd.DataFrame(personnel_source)
    
    # Preparar JOIN
    df_reports['reporteurid'] = pd.to_numeric(df_reports['reporteurid'])
    df_personnel['reporteurid'] = pd.to_numeric(df_personnel['reporteurid'])

    df_maint_reports = df_reports[df_reports['reporteurclass'] == 'MAREP'].copy() # Només manteniment

    merged_data = pd.merge(df_maint_reports, df_personnel, on='reporteurid', how='inner')

    merged_data['reportingdate'] = pd.to_datetime(merged_data['reportingdate'])
    merged_data['month_key'] = merged_data['reportingdate'].dt.year * 100 + merged_data['reportingdate'].dt.month

    maint_airport_summary = merged_data.groupby(['month_key', 'aircraftregistration', 'airport']).agg(
        MaintenanceReportCount=('reporteurclass', 'count')
    ).reset_index()

    maint_airport_summary.rename(columns={'airport': 'AirportCode', 'month_key': 'MonthKey'}, inplace=True)
    
    records = maint_airport_summary.to_dict('records')

    for row in tqdm(records, desc="Transformant i Carregant MonthlyMaintenanceReports"):
        # Justificació: per evitar carregar tot a memoria
        yield row