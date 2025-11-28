from dw import DW
import extract
import transform
import load
from itertools import tee # Clonar iteradors fonts de dades
import pandas as pd
import os

# Netejar cleaning.log si tornem a executar
if os.path.exists('cleaning.log'):
    with open('cleaning.log', 'w') as f:
        f.write('')

if __name__ == '__main__':
    dw = DW(create=True)
    APPLY_CLEANING = True # Netejar dades brutes
    print(f"{ 'SI' if APPLY_CLEANING else 'NO'} estem netejant dades")
 
    print("\n--- EXTRACCIÓ I CÀRREGA AIRCRAFT ---\n")
    # La carreguem primer per fer el cleaning de registres
    aircraft_info_source = extract.extract_aircraft_info_from_csv()
    transformed_aircraft = transform.transform_aircraft_dimension(aircraft_info_source)
    load.load_dimension(transformed_aircraft, dw.aircraft_dim)

    print("\n--- EXTRACCIÓ DE LES ALTRES FONTS DE DADES ---\n")
    personnel_source = extract.extract_personnel_info_from_csv()
    flights_source = extract.extract_flights_from_aims()
    maintenance_source = extract.extract_maintenance_from_aims()
    reports_source = extract.extract_reports_from_amos()

    # BR ValidAircraftRegistration
    flights_source = transform.clean_invalid_aircraft(flights_source, dw)
    maintenance_source = transform.clean_invalid_aircraft(maintenance_source, dw)
    reports_source = transform.clean_invalid_aircraft(reports_source, dw)

    print("\n--- TRANSFORMANT I CARREGANT DATE I MONTH ---\n")

    flights1, flights2 = tee(flights_source)
    maint1, maint2 = tee(maintenance_source)
    reports1, reports2 = tee(reports_source)

    date_data, month_data, flights_for_facts, maint_for_facts, reports_filtered = transform.transform_date_dimensions(flights1, maint1, reports1)

    load.load_dimension(date_data, dw.date_dim)
    load.load_dimension(month_data, dw.month_dim)

    print("\n--- TRANSFORMANT I CARREGANT FETS ---\n")

    # Clonem font reports
    reports_for_summary, reports_for_maint = tee(reports_filtered, 2)

    transformed_data = transform.transform_daily_utilization(flights_for_facts, apply_cleaning=APPLY_CLEANING)
    load.load_daily_utilization(dw, transformed_data)

    print("\n")

    transformed_data = transform.transform_monthly_summary(maint_for_facts, reports_for_summary)
    load.load_monthly_summary(dw, transformed_data)

    print("\n")

    transformed_data = transform.transform_monthly_maintenance_reports(reports_for_maint, personnel_source)
    load.load_monthly_maintenance_reports(dw, transformed_data)

    print("\nS'ha completat l'ETL")
    dw.close()