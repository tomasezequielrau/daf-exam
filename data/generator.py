"""This script generate a database of different weather station in Uruguay from the BigQuery public dataset."""

import pandas as pd
import sqlite3


QUERY = """
WITH weather_stations_gsod as (
        SELECT
          country as weather_station_country_code
        , case when country='UY' then 'Uruguay'
               when country='AR' then 'Argentina'
               when country='CI' then 'Chile'
               when country='CO' then 'Colombia'
               when country='PE' then 'Perú'
               when country='BL' then 'Bolivia'
               when country='PA' then 'Paraguay'
               when country='PM' then 'Panamá'
               when country='DR' then 'República Dominicana'
               else null 
          end as weather_station_country_name
        , name as weather_station_name
        , usaf as weather_station_id
        , wban
        FROM `bigquery-public-data.noaa_gsod.stations` b
        WHERE country in ('UY')
        and lon is not null and lat is not null
        )
, weather_prog_gsod as (
        SELECT stn as weather_station_id, * except(stn)
        FROM `bigquery-public-data.noaa_gsod.gsod2021`
        )
SELECT
  ws.*
, wp.* except(weather_station_id, wban)
FROM weather_stations_gsod ws
JOIN weather_prog_gsod wp ON wp.weather_station_id = ws.weather_station_id and wp.wban = ws.wban
ORDER BY weather_station_name, date
"""

if __name__ == "__main__":
    engine = sqlite3.connect('weather_station.db')
    sqlite_table = "weather_station"

    project_name = "YOUR-PROJECT-NAME"
    data = pd.read_gbq(QUERY, project_id=project_name)
    data.to_sql(sqlite_table, engine, if_exists='replace')
