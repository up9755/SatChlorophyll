@ECHO OFF
REM https://resources.marine.copernicus.eu/product-download/OCEANCOLOUR_MED_CHL_L4_REP_OBSERVATIONS_009_078
REM .
REM replace all instances of:
REM   <USERNAME> with your Copernicus marine username account
REM   <PASSWORD> with your Copernicus marine account password
REM .
REM Years from 2010 through 2020, split into full-year files, bounded by longitude and latitude
REM .
FOR /L %%i IN (2010,1,2020) DO (
  python -m motuclient --motu https://my.cmems-du.eu/motu-web/Motu --service-id OCEANCOLOUR_MED_CHL_L4_REP_OBSERVATIONS_009_078-TDS --product-id dataset-oc-med-chl-multi-l4-interp_1km_daily-rep --longitude-min 11.8 --longitude-max 19.7 --latitude-min 39.9 --latitude-max 45.9 --date-min "%%i-01-01 00:00:00" --date-max "%%i-12-31 23:59:59" --variable CHL --out-dir . --out-name "REP_L4_%%i.nc" --user <USERNAME> --pwd <PASSWORD>
  ECHO -------------------------------------
  ECHO Downloaded year %%i.
  ECHO -------------------------------------
  ECHO.
)

@ECHO Press enter to close...
PAUSE >NUL