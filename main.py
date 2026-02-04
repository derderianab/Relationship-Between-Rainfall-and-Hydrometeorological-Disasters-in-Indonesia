import pandas as pd
import geopandas as gpd
import fiona
import xarray as xr
import rioxarray
from rasterstats import zonal_stats


### Datasets
disaster_data = r"data/2021_2025_disaster.csv"
regency_boundaries = r"data/RBI50K_ADMINISTRASI_KABKOTA_20230907.gdb"
rainfall_p05_2025 = r"data/rainfall/chirps-v2.0.2025.days_p05.nc"
rainfall_p25_2025 = r"data/rainfall/chirps-v2.0.2025.days_p25.nc"
# Indonesia's Extend
Lat = [-12, 7]
Long = [94, 142]


##### Pre Process Disaster Data
disaster_df = pd.read_csv(disaster_data)
wet_hidromet_class = ["Banjir", "Cuaca ekstrem", "Longsor", "Gelombang pasang / Abrasi"]
wet_hidromet_df = disaster_df[disaster_df["Jenis Bencana"].isin(wet_hidromet_class)]
print(wet_hidromet_df['Tanggal / Waktu Kejadian'])


##### Pre Process Regencies Data
regencies = gpd.read_file(regency_boundaries, layer="ADMINISTRASI_AR_KABKOTA", columns=["KDPKAB", "NAMOBJ"])
regencies = regencies.to_crs("EPSG:4326")
test_regency = regencies.iloc[0]
test_polygon = test_regency.geometry
#print(test_polygon)


##### Pre Process Rainfall Data
ds = xr.open_dataset(rainfall_p25_2025)
precip = ds.precip

if precip.latitude.values[0] < precip.latitude.values[-1]:
    precip = precip.sortby("latitude", ascending=False)

print(precip.time)

### Overlay NC and Polygon
# results = []
# for t in range(len(precip.time)):
#     daily = precip.isel(time=t)
#
#     stats = zonal_stats(
#         test_polygon,
#         daily.values,
#         affine=daily.rio.transform(),
#         stats="mean",
#         nodata=-9999
#     )
#     print(stats)
#     results.append(stats)
# print(len(results))
