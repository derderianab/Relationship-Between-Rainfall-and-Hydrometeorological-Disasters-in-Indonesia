import pandas as pd
import geopandas as gpd
import fiona
import xarray as xr
import rioxarray
from rasterstats import zonal_stats
import matplotlib.pyplot as plt
from shapely import box


######################### Functions #########################
def flip_raster_orientation(precip):
    if precip.latitude.values[0] < precip.latitude.values[-1]:
        flipped_raster = precip.sortby("latitude", ascending=False)
        return flipped_raster
    return precip


######################### Datasets #########################

### Regency Boundaries (Shapefile)
regency_boundaries = r"data/shapefile/regency_boundaries.shp"

### CHIRPS datasets (rainfall)
rainfall_p25_2025 = r"data/rainfall/chirps-v2.0.2025.days_p25.nc"
rainfall_p25_2024 = r"data/rainfall/chirps-v2.0.2024.days_p25.nc"
rainfall_p25_2023 = r"data/rainfall/chirps-v2.0.2023.days_p25.nc"
rainfall_p25_2022 = r"data/rainfall/chirps-v2.0.2022.days_p25.nc"
rainfall_p25_2021 = r"data/rainfall/chirps-v2.0.2021.days_p25.nc"
rainfall_p25_2020 = r"data/rainfall/chirps-v2.0.2020.days_p25.nc"


######################### Preprocess Data #########################

### Pre Process Regencies Data
regencies_gdf = gpd.read_file(regency_boundaries, columns=["KDPKAB", "NAMOBJ"])
regencies_gdf = regencies_gdf.to_crs("EPSG:4326")
filtered_gdf = regencies_gdf[regencies_gdf["KDPKAB"].notna()]
print(regencies_gdf.columns)

### Pre Process Rainfall Data
all_years = [2025, 2024, 2023, 2022, 2021, 2020]
years = [2020]
rainfall_files = {
    2025: rainfall_p25_2025,
    2024: rainfall_p25_2024,
    2023: rainfall_p25_2023,
    2022: rainfall_p25_2022,
    2021: rainfall_p25_2021,
    2020: rainfall_p25_2020
}

precip_data = {}
for year in years:
    ds = xr.open_dataset(rainfall_files[year])
    raw_precip = ds.precip
    precip = flip_raster_orientation(raw_precip)
    precip_data[year] = precip

all_records = []

for year in years:
    nc_data = precip_data[year]

    for i in nc_data.time.values:
        daily_precip = nc_data.sel(time=i)

        for j in range(len(filtered_gdf)):
            geom = filtered_gdf.iloc[j].geometry
            kode = filtered_gdf.iloc[j].KDPKAB
            nama = filtered_gdf.iloc[j].NAMOBJ

            stats = zonal_stats(
                geom,
                daily_precip.values,
                affine=daily_precip.rio.transform(),
                stats="mean",
                nodata=-9999,
                all_touched=True
            )
            print(i, kode, nama, stats[0]["mean"])

            #create a new DF
            all_records.append({
                "date": i,
                "KDPKAB": kode,
                "NAMOBJ": nama,
                "precip": stats[0]["mean"]
            })

            print(len(all_records))

df_control = pd.DataFrame(all_records)

### Save as csv
df_control.to_csv(f"output/df_control_{years[0]}.csv", index=False)

print(all_records[0])