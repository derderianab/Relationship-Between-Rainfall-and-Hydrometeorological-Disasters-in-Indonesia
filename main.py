import pandas as pd
import geopandas as gpd
import fiona
import xarray as xr
import rioxarray
from rasterstats import zonal_stats


def flip_raster_orientation(precip):
    if precip.latitude.values[0] < precip.latitude.values[-1]:
        flipped_raster = precip.sortby("latitude", ascending=False)
        return flipped_raster
    return precip


### Datasets
disaster_data = r"data/2021_2025_disaster.csv"
test_data = r"data/test_data.csv"
regency_boundaries = r"data/shapefile/regency_boundaries.shp"
rainfall_p05_2025 = r"data/rainfall/chirps-v2.0.2025.days_p05.nc"
rainfall_p25_2025 = r"data/rainfall/chirps-v2.0.2025.days_p25.nc"
# Indonesia's Extend
Lat = [-12, 7]
Long = [94, 142]


##### Pre Process Disaster Data
disaster_df = pd.read_csv(test_data)
wet_hidromet_class = ["Banjir", "Longsor"]
hidromet_df = disaster_df[disaster_df["Jenis Bencana"].isin(wet_hidromet_class)].copy()
hidromet_df["Kode Kabupaten"] = hidromet_df["Kode Kabupaten"].astype(str)


##### Pre Process Regencies Data
regencies_gdf = gpd.read_file(regency_boundaries, columns=["KDPKAB", "NAMOBJ"])
regencies_gdf = regencies_gdf.to_crs("EPSG:4326")


##### Join Polygon to Disaster Data Frame
hidromet_df_with_geom = hidromet_df.merge(regencies_gdf[["KDPKAB", "geometry"]], left_on="Kode Kabupaten", right_on="KDPKAB", how="left")
hidromet_gdf=gpd.GeoDataFrame(hidromet_df_with_geom, geometry="geometry", crs=regencies_gdf.crs)
hidromet_gdf[["date", "time"]] = hidromet_gdf["Tanggal / Waktu Kejadian"].str.split(' ', expand=True)
hidromet_gdf["date"] = pd.to_datetime(hidromet_gdf["date"])
hidromet_gdf["precip"] = None
print(hidromet_gdf.columns)


##### Pre Process Rainfall Data
ds_2025 = xr.open_dataset(rainfall_p25_2025)
raw_precip_2025 = ds_2025.precip
precip_2025 = flip_raster_orientation(raw_precip_2025)


#### Add Daily Precipitation Value to Disaster Dataset
for i in range(len(hidromet_gdf)):
    if hidromet_gdf.iloc[i].Tahun == 2025:

        geom = hidromet_gdf.iloc[i].geometry
        if geom is None:
            hidromet_gdf.loc[hidromet_gdf.index[i], "precip"] = None
            continue
        daily = precip_2025.sel(time=hidromet_gdf.iloc[i].date)
        stats = zonal_stats(
            geom,
            daily.values,
            affine=daily.rio.transform(),
            stats="mean",
            nodata=-9999,
            all_touched=True
        )
        hidromet_gdf.loc[hidromet_gdf.index[i], "precip"] = stats[0]["mean"]

        print(stats)
        print(hidromet_gdf.iloc[i]["precip"])


### Calculate Precip Mean Value of Each Regency
grouped_gdf = hidromet_gdf.groupby(by="Kode Kabupaten")
kab_stats = (
    hidromet_gdf
    .groupby("KDPKAB")
    .agg(
        nama_kab=("Kabupaten", "first"),
        geometry=("geometry", "first"),
        mean_precip=("precip", "mean"),
        max_precip=("precip", "max"),
        min_precip=("precip", "min"),
        median_precip=("precip", "median"),
        jumlah_kejadian=("precip", "count")
    )
    .reset_index()
)

kab_stats = gpd.GeoDataFrame(kab_stats, geometry="geometry", crs=hidromet_gdf.crs)
print(kab_stats)

#Function parameters
##hidromet_gdf
##Tahun



#### Overlay NC and Polygon
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
