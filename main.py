import pandas as pd
import geopandas as gpd
import fiona
import xarray as xr
import rioxarray
from rasterstats import zonal_stats
import matplotlib.pyplot as plt
from shapely import box


def flip_raster_orientation(precip):
    if precip.latitude.values[0] < precip.latitude.values[-1]:
        flipped_raster = precip.sortby("latitude", ascending=False)
        return flipped_raster
    return precip


##### Datasets
disaster_data = r"data/2021_2025_disaster.csv"
test_data = r"data/test_data.csv"
regency_boundaries = r"data/shapefile/regency_boundaries.shp"
regency_boundaries_s0001 = r"data/shapefile/regency_boundaries_s0001.shp"

### CHIRPS datasets (rainfall)
rainfall_p05_2025 = r"data/rainfall/chirps-v2.0.2025.days_p05.nc"

rainfall_p25_2025 = r"data/rainfall/chirps-v2.0.2025.days_p25.nc"
rainfall_p25_2024 = r"data/rainfall/chirps-v2.0.2024.days_p25.nc"
rainfall_p25_2023 = r"data/rainfall/chirps-v2.0.2023.days_p25.nc"
rainfall_p25_2022 = r"data/rainfall/chirps-v2.0.2022.days_p25.nc"
rainfall_p25_2021 = r"data/rainfall/chirps-v2.0.2021.days_p25.nc"

### Indonesia's Extend
Lat = [-12, 7]
Long = [94, 142]


##### Pre Process Disaster Data
disaster_df = pd.read_csv(disaster_data, dtype={"Kode Kabupaten": str})
wet_hidromet_class = ["Banjir", "Longsor"]
hidromet_df = disaster_df[disaster_df["Jenis Bencana"].isin(wet_hidromet_class)].copy()


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
### Year 2025
ds_2025 = xr.open_dataset(rainfall_p25_2025)
raw_precip_2025 = ds_2025.precip
precip_2025 = flip_raster_orientation(raw_precip_2025)

### Year 2024
ds_2024 = xr.open_dataset(rainfall_p25_2024)
raw_precip_2024 = ds_2024.precip
precip_2024 = flip_raster_orientation(raw_precip_2024)

### Year 2023
ds_2023 = xr.open_dataset(rainfall_p25_2023)
raw_precip_2023 = ds_2023.precip
precip_2023 = flip_raster_orientation(raw_precip_2023)

### Year 2022
ds_2022 = xr.open_dataset(rainfall_p25_2022)
raw_precip_2022 = ds_2022.precip
precip_2022 = flip_raster_orientation(raw_precip_2022)

### Year 2021
ds_2021 = xr.open_dataset(rainfall_p25_2021)
raw_precip_2021 = ds_2021.precip
precip_2021 = flip_raster_orientation(raw_precip_2021)


##### Add Daily Precipitation Value to Disaster Dataset
for i in range(len(hidromet_gdf)):
    if hidromet_gdf.iloc[i].Tahun == 2025:
        geom = hidromet_gdf.iloc[i].geometry
        if geom is None:
            hidromet_gdf.loc[hidromet_gdf.index[i], "precip"] = None
            continue
        #daily = precip_2025.sel(time=hidromet_gdf.iloc[i].date)
        date_prev = hidromet_gdf.iloc[i].date - pd.Timedelta(days=1)
        daily = precip_2025.sel(time=date_prev)
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

    elif hidromet_gdf.iloc[i].Tahun == 2024:
        geom = hidromet_gdf.iloc[i].geometry
        if geom is None:
            hidromet_gdf.loc[hidromet_gdf.index[i], "precip"] = None
            continue
        #daily = precip_2024.sel(time=hidromet_gdf.iloc[i].date)
        date_prev = hidromet_gdf.iloc[i].date - pd.Timedelta(days=1)
        daily = precip_2024.sel(time=date_prev)
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

    elif hidromet_gdf.iloc[i].Tahun == 2023:
        geom = hidromet_gdf.iloc[i].geometry
        if geom is None:
            hidromet_gdf.loc[hidromet_gdf.index[i], "precip"] = None
            continue
        #daily = precip_2023.sel(time=hidromet_gdf.iloc[i].date)
        date_prev = hidromet_gdf.iloc[i].date - pd.Timedelta(days=1)
        daily = precip_2023.sel(time=date_prev)
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

    elif hidromet_gdf.iloc[i].Tahun == 2022:
        geom = hidromet_gdf.iloc[i].geometry
        if geom is None:
            hidromet_gdf.loc[hidromet_gdf.index[i], "precip"] = None
            continue
        #daily = precip_2022.sel(time=hidromet_gdf.iloc[i].date)
        date_prev = hidromet_gdf.iloc[i].date - pd.Timedelta(days=1)
        daily = precip_2022.sel(time=date_prev)
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

    elif hidromet_gdf.iloc[i].Tahun == 2021:
        geom = hidromet_gdf.iloc[i].geometry
        if geom is None:
            hidromet_gdf.loc[hidromet_gdf.index[i], "precip"] = None
            continue
        #daily = precip_2021.sel(time=hidromet_gdf.iloc[i].date)
        date_prev = hidromet_gdf.iloc[i].date - pd.Timedelta(days=1)
        daily = precip_2021.sel(time=date_prev)
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

### Save as excel
precip_per_disaster = hidromet_gdf.drop(columns="geometry")
precip_per_disaster.to_excel("output/precip_per_disaster.xlsx")

##### Calculate Precip Mean Value of Each Regency
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

kab_stats["mean_precip"] = kab_stats["mean_precip"].astype(float)
kab_stats["max_precip"] = kab_stats["max_precip"].astype(float)
kab_stats["min_precip"] = kab_stats["min_precip"].astype(float)
kab_stats["median_precip"] = kab_stats["median_precip"].astype(float)
kab_stats["jumlah_kejadian"] = kab_stats["jumlah_kejadian"].astype(int)

kab_stats = gpd.GeoDataFrame(kab_stats, geometry="geometry", crs=hidromet_gdf.crs)
kab_stats = kab_stats.drop(columns=["geometry"])
kab_stats = kab_stats.drop(columns=["nama_kab"])

print("kab_stats_column: ", kab_stats.columns)
print("regencies_gdf_columns: ", regencies_gdf.columns)

### Join Calculated Precip Mean Value to Regencies Datasets

result = pd.merge(
    left = regencies_gdf,
    right = kab_stats,
    left_on="KDPKAB",
    right_on="KDPKAB",
    how="left"
)
result = result.sort_values(by = "mean_precip", ascending=True)

print(result)
print(result.columns)
print(result.info())

### Save Regency Stats to excel
df_result = result.drop(columns="geometry")
df_result.to_excel("output/result.xlsx")


##### Visualisation
### Create Background
box_bg = gpd.GeoDataFrame(
    geometry=[box(Long[0], Lat[0], Long[1], Lat[1])],
    crs=hidromet_gdf.crs
)

### Print Thematic Map
fig1, ax1 = plt.subplots(figsize=(12,7))

# Background
box_bg.plot(ax=ax1, color="lightblue", zorder=0)

# Thematic Layer
result.plot(
    column="mean_precip",
    cmap="Reds_r",
    scheme="quantiles",
    k=5,
    legend=True,
    legend_kwds={
        "fmt": "{:.1f} mm",
        "title": "Mean Daily Precipitation",
        "loc": "upper right",
        "reverse": True
    },
    ax=ax1,
    edgecolor="grey",
    linewidth=0.3,
    zorder=1,
    missing_kwds={
        "color": "lightgrey",
        "label": "Missing values",
    },
)
ax1.set_title("Mean Daily Precipitation During Flood or Landslide Events (2021–2025)")
ax1.axis("off")
fig1.savefig("output/thematic_map.png", dpi=300)

### Print Bar Chart
top_reg = result.sort_values("mean_precip").head(20)

fig2, ax2 = plt.subplots(figsize=(12,7))
ax2.bar(
    top_reg["NAMOBJ"],
    top_reg["mean_precip"]
)
ax2.set_title("Top 20 Regencies with the Lowest Mean Precipitation During Flood or Landslide Events")
ax2.set_ylabel("Mean Precipitation")
ax2.tick_params(axis="x", rotation=45)
plt.tight_layout()
fig2.savefig("output/top20_regencies.png", dpi=300)

plt.show()

