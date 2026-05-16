import pandas as pd
import geopandas as gpd
import xarray as xr
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
### Disaster records
disaster_data = r"data/disaster dataset/2021_2025_disaster.csv"

### Regency boundaries shapefile
regency_boundaries = r"data/shapefile/regency_boundaries.shp"

### CHIRPS datasets (rainfall)
rainfall_p25_2025 = r"data/rainfall/chirps-v2.0.2025.days_p25.nc"
rainfall_p25_2024 = r"data/rainfall/chirps-v2.0.2024.days_p25.nc"
rainfall_p25_2023 = r"data/rainfall/chirps-v2.0.2023.days_p25.nc"
rainfall_p25_2022 = r"data/rainfall/chirps-v2.0.2022.days_p25.nc"
rainfall_p25_2021 = r"data/rainfall/chirps-v2.0.2021.days_p25.nc"
rainfall_p25_2020 = r"data/rainfall/chirps-v2.0.2020.days_p25.nc"

### Indonesia's Extend
Lat = [-12, 7]
Long = [94, 142]


######################### Preprocess Data #########################

### Pre Process Disaster Data
disaster_df = pd.read_csv(disaster_data, dtype={"Kode Kabupaten": str})
wet_hidromet_class = ["Banjir", "Longsor"]
hidromet_df = disaster_df[disaster_df["Jenis Bencana"].isin(wet_hidromet_class)].copy()

### Pre Process Regencies Data
regencies_gdf = gpd.read_file(regency_boundaries, columns=["KDPKAB", "NAMOBJ"])
regencies_gdf = regencies_gdf.to_crs("EPSG:4326")

### Pre Process Rainfall Data
years = [2025, 2024, 2023, 2022, 2021, 2020]
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


######################### Processing #########################

##### Join Polygon to Disaster Data Frame
hidromet_df_with_geom = hidromet_df.merge(regencies_gdf[["KDPKAB", "geometry"]],
                                          left_on="Kode Kabupaten",
                                          right_on="KDPKAB",
                                          how="left")
hidromet_gdf=gpd.GeoDataFrame(hidromet_df_with_geom,
                              geometry="geometry",
                              crs=regencies_gdf.crs)
hidromet_gdf[["date", "time"]] = hidromet_gdf["Tanggal / Waktu Kejadian"].str.split(' ', expand=True)
hidromet_gdf["date"] = pd.to_datetime(hidromet_gdf["date"])
for i in range(0,8):
    name = f"precip_{i}"
    hidromet_gdf[name] = None
print(hidromet_gdf.columns)

##### Add Daily Precipitation Value to Disaster Dataset

for i in range(len(hidromet_gdf)):
    tahun = hidromet_gdf.iloc[i].Tahun
    tanggal = hidromet_gdf.iloc[i].date
    geom = hidromet_gdf.iloc[i].geometry

    if geom is None:
        continue

    time_delta = [0,1,2,3,4,5,6,7]

    for j in time_delta:
        date_prev = tanggal - pd.Timedelta(days=j)
        prev_precip = precip_data[date_prev.year].sel(time=date_prev)

        stats = zonal_stats(
            geom,
            prev_precip.values,
            affine=prev_precip.rio.transform(),
            stats="mean",
            nodata=-9999,
            all_touched=True
        )
        hidromet_gdf.loc[hidromet_gdf.index[i], f"precip_{j}"] = stats[0]["mean"]
        print("id: ", hidromet_gdf.iloc[i].id, "date: ", date_prev, f"precip_{j} stats: ", stats)
        print(hidromet_gdf.iloc[i][f"precip_{j}"])

precip_per_disaster = hidromet_gdf.drop(columns="geometry")

##### Calculate Effective Antecedent Rainfall Accumulation
K = 0.9

### 3-Day Effective Antecedent Rainfall
precip_per_disaster["EAR_D3"] = (
    (K**1 * precip_per_disaster["precip_1"].astype(float)) +
    (K**2 * precip_per_disaster["precip_2"].astype(float)) +
    (K**3 * precip_per_disaster["precip_3"].astype(float))
)

### 5-Day Effective Antecedent Rainfall
precip_per_disaster["EAR_D5"] = (
    (K**1 * precip_per_disaster["precip_1"].astype(float)) +
    (K**2 * precip_per_disaster["precip_2"].astype(float)) +
    (K**3 * precip_per_disaster["precip_3"].astype(float)) +
    (K**4 * precip_per_disaster["precip_4"].astype(float)) +
    (K**5 * precip_per_disaster["precip_5"].astype(float))
)

### 7-Day Effective Antecedent Rainfall
precip_per_disaster["EAR_D7"] = (
    (K**1 * precip_per_disaster["precip_1"].astype(float)) +
    (K**2 * precip_per_disaster["precip_2"].astype(float)) +
    (K**3 * precip_per_disaster["precip_3"].astype(float)) +
    (K**4 * precip_per_disaster["precip_4"].astype(float)) +
    (K**5 * precip_per_disaster["precip_5"].astype(float)) +
    (K**6 * precip_per_disaster["precip_6"].astype(float)) +
    (K**7 * precip_per_disaster["precip_7"].astype(float))
)

print(
    precip_per_disaster[
        ["id", "EAR_D3", "EAR_D5", "EAR_D7"]
    ].head()
)

### Save as excel
precip_per_disaster.to_excel("output/3_5_7_days_EAR_per_disaster.xlsx")


##### Compute Average EAR per Regency/City
ear_average_per_region = (
    precip_per_disaster
    .groupby(["Kode Kabupaten", "Kabupaten"])[
        ["EAR_D3", "EAR_D5", "EAR_D7"]
    ]
    .mean()
    .reset_index()
)

ear_average_per_region = ear_average_per_region.rename(columns={
    "EAR_D3": "AVG_EAR_D3",
    "EAR_D5": "AVG_EAR_D5",
    "EAR_D7": "AVG_EAR_D7"
})

print(ear_average_per_region.head())

### Save as Excel
ear_average_per_region.to_excel(
    "output/average_EAR_per_region.xlsx",
    index=False
)

######################### Create Thematic Maps #########################

##### Join EAR to Regency Boundary
ear_map_gdf = regencies_gdf.merge(
    ear_average_per_region,
    left_on="KDPKAB",
    right_on="Kode Kabupaten",
    how="left"
)

map_configs = [
    ("AVG_EAR_D3", "3-Day Effective Antecedent Rainfall"),
    ("AVG_EAR_D5", "5-Day Effective Antecedent Rainfall"),
    ("AVG_EAR_D7", "7-Day Effective Antecedent Rainfall")
]

##### Create Background Box
box_bg = gpd.GeoDataFrame(
    geometry=[box(Long[0], Lat[0], Long[1], Lat[1])],
    crs="EPSG:4326"
)

for column, title in map_configs:

    fig, ax = plt.subplots(figsize=(12,7))

    # Background
    box_bg.plot(ax=ax, color="lightblue", zorder=0)

    # Thematic Layer
    ear_map_gdf.plot(
        column=column,
        cmap="Reds_r",
        scheme="quantiles",
        k=5,
        legend=True,
        legend_kwds={
            "fmt": "{:.1f} mm",
            "title": "Effective Antecedent Rainfall",
            "loc": "upper right",
            "reverse": True
        },
        ax=ax,
        edgecolor="grey",
        linewidth=0.3,
        zorder=1,
        missing_kwds={
            "color": "lightgrey",
            "label": "Missing values",
        },
    )

    ax.set_title(
        f"{title} During Flood and Landslide Events (2021–2025)"
    )

    ax.axis("off")

    plt.tight_layout()

    output_name = column.lower() + "_thematic_map.png"

    fig.savefig(
        f"output/{output_name}",
        dpi=300
    )

######################### Create Top 10 Lowest WAR Graphs #########################

graph_configs = [
    ("AVG_EAR_D3", "Top 10 Regencies/Cities with the Lowest 3-Day EAR"),
    ("AVG_EAR_D5", "Top 10 Regencies/Cities with the Lowest 5-Day EAR"),
    ("AVG_EAR_D7", "Top 10 Regencies/Cities with the Lowest 7-Day EAR")
]

for column, title in graph_configs:

    top_reg = (
        ear_average_per_region
        .sort_values(column)
        .head(10)
    )

    fig, ax = plt.subplots(figsize=(12,7))

    ax.bar(
        top_reg["Kabupaten"],
        top_reg[column]
    )

    ax.set_title(title)

    ax.set_ylabel(
        "Effective Antecedent Rainfall (mm)"
    )

    ax.tick_params(
        axis="x",
        rotation=45
    )

    plt.tight_layout()

    output_name = column.lower() + "_top10_graph.png"

    fig.savefig(
        f"output/{output_name}",
        dpi=300
    )

plt.show()

