# Relationship Between Rainfall and Hydrometeorological Disasters in Indonesia

This repository contains Python script used to analyze the relationship between antecedent rainfall conditions and hydrometeorological disasters in Indonesia, particularly flood and landslide events at regency/city level.

The analysis utilizes:
- CHIRPS daily satellite rainfall data,
- BNPB disaster event records (2021–2025),
- and Indonesian administrative boundary data from BIG.

The project aims to identify regencies and cities that experienced flood and landslide events despite relatively low effective antecedent rainfall (EAR) conditions.

---

# Methodology

CHIRPS daily rainfall data and disaster event records were processed using Python and geospatial analysis libraries. Effective antecedent rainfall (EAR) accumulation was calculated for each disaster event based on existing EAR formulations, then aggregated using zonal statistics at regency/city level to obtain mean 3-, 5-, and 7-day EAR values prior to flood and landslide occurrences.

## Processing Steps

1. Collected:
   - flood and landslide event records from BNPB (2021–2025),
   - Indonesian regency/city boundary data from BIG,
   - and CHIRPS daily rainfall datasets (2020–2025) in NetCDF format.

2. Joined administrative boundary geometries to disaster event records using GeoPandas.

3. Extracted daily precipitation values from CHIRPS NetCDF datasets using Xarray.

4. Calculated mean daily precipitation values for each disaster event using zonal statistics based on associated regency/city geometries.

5. Computed:
   - 3-day EAR,
   - 5-day EAR,
   - and 7-day EAR accumulation prior to each disaster event.

6. Averaged accumulated rainfall values for each regency/city based on recorded disaster events.

7. Generated thematic maps and comparative graphs using Matplotlib.

---

# Software and Libraries

- Python
- Pandas
- GeoPandas
- Xarray
- Rasterstats
- Matplotlib
- Shapely

---

# Repository Structure

```text
├── data/
│   ├── rainfall/
│   ├── shapefile/
│   └── disaster dataset
├── output/
│   ├── EAR results
│   ├── thematic maps
│   └── graphs
├── main.py
├── requirements.txt
└── README.md
```

---

# Outputs

The processing workflow produces:
- EAR values for each disaster event,
- average EAR values per regency/city,
- thematic rainfall maps,
- and comparative graphs of regions with the lowest EAR conditions.

---

# References

1. Badan Nasional Penanggulangan Bencana (BNPB) [National Disaster Management Agency of Indonesia].  
   *Data Informasi Bencana Indonesia (DIBI).*  
   Accessed February 2026.  
   https://dibi.bnpb.go.id/superset/dashboard/2/

2. Badan Informasi Geospasial (BIG) [Geospatial Information Agency of Indonesia].  
   *Data Batas Wilayah.*  
   Accessed February 2026.  
   https://tanahair.indonesia.go.id/portal-web/unduh

3. Climate Hazards Center (CHIRPS), University of California, Santa Barbara.  
   *Daily Rainfall Dataset.*  
   Accessed February 2026.  
   https://data.chc.ucsb.edu/products/CHIRPS-2.0/

4. Chikalamo, E. E., Mavrouli, O. C., Ettema, J., van Westen, C. J., Muntohar, A. S., & Mustofa, A. (2020).  
   *Satellite-derived rainfall thresholds for landslide early warning in Bogowonto Catchment, Central Java, Indonesia.*  
   International Journal of Applied Earth Observation and Geoinformation, 89, 102093.  
   https://doi.org/10.1016/j.jag.2020.102093

5. Adji, T. N., & Misqi, M. (2010).  
   *The distribution of flood hydrograph recession constant for characterization of karst spring and underground river flow components releasing within Gunung Sewu Karst Region.*  
   Indonesian Journal of Geography, XLII(1).