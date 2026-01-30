# Extra Credit: Geocode Georgia employer addresses and plot on a map

import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from shapely.geometry import Point
import os
import time

# Load jobs data
jobs = pd.read_csv("pset3_inputdata/jobs.csv")

# Filter to Georgia employers only
jobs_ga = jobs[jobs["EMPLOYER_STATE"] == "GA"].copy()
print(f"Number of Georgia employers: {len(jobs_ga)}")

# Standardize address components (vectorized for speed)
jobs_ga["EMPLOYER_ADDRESS_1"] = jobs_ga["EMPLOYER_ADDRESS_1"].fillna("").str.strip().str.title()
jobs_ga["EMPLOYER_CITY"] = jobs_ga["EMPLOYER_CITY"].fillna("").str.strip().str.title()

# Create full address for geocoding
jobs_ga["full_address"] = (
    jobs_ga["EMPLOYER_ADDRESS_1"].fillna("") + ", " +
    jobs_ga["EMPLOYER_CITY"].fillna("") + ", " +
    jobs_ga["EMPLOYER_STATE"].fillna("") + " " +
    jobs_ga["EMPLOYER_POSTAL_CODE"].fillna("").astype(str)
)

# Create simpler address (city, state, zip) as fallback
jobs_ga["simple_address"] = (
    jobs_ga["EMPLOYER_CITY"].fillna("") + ", " +
    jobs_ga["EMPLOYER_STATE"].fillna("") + " " +
    jobs_ga["EMPLOYER_POSTAL_CODE"].fillna("").astype(str)
)

# Get unique addresses to minimize geocoding calls
unique_addresses = jobs_ga[["full_address", "simple_address"]].drop_duplicates().reset_index(drop=True)
print(f"Unique addresses to geocode: {len(unique_addresses)}")

# Check if we have previously saved geocoded results
CACHE_FILE = "geocoded_addresses_cache.csv"

if os.path.exists(CACHE_FILE):
    print(f"Loading cached geocoded addresses from {CACHE_FILE}...")
    geocoded_df = pd.read_csv(CACHE_FILE)
    # Find addresses that still need geocoding
    already_geocoded = set(geocoded_df["full_address"].tolist())
    to_geocode = unique_addresses[~unique_addresses["full_address"].isin(already_geocoded)]
    geocoded_results = geocoded_df.to_dict("records")
    print(f"Already geocoded: {len(already_geocoded)}, remaining: {len(to_geocode)}")
else:
    to_geocode = unique_addresses
    geocoded_results = []
    print("No cache found, starting fresh...")

# Geocode remaining addresses
if len(to_geocode) > 0:
    geolocator = Nominatim(user_agent="qss20_pset3_geocoder", timeout=15)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.5, max_retries=3, error_wait_seconds=10)

    for idx, row in to_geocode.iterrows():
        address = row["full_address"]
        simple_addr = row["simple_address"]
        
        try:
            # Try full address first
            location = geocode(address)
            
            # If full address fails, try simpler address (only if different)
            if location is None and simple_addr != address:
                print(f"  Full address failed, trying simple address: {simple_addr}")
                location = geocode(simple_addr)
            
            if location:
                geocoded_results.append({
                    "full_address": address,
                    "latitude": location.latitude,
                    "longitude": location.longitude
                })
            else:
                geocoded_results.append({
                    "full_address": address,
                    "latitude": None,
                    "longitude": None
                })
        except Exception as e:
            print(f"Error geocoding '{address}': {e}")
            geocoded_results.append({
                "full_address": address,
                "latitude": None,
                "longitude": None
            })
            # Wait extra time after an error
            time.sleep(2)
        
        # Progress indicator
        completed = len(geocoded_results)
        if completed % 10 == 0:
            print(f"Geocoded {completed}/{len(unique_addresses)} addresses...")
            # Save intermediate results to cache
            pd.DataFrame(geocoded_results).to_csv(CACHE_FILE, index=False)

# Save final results to cache
geocoded_df = pd.DataFrame(geocoded_results)
geocoded_df.to_csv(CACHE_FILE, index=False)
print(f"\nSuccessfully geocoded: {geocoded_df['latitude'].notna().sum()} / {len(geocoded_df)} addresses")
print(f"Results saved to {CACHE_FILE}")

# Merge geocoded coordinates back to jobs_ga
jobs_ga_geo = jobs_ga.merge(geocoded_df, on="full_address", how="left")
jobs_ga_geo = jobs_ga_geo.dropna(subset=["latitude", "longitude"])
print(f"Jobs with valid coordinates: {len(jobs_ga_geo)}")

# Convert to GeoDataFrame
geometry = [Point(xy) for xy in zip(jobs_ga_geo["longitude"], jobs_ga_geo["latitude"])]
jobs_gdf = gpd.GeoDataFrame(jobs_ga_geo, geometry=geometry, crs="EPSG:4326")

# Download Georgia state boundary from US Census
usa = gpd.read_file("https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_state_500k.zip")
georgia = usa[usa["NAME"] == "Georgia"]

# Plotting
fig, ax = plt.subplots(figsize=(12, 10))
georgia.plot(ax=ax, color="lightgray", edgecolor="black", linewidth=1)
jobs_gdf.plot(ax=ax, color="red", markersize=30, alpha=0.6, marker="o", label="H-2A Employer Jobs")
ax.set_title("H-2A Guestworker Job Locations in Georgia", fontsize=14)
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.legend(loc="upper right")
plt.tight_layout()
plt.show()

print(f"\nPlotted {len(jobs_gdf)} employer job locations on Georgia map.")
