import matplotlib.pyplot as plt
import pandas as pd


def load_coordinates(city: str, year: int, month: int) -> pd.DataFrame:
    """Load crime coordinates for a city/month."""
    df = (
        spark.table("crime_lakehouse.analysis.crime_coordinates_monthly")
        .filter(
            (f"city = '{city}'")
            & (f"year = {year}")
            & (f"month = {month}")
        )
        .toPandas()
    )
    return df


def plot_chicago_cluster_map(year: int = 2025, month: int = 7) -> None:
    """Plot all Chicago crimes for the month, grouped by offense."""
    df = load_coordinates("Chicago", year, month)

    top_types = df["offense_type"].value_counts().head(5).index.tolist()
    df["offense_group"] = df["offense_type"].apply(
        lambda x: x if x in top_types else "Other"
    )

    colors = plt.cm.tab10.colors
    groups = sorted(df["offense_group"].unique())
    color_map = {grp: colors[i % len(colors)] for i, grp in enumerate(groups)}

    plt.figure(figsize=(8, 8))
    for grp in groups:
        sub = df[df["offense_group"] == grp]
        plt.scatter(sub["longitude"], sub["latitude"], s=4, alpha=0.5, color=color_map[grp], label=grp)

    plt.title("Crime Types Cluster in Different Areas (July 2025)")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend(markerscale=3, fontsize=8, title="Offense Group")
    plt.tight_layout()
    plt.show()


def plot_chicago_theft_north_south(
    year: int = 2025, month: int = 7, north_cutoff_lat: float = 41.85
) -> None:
    """Plot Chicago theft with a north/south boundary."""
    df = load_coordinates("Chicago", year, month)
    theft_df = df[df["offense_type"].str.upper() == "THEFT"]

    plt.figure(figsize=(8, 8))
    plt.scatter(theft_df["longitude"], theft_df["latitude"], s=4, alpha=0.5, color="green")

    plt.axhline(north_cutoff_lat, color="black", linewidth=1)

    min_lon = theft_df["longitude"].min()
    plt.text(min_lon, north_cutoff_lat + 0.01, f"North (lat ≥ {north_cutoff_lat})", fontsize=9)
    plt.text(min_lon, north_cutoff_lat - 0.01, f"South (lat < {north_cutoff_lat})", fontsize=9)

    plt.title("Theft Incidents Are Concentrated in North Chicago (July 2025)")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.tight_layout()
    plt.show()


def plot_chicago_battery_assault_north_south(
    year: int = 2025, month: int = 7, north_cutoff_lat: float = 41.85
) -> None:
    """Plot battery and assault in Chicago with north/south boundary."""
    df = load_coordinates("Chicago", year, month)
    mask = df["offense_type"].str.upper().isin(["BATTERY", "ASSAULT"])
    ba_df = df[mask]

    plt.figure(figsize=(8, 8))
    plt.scatter(ba_df["longitude"], ba_df["latitude"], s=4, alpha=0.5, color="red")

    plt.axhline(north_cutoff_lat, color="black", linewidth=1)

    min_lon = ba_df["longitude"].min()
    plt.text(min_lon, north_cutoff_lat + 0.01, f"North (lat ≥ {north_cutoff_lat})", fontsize=9)
    plt.text(min_lon, north_cutoff_lat - 0.01, f"South (lat < {north_cutoff_lat})", fontsize=9)

    plt.title("Battery and Assault Locations in Chicago (July 2025)")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.tight_layout()
    plt.show()


def plot_dc_heatmap() -> None:
    """Plot DC crime heatmap by hour and weekday."""
    df = (
        spark.table("crime_lakehouse.analysis.crime_hourly")
        .filter("city = 'DC'")
        .toPandas()
    )

    day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    df["day_of_week"] = pd.Categorical(df["day_of_week"], categories=day_order, ordered=True)
    df = df.sort_values(["day_of_week", "hour"])

    pivot = df.pivot(index="day_of_week", columns="hour", values="crime_count")

    plt.figure(figsize=(10, 4))
    im = plt.imshow(pivot.values, aspect="auto", cmap="Reds")

    plt.yticks(range(len(day_order)), day_order)
    plt.xticks(range(24), range(24))
    plt.xlabel("Hour of Day")
    plt.ylabel("Day of Week")
    plt.title("Crime is More Common on Weekdays Washington, D.C. (2025 YTD)")

    cbar = plt.colorbar(im)
    cbar.set_label("Incidents")

    plt.tight_layout()
    plt.show()


def plot_population_adjusted_rates(year_filter: int = 2025) -> None:
    """Plot population-adjusted crime rates for Chicago and DC."""
    df = spark.table("crime_lakehouse.analysis.crime_monthly_pop_adjusted").toPandas()
    df = df[df["year"] == year_filter].sort_values("month")

    df["month_label"] = df.apply(lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1)

    x = range(len(df))
    chi = df["chi_per_100k"].values
    dc = df["dc_per_100k"].values

    plt.figure(figsize=(10, 4))
    plt.plot(x, chi, marker="o", color="tab:blue")
    plt.plot(x, dc, marker="o", color="tab:orange")

    plt.xticks(x, df["month_label"], rotation=45)
    plt.ylabel("Crimes per 100,000 Residents")
    plt.xlabel("Month")
    plt.title("Population-Adjusted Crime Rates: Chicago vs Washington, D.C. (2025)")

    plt.text(x[-1] + 0.05, chi[-1], "Chicago", color="tab:blue", fontsize=9)
    plt.text(x[-1] + 0.05, dc[-1], "Washington, D.C.", color="tab:orange", fontsize=9)

    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.show()


def _map_chicago_category(x: str) -> str:
    """Map Chicago offenses to unified categories."""
    if not x:
        return "Other"
    p = x.lower()
    if "theft" in p and "motor" not in p:
        return "Theft"
    if "motor" in p and "theft" in p:
        return "Motor Vehicle Theft"
    if p in ("battery", "assault", "criminal damage"):
        return "Violent Crime"
    if p == "robbery":
        return "Robbery"
    if p == "burglary":
        return "Burglary"
    return "Other"


def _map_dc_category(x: str) -> str:
    """Map DC offenses to unified categories."""
    if not x:
        return "Other"
    p = x.lower()
    if "theft" in p and "motor" not in p:
        return "Theft"
    if "motor vehicle theft" in p:
        return "Motor Vehicle Theft"
    if p in ("assault w/dangerous weapon", "robbery", "homicide"):
        return "Violent Crime"
    if p == "burglary":
        return "Burglary"
    if p == "sex abuse":
        return "Sex Crime"
    return "Other"


def plot_category_distributions() -> None:
    """Plot crime category bar charts for Chicago and DC."""
    chi = spark.table("crime_lakehouse.analysis.chi_offense_counts").toPandas()
    dc = spark.table("crime_lakehouse.analysis.dc_offense_counts").toPandas()

    chi["category"] = chi["offense_type"].apply(_map_chicago_category)
    dc["category"] = dc["offense_type"].apply(_map_dc_category)

    chi_cat = chi.groupby("category")["crime_count"].sum().reset_index()
    dc_cat = dc.groupby("category")["crime_count"].sum().reset_index()

    order = chi_cat.sort_values("crime_count", ascending=False)["category"].tolist()
    chi_cat = chi_cat.set_index("category").loc[order].reset_index()
    dc_cat = dc_cat.set_index("category").reindex(order).fillna(0).reset_index()

    def color_for(c):
        if c == "Theft": return "orange"
        if c == "Violent Crime": return "red"
        return "gray"

    chi_colors = [color_for(c) for c in chi_cat["category"]]
    dc_colors  = [color_for(c) for c in dc_cat["category"]]

    fig, axes = plt.subplots(1, 2, figsize=(10, 4), sharey=True)

    axes[0].barh(chi_cat["category"], chi_cat["crime_count"], color=chi_colors)
    axes[0].set_title("Chicago Crime Category Distribution (2025)")
    axes[0].set_xlabel("Incidents")

    axes[1].barh(dc_cat["category"], dc_cat["crime_count"], color=dc_colors)
    axes[1].set_title("Washington, D.C. Crime Category Distribution (2025)")
    axes[1].set_xlabel("Incidents")

    plt.tight_layout()
    plt.show()
