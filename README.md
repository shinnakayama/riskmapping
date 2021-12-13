# Revealing global risks of labor abuse and illegal, unreported, and unregulated fishing

## codes
- `fishing_trips.sql`: query for fishing trips in GFW datasets
- `transshipment_trips.sql`: query for trips by carrier vessels in GFW datasets
- `transshipment_loitering.sql`: query for trips by carrier vessels with loitering in GFW datasets
- `port_stop_duration.sql`: query for port stop duration in GFW datasets
- `port_visit.sql`: query for port visit for PSMA analysis

- `at_sea_analysis.py`: XGBoost and SHAP analysis for risk of fishing trips
- `fishing_bin_iuu.py`, `fishing_bin_la.py`: SQL query to bin the total fishing hours by grid for IUU fishing and labor abuse
- `plot_fishing_contour.r`: plot the total fishing hour by grid 
- `plot_fishing_shap.r`: plot shap importance and effect
- `transshipment_analysis.py`: XGBoost and SHAP analysis for risk of trips by carrier vessels
- `analyze_port_stop_duration.r`: linear mixed model on port stop duration by flag groups / gear type 
- `baci_analysis.py`: PSMA analysis


## data
- `c188.csv`: list of parties with C188 and dates
- `Updated_PSMA_dates_27 JAN 2021.csv`: list of PSMA parties and dates 
- `pew_port_capacity.csv`: Port capacity collated by PEW (https://www.pewtrusts.org/en/research-and-analysis/data-visualizations/2020/port-activity-study-reveals-potential-for-illegal-fish-to-enter-markets)
