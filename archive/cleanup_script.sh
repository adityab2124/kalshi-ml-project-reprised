#!/bin/bash

# Create archive folder
mkdir -p archive

# Move old files to archive
mv analyze_calibration.py archive/ 2>/dev/null
mv ANALYSIS_README.md archive/ 2>/dev/null
mv add_historical_prices.py archive/ 2>/dev/null
mv build_dataset.py archive/ 2>/dev/null
mv build_smpl_ds.py archive/ 2>/dev/null
mv collect_market_data.py archive/ 2>/dev/null
mv collect_open_markets.py archive/ 2>/dev/null
mv run_checks.py archive/ 2>/dev/null
mv verify_trades.py archive/ 2>/dev/null
mv test_bernie_contracts.py archive/ 2>/dev/null
mv test_google.py archive/ 2>/dev/null
mv demo.py archive/ 2>/dev/null
mv clients.py archive/ 2>/dev/null
mv *.csv archive/ 2>/dev/null
mv *.db archive/ 2>/dev/null

echo "✅ Files archived locally"
