# Kalshi Price Calibration Analysis

## Goal
Determine if Kalshi prices (e.g., 70% YES = $0.70) accurately reflect real-world probabilities.

## Approach

### 1. Data Collection (`collect_market_data.py`)
- Pulls all markets from Kalshi API
- Extracts:
  - Current YES/NO prices (probabilities)
  - Market status (open/settled)
  - Outcomes (for settled markets)
  - Market metadata (title, category, etc.)
- Saves to CSV for analysis

### 2. Calibration Analysis (`analyze_calibration.py`)
Tests if predictions are **well-calibrated**:
- **Well-calibrated**: Markets priced at 70% actually win ~70% of the time
- **Poorly calibrated**: Markets priced at 70% might only win 50% of the time

### 3. ML Models to Test

#### Baseline: Direct Kalshi Prices
- Just use the YES probability from Kalshi prices
- If well-calibrated, this should already be accurate

#### Logistic Regression
- Simple, interpretable
- Good baseline for probability estimation
- Can add features (category, time to close, etc.)

#### Random Forest
- Handles non-linear relationships
- Can capture interactions between features
- Good for feature importance analysis

#### Gradient Boosting (XGBoost/LightGBM)
- State-of-the-art for probability estimation
- Best performance typically
- Can be calibrated with Platt scaling

### 4. Metrics

**Brier Score**: Lower is better
- Measures calibration quality
- Perfect = 0, Worst = 1

**Log Loss**: Lower is better  
- Penalizes confident wrong predictions
- Good for probability models

**ROC AUC**: Higher is better
- Measures discrimination ability
- 0.5 = random, 1.0 = perfect

**Calibration Curves**: Visual check
- X-axis: Predicted probability (0-100%)
- Y-axis: Actual frequency of YES outcomes
- Perfect calibration = diagonal line

## Usage

### Step 1: Collect Data
```bash
python3 collect_market_data.py
```
This creates `kalshi_markets.csv` with all market data.

### Step 2: Analyze Calibration
```bash
# Install dependencies first
pip install pandas numpy scikit-learn matplotlib seaborn

python3 analyze_calibration.py
```

This will:
1. Load settled markets with outcomes
2. Compare predicted probabilities to actual outcomes
3. Test calibration with multiple ML models
4. Generate calibration curve plots
5. Show bin-by-bin analysis

## Expected Results

### If Kalshi is Well-Calibrated:
- Calibration curve follows diagonal line
- Brier score < 0.25
- Bin analysis shows predicted ≈ actual for each probability range

### If Kalshi is Poorly Calibrated:
- Calibration curve deviates from diagonal
- Systematic over/under-confidence
- Opportunities for arbitrage or better predictions

## Next Steps

1. **Collect more data**: Need settled markets with outcomes
2. **Feature engineering**: Add market category, time to close, volume, etc.
3. **Time series analysis**: How do prices change as events approach?
4. **Category-specific analysis**: Are some categories better calibrated?
5. **Build prediction model**: If prices are miscalibrated, can we predict better?

## Notes

- Demo API may have limited settled markets
- Production API will have more historical data
- Focus on markets with clear binary outcomes (elections, sports, etc.)

