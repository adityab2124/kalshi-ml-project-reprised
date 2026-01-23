#!/usr/bin/env python3
"""
Analyze if Kalshi prices are well-calibrated (i.e., do 70% predictions happen 70% of the time?)

This script:
1. Loads market data with outcomes
2. Compares predicted probabilities to actual outcomes
3. Tests calibration using various ML models
4. Visualizes calibration curves
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.calibration import calibration_curve, CalibratedClassifierCV
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
import matplotlib.pyplot as plt
import seaborn as sns

def load_data(csv_file="kalshi_markets.csv"):
    """Load market data"""
    df = pd.read_csv(csv_file)
    
    # Filter to settled/finalized markets with outcomes
    # Kalshi uses both 'settled' and 'finalized' status
    settled = df[df['status'].isin(['settled', 'finalized'])].copy()
    settled = settled[settled['outcome'].notna()]
    settled = settled[settled['outcome'] != '']  # Also filter out empty strings
    
    print(f"Loaded {len(settled)} settled/finalized markets with outcomes")
    if len(settled) > 0:
        print(f"  Status breakdown: {settled['status'].value_counts().to_dict()}")
        print(f"  Markets with price data: {settled['yes_probability'].notna().sum()}")
    
    # Check if we have price data
    if len(settled) > 0 and settled['yes_probability'].notna().sum() == 0:
        print("\n⚠️  WARNING: No price data found for finalized markets.")
        print("   Finalized markets don't have current prices.")
        print("   For calibration analysis, you need:")
        print("   1. Collect OPEN markets (with prices) and track until they close")
        print("   2. OR use historical price data from before markets closed")
        print("\n   Trying to find markets with any price data...")
        
        # Check all markets, not just settled
        all_with_prices = df[df['yes_probability'].notna()].copy()
        if len(all_with_prices) > 0:
            print(f"   Found {len(all_with_prices)} markets with price data (likely open markets)")
            print("   Note: These don't have outcomes yet, so can't do calibration analysis.")
        else:
            print("   No markets with price data found in dataset.")
    
    return settled

def prepare_features(df):
    """
    Prepare features for ML model:
    - yes_probability: Kalshi's predicted probability
    - Add other features if available (category, time to close, etc.)
    """
    # Create binary outcome (1 if YES won, 0 if NO won)
    df['actual_outcome'] = df['outcome'].apply(
        lambda x: 1 if str(x).upper() in ['YES', 'Y', '1', 'TRUE'] else 0
    )
    
    # Use yes_probability as the main feature
    df = df[df['yes_probability'].notna()].copy()
    
    features = ['yes_probability']
    
    # Add categorical features if available
    if 'category' in df.columns:
        df = pd.get_dummies(df, columns=['category'], prefix='cat', drop_first=True)
        cat_cols = [c for c in df.columns if c.startswith('cat_')]
        features.extend(cat_cols)
    
    X = df[features].values
    y = df['actual_outcome'].values
    
    return X, y, df

def test_calibration(X, y, model, model_name):
    """
    Test if model predictions are well-calibrated
    Returns calibration curve data
    """
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )
    
    # Train model
    model.fit(X_train, y_train)
    
    # Get predictions
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    # Calculate metrics
    brier = brier_score_loss(y_test, y_pred_proba)
    logloss = log_loss(y_test, y_pred_proba)
    auc = roc_auc_score(y_test, y_pred_proba)
    
    print(f"\n{model_name} Metrics:")
    print(f"  Brier Score (lower is better): {brier:.4f}")
    print(f"  Log Loss (lower is better): {logloss:.4f}")
    print(f"  ROC AUC (higher is better): {auc:.4f}")
    
    # Calibration curve
    fraction_of_positives, mean_predicted_value = calibration_curve(
        y_test, y_pred_proba, n_bins=10
    )
    
    return {
        'fraction_of_positives': fraction_of_positives,
        'mean_predicted_value': mean_predicted_value,
        'brier': brier,
        'logloss': logloss,
        'auc': auc,
        'model_name': model_name
    }

def plot_calibration_curves(results):
    """Plot calibration curves for all models"""
    plt.figure(figsize=(12, 8))
    
    # Perfect calibration line
    plt.plot([0, 1], [0, 1], 'k--', label='Perfect Calibration')
    
    # Plot each model
    for result in results:
        plt.plot(
            result['mean_predicted_value'],
            result['fraction_of_positives'],
            marker='o',
            label=f"{result['model_name']} (Brier: {result['brier']:.3f})"
        )
    
    plt.xlabel('Mean Predicted Probability', fontsize=12)
    plt.ylabel('Fraction of Positives', fontsize=12)
    plt.title('Calibration Curves: Kalshi Price Predictions vs Actual Outcomes', fontsize=14)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('calibration_curves.png', dpi=300)
    print("\n✓ Saved calibration_curves.png")
    plt.close()

def analyze_bins(df):
    """
    Analyze calibration by binning predictions:
    - Group predictions into bins (0-10%, 10-20%, etc.)
    - Check if actual outcomes match predicted probabilities
    """
    df['prob_bin'] = pd.cut(df['yes_probability'], bins=10, labels=False) * 10
    
    bin_analysis = df.groupby('prob_bin').agg({
        'yes_probability': 'mean',
        'actual_outcome': 'mean',
        'ticker': 'count'
    }).rename(columns={
        'yes_probability': 'predicted_prob',
        'actual_outcome': 'actual_frequency',
        'ticker': 'count'
    })
    
    print("\n" + "="*60)
    print("CALIBRATION BY BIN")
    print("="*60)
    print(f"{'Bin':<10} {'Predicted':<12} {'Actual':<12} {'Count':<10} {'Difference':<10}")
    print("-"*60)
    
    for bin_val, row in bin_analysis.iterrows():
        diff = row['actual_frequency'] - row['predicted_prob']
        print(f"{int(bin_val):<10} {row['predicted_prob']:<12.2%} {row['actual_frequency']:<12.2%} "
              f"{int(row['count']):<10} {diff:+.2%}")
    
    return bin_analysis

def main():
    """Main analysis"""
    print("="*60)
    print("KALSHI PRICE CALIBRATION ANALYSIS")
    print("="*60)
    
    # Load data
    df = load_data()
    
    if len(df) == 0:
        print("No settled markets found. Need to collect more data first.")
        return
    
    # Prepare features
    X, y, df = prepare_features(df)
    
    if len(df) == 0:
        print("\n❌ Cannot proceed: No markets with both price data and outcomes.")
        print("\nSolution: Collect OPEN markets (which have prices) and track them.")
        print("Then re-run analysis once some markets have closed.")
        return
    
    print(f"\nDataset: {len(df)} markets")
    print(f"YES outcomes: {y.sum()} ({y.mean():.1%})")
    print(f"NO outcomes: {(1-y).sum()} ({(1-y.mean()):.1%})")
    
    # Analyze calibration by bins (simple approach)
    bin_analysis = analyze_bins(df)
    
    # Test with ML models
    print("\n" + "="*60)
    print("ML MODEL CALIBRATION TESTS")
    print("="*60)
    
    models = [
        ('Baseline (Kalshi Price)', None),  # Just use the price directly
        ('Logistic Regression', LogisticRegression()),
        ('Random Forest', RandomForestClassifier(n_estimators=100, random_state=42)),
        ('Gradient Boosting', GradientBoostingClassifier(n_estimators=100, random_state=42)),
    ]
    
    results = []
    
    # Baseline: Just use Kalshi prices directly
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )
    y_pred_baseline = X_test[:, 0]  # Just use yes_probability
    
    brier_baseline = brier_score_loss(y_test, y_pred_baseline)
    logloss_baseline = log_loss(y_test, y_pred_baseline)
    auc_baseline = roc_auc_score(y_test, y_pred_baseline)
    
    fraction_of_positives, mean_predicted_value = calibration_curve(
        y_test, y_pred_baseline, n_bins=10
    )
    
    results.append({
        'fraction_of_positives': fraction_of_positives,
        'mean_predicted_value': mean_predicted_value,
        'brier': brier_baseline,
        'logloss': logloss_baseline,
        'auc': auc_baseline,
        'model_name': 'Baseline (Kalshi Price)'
    })
    
    print(f"\nBaseline (Kalshi Price) Metrics:")
    print(f"  Brier Score: {brier_baseline:.4f}")
    print(f"  Log Loss: {logloss_baseline:.4f}")
    print(f"  ROC AUC: {auc_baseline:.4f}")
    
    # Test other models
    for name, model in models[1:]:
        result = test_calibration(X, y, model, name)
        results.append(result)
    
    # Plot calibration curves
    plot_calibration_curves(results)
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print("\nInterpretation:")
    print("  - If calibration curve is close to diagonal: prices are well-calibrated")
    print("  - Lower Brier score = better calibrated predictions")
    print("  - Check if 70% predictions actually happen ~70% of the time")

if __name__ == "__main__":
    main()

