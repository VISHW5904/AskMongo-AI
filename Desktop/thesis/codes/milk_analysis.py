import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import random
# Use tensorflow.keras imports
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras import backend as K
from datetime import timedelta # Import timedelta

# Reproducibility
np.random.seed(42)
random.seed(42)
tf.random.set_seed(42)

# --- Define Target Member Codes ---
target_member_codes = [
    '0010000008650045', '0010000008650047', '0010000019930006',
    '0010000019930014', '0010000054550058', '0010000054550136',
    '730110400066', '730110400002', '760540500086', '760540500013'
]
print(f"Targeting forecasts for {len(target_member_codes)} specific member codes using LSTM.")

# Custom RMSE metric (Optional, used in compile)
def rmse(y_true, y_pred):
    return K.sqrt(K.mean(K.square(y_pred - y_true)))

# --- Data Loading and Preprocessing ---
json_path = '/content/forecast_data.json' # Make sure this path is correct
if not os.path.exists(json_path):
    print(f"File not found: {json_path}")
    exit()

try:
    with open(json_path, 'r') as f:
        try: data = [json.loads(line) for line in f]
        except json.JSONDecodeError:
            f.seek(0)
            try: data = json.load(f);
            except json.JSONDecodeError as e: print(f"Error decoding JSON: {e}."); exit()
    if isinstance(data, dict): data = [data]
    elif not isinstance(data, list): print("JSON format error."); exit()
    print(f"Successfully loaded {len(data)} records.")
except Exception as e: print(f"Error reading file: {e}"); exit()

df = pd.DataFrame(data)
required_cols = ['dateTimeOfCollection', 'qty', 'memberCode']
if not all(col in df.columns for col in required_cols): print(f"Missing columns: {[c for c in required_cols if c not in df.columns]}"); exit()
df = df[required_cols]

def extract_date(x): # Using your robust function structure
    if isinstance(x, dict):
        if '$date' in x:
            date_val = x['$date']
            if isinstance(date_val, dict) and '$numberLong' in date_val:
                 try: return pd.to_datetime(int(date_val['$numberLong']), unit='ms', errors='coerce')
                 except (ValueError, TypeError): return pd.NaT
            else: return pd.to_datetime(date_val, errors='coerce')
    return pd.to_datetime(x, errors='coerce')

if 'dateTimeOfCollection' not in df.columns: print("dateTimeOfCollection column error."); exit()
df['dateTimeOfCollection'] = df['dateTimeOfCollection'].apply(extract_date)
df['qty'] = pd.to_numeric(df['qty'], errors='coerce')
original_rows = len(df)
df.dropna(subset=['dateTimeOfCollection', 'qty', 'memberCode'], inplace=True)
print(f"Dropped {original_rows - len(df)} rows due to missing values.")

# --- Timezone and Full Pivot Table Creation ---
if not df.empty:
    if df['dateTimeOfCollection'].dt.tz is None:
        df['dateTimeOfCollection'] = df['dateTimeOfCollection'].dt.tz_localize('UTC')
    else:
        df['dateTimeOfCollection'] = df['dateTimeOfCollection'].dt.tz_convert('UTC')
    df.set_index('dateTimeOfCollection', inplace=True)
else:
    print("DataFrame is empty after cleaning. Exiting.")
    exit()

# Pivot table: sum of qty per day per memberCode (ON FULL DATASET)
df_grouped = df.groupby(['memberCode', pd.Grouper(freq='D')])['qty'].sum().reset_index()
df_grouped = df_grouped.rename(columns={'dateTimeOfCollection': 'dateTime'}) # Rename before pivot
if 'dateTime' not in df_grouped.columns: print("Error: 'dateTime' column missing after grouping."); exit()

# Create the full pivot table BEFORE filtering by date for training
pivot_df_full = df_grouped.pivot(index='dateTime', columns='memberCode', values='qty').fillna(0)
print(f"Created full pivot table with shape: {pivot_df_full.shape}")

# --- Filter for Target Members ---
available_target_codes = [code for code in target_member_codes if code in pivot_df_full.columns]
missing_target_codes = [code for code in target_member_codes if code not in pivot_df_full.columns]
if not available_target_codes: print("None of the target members found in data. Exiting."); exit()
if missing_target_codes: print(f"Warning: Missing target members: {missing_target_codes}")

# Use only available target codes from now on
pivot_df_target_full = pivot_df_full[available_target_codes].copy() # Work with target members only

# --- Split Data for Training and Actual Comparison ---
train_end_date = pd.Timestamp('2025-01-01', tz='UTC')
forecast_start_date = train_end_date + timedelta(days=1)

pivot_df_train = pivot_df_target_full[pivot_df_target_full.index <= train_end_date].copy()
pivot_df_actual = pivot_df_target_full[pivot_df_target_full.index >= forecast_start_date].copy()

if pivot_df_train.empty:
    print(f"No training data available up to {train_end_date}. Exiting.")
    exit()

print(f"Training data shape: {pivot_df_train.shape}")
if not pivot_df_actual.empty:
    print(f"Actual data shape for comparison: {pivot_df_actual.shape}")
else:
    print("Warning: No actual data found after training end date for accuracy comparison.")

# --- LSTM Forecasting Parameters ---
forecast_df = pd.DataFrame()
history_length = 50 # Days of history (Keep or adjust)
future_days = 10   # Days to predict
lstm_epochs = 60   # Slightly increased epochs
lstm_batch_size = 16

# Store scalers for potential reuse or analysis
scalers = {}

for member_col in pivot_df_train.columns:
    print(f"\nProcessing Member Code: {member_col}")
    K.clear_session() # Clear previous model graph

    member_series_train = pivot_df_train[member_col].values.reshape(-1, 1)

    if len(member_series_train) < history_length + future_days: # Need history + future for label
        print(f"Skipping {member_col}: Not enough training data ({len(member_series_train)} points) for history ({history_length}) + future ({future_days}).")
        continue

    # Normalize using training data
    scaler = MinMaxScaler(feature_range=(0, 1))
    member_scaled_train = scaler.fit_transform(member_series_train)
    scalers[member_col] = scaler # Store scaler

    # Prepare sequences for DIRECT multi-step prediction
    X, y = [], []
    # Loop stops early enough to get a full 'future_days' label sequence
    for i in range(len(member_scaled_train) - history_length - future_days + 1):
        X.append(member_scaled_train[i : i + history_length])
        # y is now the sequence of the *next* 'future_days' values
        y.append(member_scaled_train[i + history_length : i + history_length + future_days].flatten()) # Flatten to 1D array

    if not X:
        print(f"Skipping {member_col}: Could not create training sequences.")
        continue

    X = np.array(X)
    y = np.array(y)

    # Check shapes before training
    if X.shape[0] != y.shape[0]:
         print(f"Error: Mismatch in X ({X.shape[0]}) and y ({y.shape[0]}) samples for {member_col}. Skipping.")
         continue
    if y.shape[1] != future_days:
         print(f"Error: y shape ({y.shape}) does not match future_days ({future_days}) for {member_col}. Skipping.")
         continue

    print(f"Training data shape for {member_col}: X={X.shape}, y={y.shape}")

    # Build LSTM model for DIRECT multi-step output
    model = Sequential([
        # Consider return_sequences=True if adding another LSTM layer
        LSTM(60, input_shape=(X.shape[1], X.shape[2])),
        Dropout(0.3), # Slightly reduced dropout
        Dense(future_days) # Directly output 'future_days' predictions
    ])
    model.compile(loss='mse', optimizer='adam', metrics=[rmse])

    print(f"Training model for {member_col}...")
    # Train the model (Consider adding validation_split=0.1 for monitoring)
    history = model.fit(X, y, epochs=lstm_epochs, batch_size=lstm_batch_size, validation_split=0.1, verbose=0) # Added validation split
    print(f"Training completed.")
    # Optional: Print validation loss/rmse if needed
    if 'val_loss' in history.history:
       print(f"  Final Training Loss: {history.history['loss'][-1]:.4f}, Final Validation Loss: {history.history['val_loss'][-1]:.4f}")
       print(f"  Final Training RMSE: {history.history['rmse'][-1]:.4f}, Final Validation RMSE: {history.history['val_rmse'][-1]:.4f}")
    else:
       print(f"  Final Training Loss: {history.history['loss'][-1]:.4f}, Final Training RMSE: {history.history['rmse'][-1]:.4f}")


    # --- Direct Multi-step Prediction ---
    # Use the *last* sequence from the training data as input
    last_sequence_train_scaled = member_scaled_train[-history_length:].reshape(1, history_length, 1)

    # Predict the full future sequence directly
    prediction_scaled = model.predict(last_sequence_train_scaled, verbose=0)[0] # Shape (future_days,)

    # Inverse transform the direct prediction
    # Reshape needed for scaler: expects (n_samples, n_features=1)
    prediction = scaler.inverse_transform(prediction_scaled.reshape(-1, 1)).flatten()
    prediction[prediction < 0] = 0 # Ensure non-negative

    # --- REQUIREMENT 1: Print Forecast Array ---
    print(f"  Forecasted values (array): {np.round(prediction, 2)}")
    # -------------------------------------------

    # Prepare forecast index
    last_train_date = pivot_df_train.index[-1]
    forecast_index = pd.date_range(start=last_train_date + timedelta(days=1), periods=future_days, freq='D')

    # Add forecast to the forecast DataFrame
    forecast_df[member_col] = pd.Series(prediction, index=forecast_index)
    print(f"Forecast generated for {member_col}.")

# --- Plotting and Accuracy Section ---
if not forecast_df.empty:
    print(f"\n--- Plotting & Accuracy Evaluation ---")
    plot_start_date = pd.Timestamp('2024-11-01', tz='UTC')
    plot_end_date = pd.Timestamp('2025-01-15', tz='UTC')

    for member_col_to_plot in forecast_df.columns:
        plt.figure(figsize=(14, 7))
        # Get historical data from the training set within the plot range
        history_in_range = pivot_df_train.loc[plot_start_date:plot_end_date, member_col_to_plot].dropna()
        if not history_in_range.empty:
            plt.plot(history_in_range.index, history_in_range.values, label='Historical (Train)', color='cornflowerblue', marker='.', linestyle='-', linewidth=1.5)

        # Get forecast data within the plot range
        forecast_in_range = forecast_df.loc[plot_start_date:plot_end_date, member_col_to_plot].dropna()
        if not forecast_in_range.empty:
            plt.plot(forecast_in_range.index, forecast_in_range.values, label='Forecast', color='tomato', marker='o', linestyle='--', linewidth=1.5, markersize=5)

        # --- Accuracy Calculation ---
        mape = np.nan; accuracy_pct = np.nan
        actual_data_for_forecast_period = pivot_df_actual.loc[forecast_in_range.index, member_col_to_plot].dropna()

        if not actual_data_for_forecast_period.empty and len(actual_data_for_forecast_period) == len(forecast_in_range):
            actual_vals = actual_data_for_forecast_period.values
            predicted_vals = forecast_in_range.values
            epsilon = 1e-6
            mape = np.mean(np.abs((actual_vals - predicted_vals) / (actual_vals + epsilon))) * 100
            accuracy_pct = max(0, 100 - mape)
            print(f"\nAccuracy for {member_col_to_plot}: MAPE={mape:.2f}%, Accuracy={accuracy_pct:.2f}%")
            plt.plot(actual_data_for_forecast_period.index, actual_data_for_forecast_period.values, label='Actual (Post-Train)', color='green', marker='x', linestyle=':', linewidth=1.5, markersize=6)
            plot_title = f'Forecast vs Actual - Member: {member_col_to_plot}\nAccuracy (100-MAPE): {accuracy_pct:.2f}%'
        elif forecast_in_range.empty:
             plot_title = f'Historical Data - Member: {member_col_to_plot}'
        else:
            print(f"\nAccuracy for {member_col_to_plot}: Not calculated (Actual data mismatch/missing)")
            plot_title = f'Forecast (No Actuals) - Member: {member_col_to_plot}'

        # --- Date Ticks ---
        try:
            all_indices = history_in_range.index.union(forecast_in_range.index)
            if not pivot_df_actual.empty and not actual_data_for_forecast_period.empty: all_indices = all_indices.union(actual_data_for_forecast_period.index)
            if not all_indices.empty:
                 tick_start = max(plot_start_date, all_indices.min()); tick_end = min(plot_end_date, all_indices.max())
                 if tick_end >= tick_start: date_ticks = pd.date_range(start=tick_start, end=tick_end, freq='5D'); plt.xticks(ticks=date_ticks, rotation=45, ha='right', fontsize=9)
                 else: plt.xticks(rotation=45, ha='right', fontsize=9)
            else: plt.xticks(rotation=45, ha='right', fontsize=9)
        except Exception as e: print(f"Warn: Ticks failed for {member_col_to_plot}: {e}"); plt.xticks(rotation=45, ha='right', fontsize=9)

        plt.title(plot_title, fontsize=12)
        plt.xlabel('Date', fontsize=10); plt.ylabel('Quantity (qty)', fontsize=10)
        plt.legend(fontsize=9); plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout(); plt.show()

else:
    print("\nNo forecasts were generated.")

print("\nScript finished.")