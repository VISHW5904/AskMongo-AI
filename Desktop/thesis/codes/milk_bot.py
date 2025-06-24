import google.generativeai as genai
from google.colab import userdata
import json
import os
import pandas as pd
from datetime import datetime, timedelta # Added timedelta
import numpy as np # Added numpy for potential std dev if needed again

# --- Configure Gemini API ---
model = None
try:
    GOOGLE_API_KEY = userdata.get('GOOGLE_API_KEY')
    if GOOGLE_API_KEY is None:
        raise ValueError("Secret GOOGLE_API_KEY not found in Colab Secrets or is empty.")
    genai.configure(api_key=GOOGLE_API_KEY)
    print("✅ Gemini API Configured.")

    print("\n🔍 Listing available models that support 'generateContent':")
    available_models_list = [] # Renamed to avoid conflict
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(f"  - {m.name}")
            available_models_list.append(m.name)

    if not available_models_list:
        raise ValueError("No models found that support 'generateContent'. Check key/project.")

    # User's chosen model - ensure this is in your available_models_list
    # chosen_model_name_user = 'gemini-2.5-flash-preview-04-17-thinking'
    chosen_model_name = 'gemini-1.5-flash-latest' # Defaulting to a common fast model
    print(f"\nAttempting to use model: '{chosen_model_name}'")
    print("If this model is not in the list above, API calls may fail or use a different model if the name is an alias.")
    print("Please verify and select an appropriate model from the 'available models' list for best results.")


    # Check if the chosen_model_name is actually in the list (with or without 'models/' prefix)
    if not (chosen_model_name in available_models_list or f"models/{chosen_model_name}" in available_models_list):
        print(f"⚠️ Warning: Chosen model '{chosen_model_name}' not found in your explicit list of available models.")
        if available_models_list:
            print(f"   Consider using one of these: {', '.join(available_models_list[:3])}{'...' if len(available_models_list) > 3 else ''}")
        else:
            print("   No available models were listed. API calls will likely fail.")
        # Attempt to initialize anyway, API might handle aliases or have other models

    model = genai.GenerativeModel(chosen_model_name)
    # model.model_name might resolve to the fully qualified name like 'models/gemini-1.5-flash-latest'
    print(f"✅ Model '{model.model_name if hasattr(model, 'model_name') else chosen_model_name}' targeted for initialization.")


except ValueError as ve:
    print(f"❌ Configuration Error: {ve}")
    if "GOOGLE_API_KEY" in str(ve):
        print("   Ensure 'GOOGLE_API_KEY' is correctly set in Colab Secrets.")
except Exception as e:
    print(f"❌ Error configuring Gemini API or initializing model: {e}")
    print("   Make sure GOOGLE_API_KEY is valid. Check available models.")

# --- Load JSON Data ---
# (Your existing data loading logic - no changes needed here, assuming it works)
json_path = '/content/play_data.json'
milk_df = None
data_loaded = False
if os.path.exists(json_path):
    try:
        with open(json_path, 'r', encoding='utf-8') as f: data_from_json = json.load(f)
        print(f"✅ Successfully loaded JSON from {json_path}")
        records_for_df = []
        for record in data_from_json:
            flat_record = record.copy()
            if '_id' in flat_record and isinstance(flat_record['_id'], dict): flat_record['_id'] = flat_record['_id'].get('$oid', str(flat_record['_id']))
            if 'dateTimeOfCollection' in flat_record:
                dt_obj = None; date_val = flat_record['dateTimeOfCollection']
                if isinstance(date_val, dict) and '$date' in date_val: date_str = date_val['$date']
                elif isinstance(date_val, str): date_str = date_val
                else: date_str = None
                if date_str:
                    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                        try: dt_obj = datetime.strptime(date_str.split('.')[0].replace('Z',''), fmt.split('.')[0].replace('Z','')); break
                        except: continue
                flat_record['dateTimeParsed'] = pd.to_datetime(dt_obj, errors='coerce')
            records_for_df.append(flat_record)
        milk_df = pd.DataFrame(records_for_df)
        if 'dateTimeParsed' in milk_df.columns: milk_df['dateTimeParsed'] = pd.to_datetime(milk_df['dateTimeParsed'], errors='coerce')
        if 'qty' in milk_df.columns: milk_df['qty'] = pd.to_numeric(milk_df['qty'], errors='coerce').fillna(0)
        if 'fat' in milk_df.columns: milk_df['fat'] = pd.to_numeric(milk_df['fat'], errors='coerce')
        if 'snf' in milk_df.columns: milk_df['snf'] = pd.to_numeric(milk_df['snf'], errors='coerce')
        if 'rtpl' in milk_df.columns: milk_df['rtpl'] = pd.to_numeric(milk_df['rtpl'], errors='coerce')
        if 'amount' in milk_df.columns: milk_df['amount'] = pd.to_numeric(milk_df['amount'], errors='coerce').fillna(0) # Ensure amount is loaded
        if 'memberCode' in milk_df.columns: milk_df['memberCode'] = milk_df['memberCode'].astype(str)
        if 'dcsCode' in milk_df.columns: milk_df['dcsCode'] = milk_df['dcsCode'].astype(str) # Ensure dcsCode is str
        print(f"✅ Data ready in DataFrame (Shape: {milk_df.shape}). Missing 'dateTimeParsed': {milk_df['dateTimeParsed'].isnull().sum()}")
        data_loaded = True
    except Exception as e: print(f"❌ Error loading/processing data: {e}")
else: print(f"❌ Error: File not found at path: {json_path}")


# --- LLM Interaction Function ---
def get_query_parameters_from_llm(user_query, prev_context=None):
    if not model:
        print("LLM Model not available.")
        return {"intent": "error_llm_not_initialized"}

    prev_context_summary = "No previous context."
    if prev_context:
        prev_context_summary = "Previous context: "
        if prev_context.get("last_params") and prev_context["last_params"].get("intent"):
            prev_context_summary += f"Last query intent was '{prev_context['last_params']['intent']}'. "
        if prev_context.get("last_identified_entities"):
             prev_context_summary += f"Last query focused on entities: {prev_context.get('last_identified_entities')}. "

    # --- UPDATED SCHEMA ---
    json_schema = """
    {
        "type": "object",
        "properties": {
            "intent": {"type": "string", "description": "Primary goal of the user's query. See prompt for examples."},
            "entity1_type": {"type": ["string", "null"], "enum": ["memberCode", "dcsCode", null], "description": "Type of the first entity for comparison or filtering."},
            "entity1_id": {"type": ["string", "null"], "description": "ID of the first entity."},
            "entity2_type": {"type": ["string", "null"], "enum": ["memberCode", "dcsCode", null], "description": "Type of the second entity for comparison."},
            "entity2_id": {"type": ["string", "null"], "description": "ID of the second entity."},
            "start_date": {"type": ["string", "null"], "description": "Start date (YYYY-MM-DD). Infer from natural language like 'last week', 'March 2025'."},
            "end_date": {"type": ["string", "null"], "description": "End date (YYYY-MM-DD)."},
            "period1_start_date": {"type": ["string", "null"], "description": "Start date for the first period in a two-period comparison."},
            "period1_end_date": {"type": ["string", "null"], "description": "End date for the first period."},
            "period2_start_date": {"type": ["string", "null"], "description": "Start date for the second period."},
            "period2_end_date": {"type": ["string", "null"], "description": "End date for the second period."},
            "metric_to_filter": {"type": ["string", "null"], "enum": ["qty", "fat", "snf", "rtpl", "amount", null]},
            "filter_operator": {"type": ["string", "null"], "enum": [">", "<", ">=", "<=", "==", "!=", null]},
            "filter_value": {"type": ["number", "null"]},
            "metric_to_compare": {"type": ["string", "null"], "enum": ["avg_qty", "total_qty", "avg_fat", "total_fat", "avg_snf", "total_snf", "avg_rtpl", "total_rtpl", "avg_amount", "total_amount", "collection_count", null]},
            "trend_metric": {"type": ["string", "null"], "enum": ["qty", "fat", "snf", "rtpl", "amount", null]},
            "trend_aggregation": {"type": ["string", "null"], "enum": ["sum", "mean", null], "description": "'sum' for total, 'mean' for average."},
            "trend_resample_period": {"type": ["string", "null"], "enum": ["D", "W", "M", "Q", "Y", null], "description": "D=Day, W=Week, M=Month, Q=Quarter, Y=Year."},
            "sort_by_metric": {"type": ["string", "null"], "enum": ["qty", "fat", "snf", "rtpl", "amount", "dateTimeParsed", null]},
            "sort_order": {"type": ["string", "null"], "enum": ["asc", "desc", null]},
            "limit_n": {"type": ["integer", "null"]},
            "list_unique_target": {"type": ["string", "null"], "enum": ["memberCode", "dcsCode", null]},
            "count_unique_target": {"type": ["string", "null"], "enum": ["memberCode", "dcsCode", null]},
            "clarification_needed": {"type": "boolean", "default": false, "description": "True if the query is ambiguous and needs clarification."},
            "clarification_question": {"type": ["string", "null"], "description": "A question to ask the user if clarification_needed is true."}
        },
        "required": ["intent"]
    }
    """
    # --- UPDATED PROMPT ---
    prompt = f"""
    You are an AI assistant analyzing milk collection data.
    Data fields: 'memberCode', 'dcsCode', 'dateTimeParsed' (datetime), 'qty', 'fat', 'snf', 'rtpl', 'amount'.
    Today's date is {datetime.now().strftime('%Y-%m-%d')}.
    {prev_context_summary}

    Parse the user's question into a JSON object matching the schema.
    If a query is ambiguous, set "clarification_needed": true and "clarification_question".
    Handle date inference: "last X months", "yesterday", "November", "March 2025" should be converted to YYYY-MM-DD start/end dates.
    For follow-up questions, use the previous context if terms like "them", "their" are used.

    Intent Examples & Parameter Mapping:
    - "get_summary_and_details": "Details for member X from date A to B." -> entity1_type: memberCode, entity1_id: X, start_date: A, end_date: B
    - "get_top_n_records": "Top 5 collections by quantity." -> sort_by_metric: qty, sort_order: desc, limit_n: 5
    - "list_unique": "List unique member codes." -> list_unique_target: memberCode
    - "count_unique": "How many DCS codes?" -> count_unique_target: dcsCode
    - "compare_two_entities":
        - "Compare average qty for member XXX and YYY." -> entity1_type: memberCode, entity1_id: XXX, entity2_type: memberCode, entity2_id: YYY, metric_to_compare: avg_qty
        - "Which member collected more milk: M1 or M2?" -> entity1_type: memberCode, entity1_id: M1, entity2_type: memberCode, entity2_id: M2, metric_to_compare: total_qty (infer comparison based on this)
    - "compare_entity_over_two_periods": "Compare results for member XXX for Nov and Dec." -> entity1_type: memberCode, entity1_id: XXX, metric_to_compare: total_qty (or other relevant default), period1_start_date: YYYY-11-01, period1_end_date: YYYY-11-30, period2_start_date: YYYY-12-01, period2_end_date: YYYY-12-31
    - "get_trend":
        - "Show the trend of total milk collection over the last 3 months." -> trend_metric: qty, trend_aggregation: sum, trend_resample_period: M (infer start/end dates for last 3 months)
        - "Trend of SNF for member XXX over time." -> trend_metric: snf, trend_aggregation: mean (for individual readings), entity1_type: memberCode, entity1_id: XXX
        - "Trend of average fat per month in 2023." -> trend_metric: fat, trend_aggregation: mean, trend_resample_period: M, start_date: 2023-01-01, end_date: 2023-12-31
        - "Trend of average fat per month in 2023 for member X and member Y." -> (This is complex. Prioritize one entity or ask for clarification. For now, handle for one entity: entity1_type: memberCode, entity1_id: X, trend_metric: fat, trend_aggregation: mean, trend_resample_period: M, start_date: 2023-01-01, end_date: 2023-12-31)

    User Question: "{user_query}"

    JSON Output:
    """
    try:
        print("🤖 Sending query to LLM...")
        response = model.generate_content(prompt)
        json_response_text = response.text.strip().lstrip('```json').rstrip('```').strip()
        parameters = json.loads(json_response_text)

        # Default all possible keys to None
        all_schema_keys = [p for p in json.loads(json_schema)["properties"].keys()]
        for key in all_schema_keys:
            parameters.setdefault(key, None)

        if not parameters.get("intent"):
            parameters["intent"] = "unknown"
            print("⚠️ LLM did not identify intent.")

        # Normalize top/bottom
        if parameters["intent"] == "get_bottom_n_records":
            parameters["intent"] = "get_top_n_records"
            if parameters.get("sort_order") is None: parameters["sort_order"] = "asc"

        print(f"✅ LLM analysis. Intent: {parameters.get('intent')}")
        return parameters
    except json.JSONDecodeError as e:
        print(f"❌ Error decoding JSON from LLM: {e}. Raw: {response.text if 'response' in locals() else 'N/A'}")
        return {"intent": "error_decoding_json"}
    except Exception as e:
        print(f"❌ LLM interaction error: {e}")
        return {"intent": "error_llm_interaction"}


# --- Helper to apply dynamic filters ---
def apply_filters_from_params(df, params):
    filtered_df = df.copy()
    # Entity filters
    if params.get("entity1_id") and params.get("entity1_type"):
        e_type = params["entity1_type"]
        e_id = params["entity1_id"]
        if e_type in filtered_df.columns:
             if isinstance(e_id, list): # For follow-ups where entity1_id might be a list
                filtered_df = filtered_df[filtered_df[e_type].isin(e_id)]
             else:
                filtered_df = filtered_df[filtered_df[e_type] == str(e_id)]
        else: print(f"⚠️ Filter column '{e_type}' not found.")


    # Date filters (primary range)
    if params.get("start_date"):
        try:
            start_dt = pd.to_datetime(params["start_date"]).normalize()
            filtered_df = filtered_df[filtered_df['dateTimeParsed'] >= start_dt]
        except: print(f"⚠️ Invalid start_date: {params['start_date']}")
    if params.get("end_date"):
        try:
            # Inclusive of the end date by going to the very end of that day
            end_dt = pd.to_datetime(params["end_date"]).normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            filtered_df = filtered_df[filtered_df['dateTimeParsed'] <= end_dt]
        except: print(f"⚠️ Invalid end_date: {params['end_date']}")

    # Metric filter (e.g., qty > 10)
    if params.get("metric_to_filter") and params.get("filter_operator") and params.get("filter_value") is not None:
        field = params["metric_to_filter"]
        op = params["filter_operator"]
        val = params["filter_value"]
        if field in filtered_df.columns:
            try:
                val_num = float(val)
                if op == ">": filtered_df = filtered_df[filtered_df[field] > val_num]
                elif op == "<": filtered_df = filtered_df[filtered_df[field] < val_num]
                elif op == ">=": filtered_df = filtered_df[filtered_df[field] >= val_num]
                elif op == "<=": filtered_df = filtered_df[filtered_df[field] <= val_num]
                elif op == "==": filtered_df = filtered_df[filtered_df[field] == val_num]
                # Add other operators if needed
            except Exception as e: print(f"⚠️ Error applying metric filter {field} {op} {val}: {e}")
        else: print(f"⚠️ Metric filter field '{field}' not found.")
    return filtered_df

# --- Helper to print record details (reusing your existing one) ---
def print_record_details(df_to_print, limit=25): # Adjusted default limit
    num_to_print = min(len(df_to_print), limit)
    if num_to_print == 0: print("No records to display."); return
    print("-" * 30)
    header = f"Displaying {'first ' if len(df_to_print) > limit else ''}{num_to_print} of {len(df_to_print)} records:" if limit < len(df_to_print) else "Collection Details:"
    print(header)
    details_df = df_to_print.head(num_to_print).copy()
    if 'dateTimeParsed' in details_df.columns and pd.api.types.is_datetime64_any_dtype(details_df['dateTimeParsed']):
        details_df['displayTimestamp'] = details_df['dateTimeParsed'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ') # Match example
    else: details_df['displayTimestamp'] = details_df.get('dateTimeParsed', 'N/A').astype(str)

    for _, row in details_df.iterrows():
        ts = row.get('displayTimestamp', 'N/A')
        mc = row.get('memberCode', 'N/A')
        print(f"  Member: {mc}, Timestamp: {ts}")
        parts = [f"{col.upper()}: {row.get(col, 0):.2f}" if isinstance(row.get(col), (int, float)) else f"{col.upper()}: {row.get(col, 'N/A')}"
                 for col in ['qty', 'fat', 'snf', 'rtpl', 'amount'] if col in row and pd.notna(row[col])]
        print(f"    {', '.join(parts)}")

# --- Main Bot Execution ---
def run_llm_bot():
    if not data_loaded or milk_df is None or milk_df.empty: print("\nBot cannot run: Data issues."); return
    if not model: print("\nBot cannot run: LLM not initialized."); return

    print("\n" + "="*50 + "\n🥛 Advanced AI Milk Collection Analyst 🥛")
    print("Ask about summaries, trends, comparisons, etc. Type 'quit' to exit.")
    print("="*50 + "\n")

    conversation_context = {
        "last_params": None,
        "last_identified_entities": None # Store a dict like {'type': 'memberCode', 'ids': ['M001', 'M002']}
    }

    while True:
        user_query = input("Ask your question: ").strip()
        if user_query.lower() in ['quit', 'exit']: print("\nExiting..."); break
        if not user_query: continue

        params = get_query_parameters_from_llm(user_query, conversation_context)

        if params.get("intent") in ["error_llm_not_initialized", "error_decoding_json", "error_llm_interaction"]:
            print("An internal error occurred. Please try again."); continue
        if params.get("clarification_needed") and params.get("clarification_question"):
            print(f"🤖 Clarification: {params['clarification_question']}"); continue

        intent = params.get("intent", "unknown")
        print(f"\n--- Results (Intent: {intent}) ---")

        # Contextual resolution for entities (basic)
        entity1_id_value = params.get("entity1_id") # Get the value, which might be None

        is_pronoun_reference = False
        if isinstance(entity1_id_value, str): # Only call .lower() if it's a string
            if entity1_id_value.lower() in ["them", "those", "their"]:
                is_pronoun_reference = True

        if is_pronoun_reference and conversation_context.get("last_identified_entities"):
            params["entity1_type"] = conversation_context["last_identified_entities"]["type"]
            params["entity1_id"] = conversation_context["last_identified_entities"]["ids"] # LLM needs to know if it's list
            print(f"(Interpreted entity1 from context: {params['entity1_id']})")
        
        # --- Intent Handling ---
        if intent == "get_summary_and_details":
            filtered_data = apply_filters_from_params(milk_df, params)
            print(f"Summary for selection ({len(filtered_data)} records):")
            if not filtered_data.empty:
                print(f"  Total Qty: {filtered_data['qty'].sum():.2f}, Avg Qty: {filtered_data['qty'].mean():.2f}")
                if 'amount' in filtered_data: print(f"  Total Amount: {filtered_data['amount'].sum():.2f}")
                # Fix: Ensure limit passed to print_record_details is an integer
                detail_limit = params.get("limit_n")
                print_record_details(filtered_data, limit=detail_limit if isinstance(detail_limit, int) else 10) # Default to 10 if None or not int
                # Update context if a single entity was focused on
                if params.get("entity1_id") and not isinstance(params.get("entity1_id"), list):
                     conversation_context["last_identified_entities"] = {"type": params.get("entity1_type"), "ids": [params.get("entity1_id")]}

            else: print("No data matches criteria for summary.")

        elif intent == "get_top_n_records":
            filtered_data = apply_filters_from_params(milk_df, params) # Apply general filters first
            metric = params.get("sort_by_metric")
            limit = int(params.get("limit_n", 5)) # This already defaults to 5 if limit_n is None/missing, but let's be explicit
            ascending = params.get("sort_order") == "asc"
            # Check that metric is a string before checking columns
            if isinstance(metric, str) and metric in filtered_data.columns:
                sorted_df = filtered_data.sort_values(by=metric, ascending=ascending).head(limit)
                print(f"{'Top' if not ascending else 'Bottom'} {limit} records by {metric}:")
                 # Fix: Ensure limit passed to print_record_details is an integer
                print_record_details(sorted_df, limit=limit if isinstance(limit, int) else 5) # Default to 5 for top/bottom if limit_n somehow wasn't int
                # Update context with these members if sorted by member-relevant metric
                if metric in ['qty', 'amount'] and 'memberCode' in sorted_df.columns:
                     conversation_context["last_identified_entities"] = {"type": "memberCode", "ids": sorted_df['memberCode'].unique().tolist()}
            else: print(f"⚠️ Cannot sort by '{metric}'. Metric not found or is invalid type.")


        elif intent == "get_extreme_metric_value":
            filtered_data = apply_filters_from_params(milk_df, params) # Apply general filters first
            metric = params.get("sort_by_metric")
            # Default to 1 if limit_n is not specified for extreme value
            limit = int(params.get("limit_n", 1)) # This should handle None already due to default
            # Default sort_order to 'desc' (highest) if not specified by LLM
            ascending_order = params.get("sort_order", "desc") == "asc" # True for 'asc' (lowest)

            # Check that metric is a string before checking columns
            if isinstance(metric, str) and metric in filtered_data.columns:
                sorted_df = filtered_data.sort_values(by=metric, ascending=ascending_order)
                result_df = sorted_df.head(limit) # Get top 'limit' records (usually 1 for extreme)

                extreme_type = "Highest" if not ascending_order else "Lowest"
                if limit > 1:
                     print(f"{extreme_type} {limit} value(s) for {metric}:")
                else:
                     print(f"{extreme_type} value for {metric}:")

                # Fix: Ensure limit passed to print_record_details is an integer
                print_record_details(result_df, limit=limit if isinstance(limit, int) else 1) # Default to 1 for extreme if limit_n somehow wasn't int
                if not result_df.empty and limit == 1:
                     print(f"  Value of {metric}: {result_df.iloc[0][metric]}")
                # Update context with these members if sorted by member-relevant metric
                if metric in ['qty', 'amount'] and 'memberCode' in result_df.columns:
                     conversation_context["last_identified_entities"] = {"type": "memberCode", "ids": result_df['memberCode'].unique().tolist()}
            else: print(f"❌ Error: Metric '{metric}' not found or is invalid type.")


        elif intent == "list_unique":
            target_col = params.get("list_unique_target")
            # Check that target_col is a string before checking columns
            if isinstance(target_col, str) and target_col in milk_df.columns:
                filtered_data = apply_filters_from_params(milk_df, params) # Users might ask "list unique members for DCS X"
                unique_vals = filtered_data[target_col].unique()
                print(f"Unique {target_col}s ({len(unique_vals)}): {', '.join(map(str, unique_vals[:30]))}{'...' if len(unique_vals) > 30 else ''}")
                conversation_context["last_identified_entities"] = {"type": target_col, "ids": list(unique_vals)}
            else: print(f"⚠️ Cannot list unique for '{target_col}'. Target not found or is invalid type.")

        elif intent == "count_unique":
            target_col = params.get("count_unique_target")
            # Check that target_col is a string before checking columns
            if isinstance(target_col, str) and target_col in milk_df.columns:
                filtered_data = apply_filters_from_params(milk_df, params)
                print(f"Number of unique {target_col}s: {filtered_data[target_col].nunique()}")
            else: print(f"⚠️ Cannot count unique for '{target_col}'. Target not found or is invalid type.")

        elif intent == "compare_two_entities":
            e1_type, e1_id = params.get("entity1_type"), params.get("entity1_id")
            e2_type, e2_id = params.get("entity2_type"), params.get("entity2_id")
            metric_comp = params.get("metric_to_compare") # e.g., "avg_qty", "total_fat"

            if not (e1_type and e1_id and e2_type and e2_id and metric_comp):
                print("⚠️ Insufficient info for comparison (need two entities and a metric)."); continue

            df_e1 = apply_filters_from_params(milk_df, {"entity1_type": e1_type, "entity1_id": e1_id, "start_date": params.get("start_date"), "end_date": params.get("end_date")})
            df_e2 = apply_filters_from_params(milk_df, {"entity1_type": e2_type, "entity1_id": e2_id, "start_date": params.get("start_date"), "end_date": params.get("end_date")})

            # Ensure metric_comp is a string before splitting
            if not isinstance(metric_comp, str) or '_' not in metric_comp:
                print(f"⚠️ Invalid or unexpected format for metric comparison: {metric_comp}"); continue

            op, metric_field = metric_comp.split('_', 1) # "avg_qty" -> ("avg", "qty")

            val1, val2 = "N/A", "N/A"
            if not df_e1.empty and metric_field in df_e1.columns:
                if op == "avg": val1 = df_e1[metric_field].mean()
                elif op == "total": val1 = df_e1[metric_field].sum()
                elif op == "count": val1 = len(df_e1) # collection_count
            if not df_e2.empty and metric_field in df_e2.columns:
                if op == "avg": val2 = df_e2[metric_field].mean()
                elif op == "total": val2 = df_e2[metric_field].sum()
                elif op == "count": val2 = len(df_e2)

            print(f"Comparison of {metric_comp.replace('_', ' ')}:")
            print(f"  {e1_type} {e1_id}: {val1 if not isinstance(val1, float) else f'{val1:.2f}'}")
            print(f"  {e2_type} {e2_id}: {val2 if not isinstance(val2, float) else f'{val2:.2f}'}")
            if isinstance(val1, (int,float)) and isinstance(val2, (int,float)):
                if val1 > val2: print(f"  {e1_id} performed better/higher.")
                elif val2 > val1: print(f"  {e2_id} performed better/higher.")
                else: print("  Performances are equal.")
            conversation_context["last_identified_entities"] = {"type": e1_type, "ids": [e1_id, e2_id]}


        elif intent == "compare_entity_over_two_periods":
            e_type, e_id = params.get("entity1_type"), params.get("entity1_id")
            metric_comp = params.get("metric_to_compare")
            p1_start, p1_end = params.get("period1_start_date"), params.get("period1_end_date")
            p2_start, p2_end = params.get("period2_start_date"), params.get("period2_end_date")

            if not (e_type and e_id and metric_comp and p1_start and p1_end and p2_start and p2_end):
                print("⚠️ Insufficient info for period comparison."); continue

            df_p1 = apply_filters_from_params(milk_df, {"entity1_type": e_type, "entity1_id": e_id, "start_date": p1_start, "end_date": p1_end})
            df_p2 = apply_filters_from_params(milk_df, {"entity1_type": e_type, "entity1_id": e_id, "start_date": p2_start, "end_date": p2_end})

            # Ensure metric_comp is a string before splitting
            if not isinstance(metric_comp, str) or '_' not in metric_comp:
                 print(f"⚠️ Invalid or unexpected format for metric comparison: {metric_comp}"); continue

            op, metric_field = metric_comp.split('_', 1)
            val1, val2 = "N/A", "N/A"

            # Check if metric_field exists in the DataFrame before attempting calculation
            if metric_field not in milk_df.columns:
                print(f"⚠️ Metric field '{metric_field}' not found in data for comparison."); continue

            if not df_p1.empty:
                if op == "avg": val1 = df_p1[metric_field].mean()
                elif op == "total": val1 = df_p1[metric_field].sum()
            if not df_p2.empty:
                if op == "avg": val2 = df_p2[metric_field].mean()
                elif op == "total": val2 = df_p2[metric_field].sum()

            # Prepare values for printing outside the format specifier
            val1_display = f"{val1:.2f}" if isinstance(val1, float) else val1
            val2_display = f"{val2:.2f}" if isinstance(val2, float) else val2


            print(f"Comparison of {metric_comp.replace('_',' ')} for {e_type} {e_id}:")
            # Use the prepared display variables in the f-string
            print(f"  Period 1 ({p1_start} to {p1_end}): {val1_display}")
            print(f"  Period 2 ({p2_start} to {p2_end}): {val2_display}")
            conversation_context["last_identified_entities"] = {"type": e_type, "ids": [e_id]}


        elif intent == "get_trend":
            trend_df = apply_filters_from_params(milk_df, params) # Filters by entity1, start/end date
            metric = params.get("trend_metric")
            agg = params.get("trend_aggregation") # sum or mean
            period = params.get("trend_resample_period") # D, W, M, Q, Y

            # Check parameters before using them in the condition
            if not (isinstance(metric, str) and isinstance(agg, str) and isinstance(period, str) and metric in trend_df.columns and 'dateTimeParsed' in trend_df.columns):
                print("⚠️ Insufficient info or invalid metric/aggregation/period for trend.");
                print(f"  Params received: metric={metric}, agg={agg}, period={period}")
                if isinstance(metric, str):
                     print(f"  Metric '{metric}' in columns: {metric in trend_df.columns}")
                print(f"  'dateTimeParsed' in columns: {'dateTimeParsed' in trend_df.columns}")
                continue


            if trend_df.empty: print("No data for the selected criteria to show trend."); continue

            trend_data = trend_df.set_index('dateTimeParsed').resample(period)[metric]
            if agg == "sum": trend_data = trend_data.sum()
            elif agg == "mean": trend_data = trend_data.mean()
            else: print(f"⚠️ Unknown trend aggregation: {agg}"); continue

            print(f"Trend for {agg} of {metric} (resampled by {period}):")
            if not trend_data.empty: print(trend_data.to_string())
            else: print("No trend data to display after resampling.")
            

        elif intent == "rank_members" and params.get("rank_members_metric") and params.get("limit_n"):
            rank_metric_key = params["rank_members_metric"]
            limit = int(params.get("limit_n", 5)) # Default limit if not specified
            ascending_order = params.get("sort_order") == "asc"

            # Apply date/general filters before ranking
            filtered_for_ranking = apply_filters_from_params(milk_df, {"start_date": params.get("start_date"), "end_date": params.get("end_date")})

            if 'memberCode' not in filtered_for_ranking.columns:
                print("❌ Error: 'memberCode' column not found for ranking members.")
            else:
                grouped = filtered_for_ranking.groupby('memberCode')
                ranked_series = None
                # Ensure rank_metric_key is a string before replacing
                metric_name_display = rank_metric_key.replace("_", " ").title() if isinstance(rank_metric_key, str) else str(rank_metric_key)

                if rank_metric_key == "total_qty": ranked_series = grouped['qty'].sum()
                elif rank_metric_key == "avg_qty": ranked_series = grouped['qty'].mean()
                elif rank_metric_key == "total_amount": ranked_series = grouped['amount'].sum() if 'amount' in filtered_for_ranking else None
                elif rank_metric_key == "avg_amount": ranked_series = grouped['amount'].mean() if 'amount' in filtered_for_ranking else None
                elif rank_metric_key == "collection_count": ranked_series = grouped.size()
                else: print(f"⚠️ Unknown ranking metric: {rank_metric_key}")

                if ranked_series is not None:
                    ranked_df = ranked_series.reset_index(name='metric_value')
                    # Ensure 'metric_value' is numeric before sorting
                    ranked_df['metric_value'] = pd.to_numeric(ranked_df['metric_value'], errors='coerce')
                    ranked_df = ranked_df.dropna(subset=['metric_value']) # Drop rows where conversion failed
                    ranked_df = ranked_df.sort_values(by='metric_value', ascending=ascending_order).head(limit)

                    print(f"{('Top' if not ascending_order else 'Bottom')} {len(ranked_df)} Members by {metric_name_display}:") # Use actual length after head
                    if not ranked_df.empty:
                         for _, row in ranked_df.iterrows():
                            print(f"  Member: {row['memberCode']}, {metric_name_display}: {row['metric_value']:.2f}")
                         conversation_context["last_identified_entities"] = {"type": "memberCode", "ids": ranked_df['memberCode'].tolist()}
                    else:
                         print("No members found matching criteria for ranking.")

                elif rank_metric_key in ["total_amount", "avg_amount"] and 'amount' not in filtered_for_ranking:
                     print(f"⚠️ Cannot rank by {metric_name_display} as 'amount' data is not available in the current selection or data.")


        elif intent == "unknown" or intent is None:
            print("I'm not sure how to answer that. Could you rephrase or try a different type of query?")

        else: # Catch any other intents defined in schema but not yet handled
             print(f"⚠️ Intent '{intent}' is recognized but not fully implemented yet.")

        # Update context
        conversation_context["last_params"] = params.copy()
        
        if intent not in ["get_summary_and_details", "get_top_n_records", "get_extreme_metric_value",
                           "list_unique", "count_unique", "compare_two_entities",
                           "compare_entity_over_two_periods", "rank_members"]:
             conversation_context["last_identified_entities"] = None # Clear if query was general or different type

        print("\n" + "="*50 + "\n")


if __name__ == "__main__":
    # --- Dummy Model and Data for testing if real ones fail to load ---
    class DummyModel:
        def __init__(self, name="dummy-model"): self.model_name = name
        def generate_content(self, prompt_text):
            
            return DummyResponse('{"intent": "get_trend", "trend_metric": "qty", "trend_aggregation": null, "trend_resample_period": "M", "start_date": "2024-11-01", "end_date": "2024-12-31"}')


    class DummyResponse:
        def __init__(self, text): self.text = text

    # Check if the real model initialized correctly, if not, use dummy
    if 'model' not in locals() or model is None or not isinstance(model, genai.GenerativeModel):
        print("⚠️ Real Gemini Model failed to initialize or was not available. Using DUMMY model for testing structure.")
        model = DummyModel()

    # Check if real data loaded correctly, if not, use dummy
    if 'milk_df' not in locals() or milk_df is None or milk_df.empty:
        print("⚠️ Real data loading failed or DataFrame is empty. Using DUMMY data for testing structure.")
        sample_data = []
        start_dt_dummy = datetime(2024, 11, 1) # For trend/comparison testing
        members = ["0010000019930006", "0010000008650047", "M003", "M004", "M005", "M006"] # Added more members for ranking
        dcs = ["DCS1", "DCS2", "DCS3"] # Added more DCS
        for i in range(200): # ~3+ months of data
            member_id = members[i % len(members)]
            dcs_id = dcs[i % len(dcs)]
            coll_date = start_dt_dummy + timedelta(days=i, hours=np.random.choice([6,18]))
            qty = np.random.uniform(5, 15) * (1 + np.sin(i/30)*0.5) # Add some variation
            fat = np.random.uniform(3.5, 4.5)
            snf = np.random.uniform(8.2, 8.8)
            amount = qty * (30 + fat*np.random.uniform(0.5, 1.0) + snf*np.random.uniform(0.2, 0.5)) # Simulate amount calculation
            sample_data.append({
                "memberCode": member_id, "dcsCode": dcs_id, "dateTimeParsed": coll_date,
                "qty": round(qty,2), "fat": round(fat,2), "snf": round(snf,2),
                "rtpl": round(qty * fat * 0.8, 2), # Example calculation
                "amount": round(amount ,2)
            })
        milk_df = pd.DataFrame(sample_data)
        milk_df['dateTimeParsed'] = pd.to_datetime(milk_df['dateTimeParsed'])
        milk_df['qty'] = pd.to_numeric(milk_df['qty'], errors='coerce').fillna(0)
        milk_df['fat'] = pd.to_numeric(milk_df['fat'], errors='coerce')
        milk_df['snf'] = pd.to_numeric(milk_df['snf'], errors='coerce')
        milk_df['rtpl'] = pd.to_numeric(milk_df['rtpl'], errors='coerce')
        milk_df['amount'] = pd.to_numeric(milk_df['amount'], errors='coerce').fillna(0)
        milk_df['memberCode'] = milk_df['memberCode'].astype(str)
        milk_df['dcsCode'] = milk_df['dcsCode'].astype(str)
        data_loaded = True
    # --- End Dummy Data ---


    if model and data_loaded and milk_df is not None and not milk_df.empty:
        print(f"\nBot starting with Model: {model.model_name if hasattr(model, 'model_name') else 'N/A'} and Data Shape: {milk_df.shape}\n")
        run_llm_bot()
    else:
        print("\nBot cannot start. Check API key, model selection, and data file.")
