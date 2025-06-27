# --- START OF FILE v.py ---

import pymongo
import google.generativeai as genai
import json
from datetime import datetime
import re
import ast

class EnhancedMongoDBBot:
    def __init__(self, gemini_api_key, mongodb_connection_string):
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash-latest')

        try:
            self.client = pymongo.MongoClient(mongodb_connection_string)
            self.db = self.client['lactalis_db']
            self.collection = self.db['milk_collections']
            self.client.admin.command('ping')
            print("✅ MongoDB connection successful.")
        except pymongo.errors.ConnectionFailure as e:
            print(f" MongoDB Connection Error: {e}")
            raise
        except Exception as e:
            print(f" An unexpected error during MongoDB initialization: {e}")
            raise

        self.schema_info = self._get_collection_schema()
        # Note: field_mappings are not used directly, but are for context in the prompt.
        self.field_mappings = {
            'member': 'memberCode', 'dcs': 'dcsCode', 'quantity': 'qty',
            'fat': 'fat', 'snf': 'snf', 'date': 'dateTimeOfCollection',
            'plant': 'plantCode', 'union': 'unionCode'
        }

    def _get_collection_schema(self):
        try:
            sample_doc = self.collection.find_one()
            if sample_doc:
                if '_id' in sample_doc:
                    del sample_doc['_id']
                # Convert datetime objects to string 'datetime' for the prompt
                for k, v in sample_doc.items():
                    if isinstance(v, datetime):
                        sample_doc[k] = 'datetime'
                    else:
                        sample_doc[k] = type(v).__name__
                return sample_doc
            return {}
        except Exception as e:
            print(f" Error getting schema: {e}")
            return {}

    def _natural_language_to_query(self, user_question):
        # This prompt is heavily revised to be more robust and provide better examples.
        prompt = """
You are an expert MongoDB query generator. Your ONLY task is to convert a user's natural language question into a single, executable PyMongo query string.

**DATABASE SCHEMA**
- Collection Name: `milk_collections`
- Schema for your reference:
%s

**KEY FIELDS & ALIASES**
- `memberCode` (string): member, member code
- `dcsCode` (string): dcs, dcs code
- `qty` (float/int): quantity, qty, milk, milk quantity
- `fat` (float): fat
- `snf` (float): snf
- `dateTimeOfCollection` (datetime): date, datetime, collection date, time

**OUTPUT RULES**
1.  **OUTPUT ONLY THE QUERY STRING.** Start your response IMMEDIATELY with `find({...})`, `aggregate([...])`, `distinct("field", {...})`, or `count_documents({...})`.
2.  **NO EXTRA TEXT.** Do not include explanations, comments, markdown, or language names (like "python" or "json").
3.  **USE CORRECT FIELD NAMES.** Use the schema field names (`memberCode`, `qty`), not user aliases (`member`, `quantity`).
4.  **CORRECT TYPES.** Numbers should be numbers (`4.1`), codes and other non-numeric IDs must be strings (`"730110400002"`).
5.  **VALID SYNTAX.** All keys and string values must be in double quotes.
6.  **DATE HANDLING:**
    - Always use `ISODate`. A specific date like "on 01/01/2025" becomes a full-day range: `{"dateTimeOfCollection": {"$gte": ISODate("2025-01-01T00:00:00Z"), "$lt": ISODate("2025-01-02T00:00:00Z")}}`.
    - A date range like "between 01/01/2025 and 03/01/2025" becomes `{"$gte": ISODate("2025-01-01T00:00:00Z"), "$lt": ISODate("2025-03-02T00:00:00Z")}`.
7.  **AGGREGATION LOGIC:**
    - **Top/Bottom N:** Use `$sort` and `$limit`. For "top", sort descending (`-1`). For "bottom/lowest/least", sort ascending (`1`).
    - **Grouped Operations:** Use `$group` with accumulators like `$sum`, `$avg`, `$min`, `$max`. For a global average/sum (not grouped by a field), use `"_id": None`.
    - **Trends:** Group by time unit (e.g., `{"month": {"$month": "$dateTimeOfCollection"}}`) and `$sort` by it.
    - **Comparisons:** For comparing entities (e.g., member A vs member B), use `$match` with `{"$in": ["A", "B"]}` and then `$group` by the entity field.
8.  **AVOID INVALID QUERIES:** Never start a filter with an operator.
    - WRONG: `find({"$gt": 4.1})`
    - RIGHT: `find({"fat": {"$gt": 4.1}})`

**USER QUESTION:** "%s"

---
**EXAMPLES**
- User: "Show me collections for member 730110400002"
  -> `find({"memberCode": "730110400002"})`
- User: "Count records for dcs 001000001993 for Jan 2025"
  -> `count_documents({"dcsCode": "001000001993", "dateTimeOfCollection": {"$gte": ISODate("2025-01-01T00:00:00Z"), "$lt": ISODate("2025-02-01T00:00:00Z")}})`
- User: "Find records with fat > 4.1 and snf > 8.5 for member 0010000019930016 in january 2025"
  -> `find({"memberCode": "0010000019930016", "fat": {"$gt": 4.1}, "snf": {"$gt": 8.5}, "dateTimeOfCollection": {"$gte": ISODate("2025-01-01T00:00:00Z"), "$lt": ISODate("2025-02-01T00:00:00Z")}})`
- User: "List 10 unique member codes with qty over 100"
  -> `distinct("memberCode", {"qty": {"$gt": 100}})`
- User: "What is the lowest quantity collected on 09/11/2024?"
  -> `aggregate([{"$match": {"dateTimeOfCollection": {"$gte": ISODate("2024-11-09T00:00:00Z"), "$lt": ISODate("2024-11-10T00:00:00Z")}}}, {"$sort": {"qty": 1}}, {"$limit": 1}])`
- User: "Minimum SNF for DCS 001000001993"
  -> `aggregate([{"$match": {"dcsCode": "001000001993"}}, {"$group": {"_id": "$dcsCode", "minSNF": {"$min": "$snf"}}}])`
- User: "Top 5 members by total quantity"
  -> `aggregate([{"$group": {"_id": "$memberCode", "totalQty": {"$sum": "$qty"}}}, {"$sort": {"totalQty": -1}}, {"$limit": 5}])`
- User: "Which member has the highest average SNF?"
  -> `aggregate([{"$group": {"_id": "$memberCode", "avgSNF": {"$avg": "$snf"}}}, {"$sort": {"avgSNF": -1}}, {"$limit": 1}])`
- User: "What is the average quantity per collection overall?"
  -> `aggregate([{"$group": {"_id": null, "avgQty": {"$avg": "$qty"}}}])`
- User: "Trend of average fat per month in 2024"
  -> `aggregate([{"$match": {"dateTimeOfCollection": {"$gte": ISODate("2024-01-01T00:00:00Z"), "$lt": ISODate("2025-01-01T00:00:00Z")}}}, {"$group": {"_id": {"month": {"$month": "$dateTimeOfCollection"}}, "avgFat": {"$avg": "$fat"}}}, {"$sort": {"_id.month": 1}}])`
- User: "Compare average qty for member 0010000008650047 and member 760540500086"
  -> `aggregate([{"$match": {"memberCode": {"$in": ["0010000008650047", "760540500086"]}}}, {"$group": {"_id": "$memberCode", "avgQty": {"$avg": "$qty"}}}])`
- User: "Compare average quantity for member 0010000008650047 in November and December 2024"
  -> `aggregate([{"$match": {"memberCode": "0010000008650047", "dateTimeOfCollection": {"$gte": ISODate("2024-11-01T00:00:00Z"), "$lt": ISODate("2025-01-01T00:00:00Z")}}}, {"$group": {"_id": {"month": {"$month": "$dateTimeOfCollection"}}, "avgQty": {"$avg": "$qty"}}}, {"$sort": {"_id.month": 1}}])`
- User: "Compare total quantity for DCS 001000001993 and 001000002000 in 2024"
  -> `aggregate([{"$match": {"dcsCode": {"$in": ["001000001993", "001000002000"]}, "dateTimeOfCollection": {"$gte": ISODate("2024-01-01T00:00:00Z"), "$lt": ISODate("2025-01-01T00:00:00Z")}}}, {"$group": {"_id": "$dcsCode", "totalQty": {"$sum": "$qty"}}}])`
""" % (json.dumps(self.schema_info, indent=2), user_question)
        try:
            response = self.model.generate_content(prompt)
            query_text = response.text.strip()
            # Clean up potential markdown code blocks, just in case
            query_text = re.sub(r"^(```(json|mongodb|python)?\n?)", "", query_text)
            query_text = re.sub(r"```$", "", query_text)
            return query_text.strip()
        except Exception as e:
            print(f"Error generating query with LLM: {e}")
            return f"error: LLM query generation failed: {e}"

    def _safe_eval(self, query_string: str):
        """
        Safely evaluates a string containing a Python expression (like a dict or list),
        primarily for converting ISODate strings to datetime objects.
        """
        if not isinstance(query_string, str):
            raise ValueError(f"Input to _safe_eval must be a string, got {type(query_string)}")

        query_string = query_string.strip()
        if not query_string:
            return {} # An empty query string means an empty filter

        # Replace JavaScript-style null with Python None
        query_string = re.sub(r'\bnull\b', 'None', query_string, flags=re.IGNORECASE)

        # 1. Replace MongoDB-specific syntax with Python-compatible equivalents.
        # Handle ISODate("...") by replacing it with a Python datetime constructor call
        def iso_to_datetime_str(match):
            iso_str = match.group(1).replace('Z', '+00:00')
            if 'T' not in iso_str: # Handle date-only strings
                iso_str += 'T00:00:00+00:00'
            return f'datetime.fromisoformat("{iso_str}")'

        processed_string = re.sub(r'ISODate\("([^"]+)"\)', iso_to_datetime_str, query_string)

        # 2. Use a controlled `eval` to parse the string. This is necessary because
        # `ast.literal_eval` does not support function calls like `datetime.fromisoformat`.
        # The scope is heavily restricted to prevent security issues.
        try:
            allowed_globals = {"__builtins__": {}}
            allowed_locals = {
                "datetime": datetime,
                "None": None,
                "True": True,
                "False": False
            }
            return eval(processed_string, allowed_globals, allowed_locals)
        except (SyntaxError, NameError, TypeError, Exception) as e:
            raise ValueError(f"Could not parse query. Generated string: '{query_string}'. Error: {e}")

    def _has_invalid_top_level_operator(self, query_dict):
        """Checks for invalid top-level operators like {"$gt": 4}, which is a common LLM error."""
        if isinstance(query_dict, dict):
            for key in query_dict:
                if key.startswith('$'):
                    return True
        return False

    def _execute_mongodb_query(self, query_text):
        if query_text.startswith("error:"):
            return {"error": query_text}
        
        try:
            if query_text.startswith('find('):
                # Robustly extract the arguments inside find(...)
                match = re.match(r'find\((.*)\)$', query_text.strip(), re.DOTALL)
                if not match:
                    return {"error": f"Invalid find query format: {query_text}"}
                args_str = match.group(1).strip()
                # Support both find(filter) and find(filter, projection)
                if args_str:
                    # Split on first comma not inside braces or brackets
                    depth = 0
                    split_idx = None
                    for i, c in enumerate(args_str):
                        if c in '{[':
                            depth += 1
                        elif c in '}]':
                            depth -= 1
                        elif c == ',' and depth == 0:
                            split_idx = i
                            break
                    if split_idx is not None:
                        filter_str = args_str[:split_idx].strip()
                        projection_str = args_str[split_idx+1:].strip()
                    else:
                        filter_str = args_str
                        projection_str = None
                else:
                    filter_str = "{}"
                    projection_str = None
                # Evaluate filter
                filter_query = self._safe_eval(filter_str)
                if self._has_invalid_top_level_operator(filter_query):
                    return {"error": "Invalid query: a top-level key cannot be a MongoDB operator (like $gt, $lt). Please rephrase your question to specify a field."}
                # Evaluate projection if present
                if projection_str:
                    # Remove trailing .limit(...) if present
                    projection_str = re.sub(r'\)\.limit\(\d+\)\s*$', '', projection_str)
                    projection_str = re.sub(r'\.limit\(\d+\)\s*$', '', projection_str)
                    try:
                        projection = self._safe_eval(projection_str)
                    except Exception as e:
                        return {"error": f"Projection parsing error: {e}"}
                    cursor = self.collection.find(filter_query, projection)
                else:
                    cursor = self.collection.find(filter_query)
                # Support dynamic limit if specified in the query, otherwise return all matching records
                match_limit = re.search(r'\.limit\((\d+)\)', query_text)
                if match_limit:
                    limit = int(match_limit.group(1))
                    results = list(cursor.limit(limit))
                    total_count = self.collection.count_documents(filter_query)
                    return {"type": "find", "results": results, "count": len(results), "total_count": total_count}
                else:
                    results = list(cursor)
                    return {"type": "find", "results": results, "count": len(results)}

            elif query_text.startswith('aggregate('):
                match = re.match(r'aggregate\((.*)\)$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid aggregate query format: {query_text}"}
                
                pipeline = self._safe_eval(match.group(1).strip())
                if not isinstance(pipeline, list):
                    return {"error": f"Aggregation pipeline must be a list, but got {type(pipeline)} from '{query_text}'"}
                
                # Validate the $match stage if it exists
                if pipeline and pipeline[0].get('$match'):
                    if self._has_invalid_top_level_operator(pipeline[0]['$match']):
                         return {"error": "Invalid aggregation: $match stage has a top-level MongoDB operator (like $gt, $lt). Please rephrase your question to specify a field."}

                results = list(self.collection.aggregate(pipeline))
                return {"type": "aggregate", "results": results, "count": len(results)}

            elif query_text.startswith('distinct('):
                match = re.match(r'distinct\("([^"]+)"(?:,\s*(.*))?\)$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid distinct query format: {query_text}"}
                
                field_name = match.group(1)
                filter_str = match.group(2).strip() if match.group(2) else "{}"
                filter_query = self._safe_eval(filter_str)

                if self._has_invalid_top_level_operator(filter_query):
                    return {"error": "Invalid query: a top-level key cannot be a MongoDB operator (like $gt, $lt). Please rephrase your question to specify a field."}
                
                results = self.collection.distinct(field_name, filter_query)
                return {"type": "distinct", "field": field_name, "results": results, "count": len(results)}

            elif query_text.startswith('count_documents('):
                match = re.match(r'count_documents\((.*)\)$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid count_documents query format: {query_text}"}
                
                filter_str = match.group(1).strip() if match.group(1) else "{}"
                filter_query = self._safe_eval(filter_str)

                if self._has_invalid_top_level_operator(filter_query):
                    return {"error": "Invalid query: a top-level key cannot be a MongoDB operator (like $gt, $lt). Please rephrase your question to specify a field."}

                count = self.collection.count_documents(filter_query)
                return {"type": "count", "results": count}

            else:
                return {"error": f"Unsupported query format. Query must start with find(...), aggregate(...), distinct(...), or count_documents(...). Received: '{query_text}'"}

        except ValueError as ve:
             return {"error": f"Query Parsing Error: {str(ve)}"}
        except pymongo.errors.OperationFailure as oe:
            return {"error": f"MongoDB Operation Failure: {str(oe)}"}
        except Exception as e:
            return {"error": f"An unexpected error occurred during query execution: {str(e)} on query: '{query_text}'"}

    def _format_results_to_natural_language(self, query_results_dict, sample_size=10):
        results_data = query_results_dict.get('results')
        query_type = query_results_dict.get('type', 'unknown')
        count = query_results_dict.get('count', 0)

        if query_type == "count":
            return f"Found {results_data} matching records."

        if not results_data:
            return "No matching records found."

        if query_type == "distinct":
            output = f"Found {count} unique values for field '{query_results_dict.get('field')}'. "
            if count > sample_size:
                output += f"Showing first {sample_size}:\n"
                results_to_show = results_data[:sample_size]
            else:
                output += "\n"
                results_to_show = results_data
            return output + json.dumps(results_to_show, indent=2, default=str)

        if isinstance(results_data, list):
            # Show both the number of records shown and the total matching records if available
            total_count = query_results_dict.get('total_count')
            if total_count is not None and total_count > count:
                output = f"Found {count} records out of {total_count} matching records. "
            else:
                output = f"Found {count} records. "
            if count > sample_size:
                output += f"Showing first {sample_size}:\n"
                results_to_show = results_data[:sample_size]
            else:
                output += "\n"
                results_to_show = results_data
            
            # Clean up ObjectId for display
            for doc in results_to_show:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])

            return output + json.dumps(results_to_show, indent=2, default=str)

        # Fallback for any other structure
        return json.dumps(results_data, indent=2, default=str)

    def _validate_llm_query(self, query_text):
        if not isinstance(query_text, str): return False
        valid_starts = ['find(', 'aggregate(', 'distinct(', 'count_documents(']
        return any(query_text.strip().startswith(v) for v in valid_starts)

    def ask_question(self, user_question: str):
        print(f"\n👤 User question: \"{user_question}\"")
        
        # Determine sample size from question, default to 10
        sample_size = 10
        match = re.search(r'\b(top|first|last|show|list)\s+(\d+)\b', user_question, re.IGNORECASE)
        if match:
            try:
                sample_size = int(match.group(2))
            except (ValueError, IndexError):
                sample_size = 10
        
        print("🔄 Generating MongoDB query via LLM...")
        mongo_query_str = self._natural_language_to_query(user_question)
        print(f" LLM generated query: {mongo_query_str}")

        if not self._validate_llm_query(mongo_query_str):
            print(" LLM did not return a valid MongoDB query format.")
            return f"Sorry, I could not generate a valid query. The LLM returned:\n{mongo_query_str}"

        print("Executing MongoDB query...")
        query_results_dict = self._execute_mongodb_query(mongo_query_str)

        if "error" in query_results_dict:
            return f" Query Execution Failed: {query_results_dict['error']}"

        print(" Formatting response...")
        return self._format_results_to_natural_language(query_results_dict, sample_size=sample_size)

    def get_sample_questions(self):
        return [
            "Show me 5 collections for member 730110400002",
            "What are the details for dcsCode 001000001993 between 01/01/2025 and 03/01/2025?",
            "Find records where fat is greater than 4.1 and limit to 3 results",
            "List top 3 members by total quantity",
            "Count records for member 730110400002",
            "List 5 unique dcs codes where fat is over 4.0",
            "Which member has the highest average SNF?",
            "Compare average quantity for member 0010000008650047 and 760540500086",
            "What is the trend of average fat per month in 2024?",
            "What is the lowest quantity collected on 09-11-2024?"
        ]

def main():
    print(" Welcome to MongoDB Bot ")
    try:
        # --- IMPORTANT ---
        # Replace these placeholders with your actual credentials.
        # It's recommended to use environment variables for security.
        GEMINI_API_KEY = "xyz"  # <--- REPLACE THIS
        MONGODB_CONNECTION_STRING = "xyz"  # <--- REPLACE THIS

        if GEMINI_API_KEY.startswith("YOUR_") or MONGODB_CONNECTION_STRING.startswith("YOUR_"):
            print("\n🚨 CRITICAL SETUP REQUIRED 🚨")
            print("Please open the v.py script and replace the placeholder values for")
            print("GEMINI_API_KEY and MONGODB_CONNECTION_STRING in the main() function.")
            return

        bot = EnhancedMongoDBBot(GEMINI_API_KEY, MONGODB_CONNECTION_STRING)
        print("\n Enhanced MongoDB Bot is ready!")
        print("Type 'samples' to see example questions, or 'quit' to exit.\n")

        while True:
            user_input = input(" Your question: ").strip()
            if user_input.lower() in ['quit', 'exit', 'bye']:
                print("👋 Goodbye!")
                break
            if user_input.lower() == 'samples':
                print("\n--- Sample Questions You Can Ask ---")
                for i, question in enumerate(bot.get_sample_questions(), 1):
                    print(f"{i}. {question}")
                print("-------------------------------------\n")
                continue
            if not user_input:
                continue

            print("\n" + "="*70)
            response = bot.ask_question(user_input)
            print(f"\n Answer:\n{response}")
            print("="*70 + "\n")
    except Exception as e:
        print(f"\n An unexpected error occurred in main: {e}")

if __name__ == "__main__":
    main()