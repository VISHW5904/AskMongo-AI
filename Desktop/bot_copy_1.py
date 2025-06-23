import pymongo
import google.generativeai as genai
import json
from datetime import datetime
import re  # Ensure re is imported at the top level
import ast # For literal_eval

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
        self.field_mappings = {
            'member': 'memberCode', 'membercode': 'memberCode', 'member code': 'memberCode', 'members': 'memberCode', 'member codes': 'memberCode', 'membercodes': 'memberCode',
            'dcs': 'dcsCode', 'dcscode': 'dcsCode', 'dcs code': 'dcsCode',
            'quantity': 'qty', 'fat': 'fat', 'amount': 'amount', 'amt': 'amount',
            'milk': 'qty', 'qty': 'qty', 'milk quantity': 'qty', 'milk qty': 'qty',
            'snf': 'snf', 'date': 'dateTimeOfCollection', 'datetime': 'dateTimeOfCollection',
            'collection_date': 'dateTimeOfCollection', 'plant': 'plantCode',
            'plantcode': 'plantCode', 'union': 'unionCode', 'unioncode': 'unionCode'
        }

    def _get_collection_schema(self):
        try:
            sample_doc = self.collection.find_one()
            if sample_doc:
                if '_id' in sample_doc:
                    del sample_doc['_id']
                return {k: type(v).__name__ for k, v in sample_doc.items()}
            return {}
        except Exception as e:
            print(f" Error getting schema: {e}")
            return {}

    def _natural_language_to_query(self, user_question):
        date_matches = re.findall(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{4}[/-]\d{1,2}[/-]\d{1,2}', user_question)
        code_matches = re.findall(r'\b\d{8,}\b', user_question)
        number_matches = re.findall(r'\b\d+\.?\d*\b', user_question)

        prompt = f"""
You are an expert MongoDB query generator for a milk collection database.
Your goal is to convert the user's natural language question into a MongoDB query string
that can be executed by PyMongo's `collection.find()` or `collection.aggregate()` methods.

Collection Name: milk_collections
Sample Document Structure (field: type):
{json.dumps(self.schema_info, indent=2)}

Key Fields (and common aliases from user questions):
- "memberCode" (string): member, member code
- "dcsCode" (string): dcs, dcs code
- "qty" (float/int): quantity, qty, milk
- "fat" (float): fat
- "snf" (float): snf
- "dateTimeOfCollection" (datetime): date, datetime, collection date.
  Dates in queries should be ISODate objects, e.g., {{"$gte": ISODate("YYYY-MM-DDTHH:MM:SSZ")}}.
  If only a date is given (e.g., "on 01/01/2025"), interpret it as the full day:
  {{"$gte": ISODate("2025-01-01T00:00:00Z"), "$lt": ISODate("2025-01-02T00:00:00Z")}}

User Question: "{user_question}"

Query Generation Rules:
1.  For simple filters output a find query. Example: `find({{"memberCode": "123", "fat": {{"$gt": 3.5}}}})`.
2.  For aggregations (sum, average, count, group by, top N), output an aggregation pipeline. Example: `aggregate([{{"$match": {{"fat": {{"$gt": 3.5}}}}}}, {{"$group": {{"_id": "$memberCode", "avgFat": {{"$avg": "$fat"}}}}}}])`.
3.  Date Handling:
    - If a specific date like "01/01/2025" is mentioned, filter for the entire day.
    - For date ranges like "between 01/01/2025 and 03/01/2025", use `$gte` for the start date and `$lt` for the day *after* the end date.
    - Convert all date strings to ISODate format, e.g., ISODate("YYYY-MM-DDTHH:MM:SSZ").
4.  Comparisons: For "greater than", use `$gt`. For "less than", use `$lt`.
5.  Top/Bottom N: Use `$sort` and `$limit` in an aggregation pipeline. Ensure the field for sorting is correct (e.g., "$qty" for quantity, or a summed field like "$totalQuantity" if grouped).
6.  Averages: Use `$avg` in a `$group` stage.
7.  Counts: For specific counts, use `count_documents({{filter}})`. For grouped counts, use `$sum: 1` in `$group`.
8.  Distinct values: use `distinct("fieldName", {{filter}})`.
9.  Handle numbers as numbers in queries, not strings, unless they are codes.
10. Ensure all MongoDB operators (like $match, $group, $gte, $sum, $avg, $sort, $limit) and field names (like memberCode, qty) are enclosed in double quotes.

Output ONLY the MongoDB query method name and its arguments. Examples:
`find({{"memberCode": "12345"}})`
`aggregate([{{"$match": {{"memberCode": "12345"}}}}, {{"$group": {{"_id": null, "totalQuantity": {{"$sum": "$qty"}}}}}}])`
`distinct("memberCode", {{"fat": {{"$gt": 4.0}}}}) `
`count_documents({{"memberCode": "12345"}}) `

Query:
"""
        try:
            response = self.model.generate_content(prompt)
            query_text = response.text.strip()
            query_text = query_text.replace('```mongodb', '').replace('```json', '').replace('```', '').strip()
            return query_text.strip()
        except Exception as e:
            print(f"Error generating query with LLM: {e}")
            return f"error: LLM query generation failed: {e}"

    def _safe_eval(self, query_string):
        if not isinstance(query_string, str):
            if isinstance(query_string, (dict, list)):
                return query_string
            raise ValueError(f"Input to _safe_eval must be a string, dict, or list, got {type(query_string)}")

        query_string = query_string.strip()
        # Fix: Convert all JSON 'null' to Python 'None' for eval
        query_string = re.sub(r':\s*null', ': None', query_string)
        # This complex regex handling is crucial for converting LLM's date strings into Python datetime objects
        query_string = re.sub(r'ISODate\("(\d{4}-\d{2}-\d{2})"\)',
                              lambda m: f'datetime.fromisoformat("{m.group(1)}T00:00:00+00:00")',
                              query_string)
        query_string = re.sub(r'ISODate\("(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)(?!\+00:00|Z)"\)',
                              lambda m: f'datetime.fromisoformat("{m.group(1)}+00:00")',
                              query_string)
        query_string = re.sub(r'ISODate\("([^"]+)"\)',
                            lambda m: f'datetime.fromisoformat("{m.group(1).replace("Z", "+00:00")}")',
                            query_string)
        try:
            # ast.literal_eval is safer than eval, but can't handle function calls like datetime()
            return ast.literal_eval(query_string)
        except (ValueError, SyntaxError, TypeError):
            try:
                # Use eval only as a fallback for datetime objects, with a restricted scope
                return eval(query_string, {"__builtins__": {}}, {"datetime": datetime, "True": True, "False": False, "None": None})
            except Exception as e_eval:
                raise ValueError(f"Query parsing error with eval: {e_eval} on string: {query_string}")

    def _execute_mongodb_query(self, query_text):
        if query_text.startswith("error:"):
            return {"error": query_text}
        try:
            if query_text.startswith('find('):
                match = re.match(r'find\((.*?)\)(?:\.limit\((\d+)\))?$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid find query format: {query_text}"}
                query_params_str = match.group(1).strip()
                limit = int(match.group(2)) if match.group(2) else 50
                filter_query = self._safe_eval(query_params_str) if query_params_str else {}
                results = list(self.collection.find(filter_query).limit(limit))
                return {"type": "find", "results": results, "count": len(results)}

            elif query_text.startswith('aggregate('):
                match = re.match(r'aggregate\((.*?)\)$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid aggregate query format: {query_text}"}
                pipeline_str = match.group(1).strip()
                pipeline = self._safe_eval(pipeline_str)
                if not isinstance(pipeline, list):
                    return {"error": f"Aggregation pipeline must be a list, got: {type(pipeline)} from '{pipeline_str}'"}
                results = list(self.collection.aggregate(pipeline))
                return {"type": "aggregate", "results": results, "count": len(results)}

            elif query_text.startswith('distinct('):
                match = re.match(r'distinct\("([^"]+)"(?:,\s*(.*?))?\)$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid distinct query format: {query_text}"}
                field_name = match.group(1)
                filter_str = match.group(2).strip() if match.group(2) else None
                filter_query = self._safe_eval(filter_str) if filter_str else {}
                results = self.collection.distinct(field_name, filter_query)
                limited_results = results[:50]
                return {"type": "distinct", "field": field_name, "results": limited_results, "count": len(results), "actual_count_limited": len(limited_results) < len(results) }

            elif query_text.startswith('count_documents('):
                match = re.match(r'count_documents\((.*?)\)$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid count_documents query format: {query_text}"}
                filter_str = match.group(1).strip()
                filter_query = self._safe_eval(filter_str) if filter_str else {}
                count = self.collection.count_documents(filter_query)
                return {"type": "count", "results": count}
            else:
                return {"error": f"Unsupported query format. Query must start with find(...), aggregate(...), distinct(...), or count_documents(...). Received: {query_text}"}
        except ValueError as ve:
             return {"error": f"Query parsing/evaluation error: {str(ve)}"}
        except pymongo.errors.OperationFailure as oe:
            return {"error": f"MongoDB operation failure: {str(oe)}"}
        except Exception as e:
            return {"error": f"Error executing MongoDB query: {str(e)} on query: {query_text}"}

    def _format_results_to_natural_language(self, user_question, query_results_dict, sample_size=10):
        results_data = query_results_dict.get('results')
        query_type = query_results_dict.get('type', 'unknown')

        # If it's a find or aggregate, just pretty-print the raw documents like MongoDB shell
        if query_type in ("find", "aggregate") and isinstance(results_data, list):
            if not results_data:
                return "No records found."
            # Show up to sample_size documents, pretty-printed
            return '\n'.join([
                json.dumps(doc, indent=2, default=str) for doc in results_data[:sample_size]
            ])
        # For distinct, show the list
        if query_type == "distinct":
            if not results_data:
                return "No records found."
            return json.dumps(results_data[:sample_size], indent=2, default=str)
        # For count, just show the number
        if query_type == "count":
            return str(results_data)
        # Fallback: show whatever is there
        return json.dumps(results_data, indent=2, default=str)

    def _validate_llm_query(self, query_text):
        """Validate if the LLM output is a supported MongoDB query string."""
        if not isinstance(query_text, str):
            return False
        valid_starts = [
            'find(', 'aggregate(', 'distinct(', 'count_documents('
        ]
        return any(query_text.strip().startswith(v) for v in valid_starts)

    def ask_question(self, user_question: str, sample_size: int = None):
        print(f"\n User question: \"{user_question}\"")
        print("🔄 Generating MongoDB query via LLM...")
        # Dynamically set sample_size from user question if a number is present
        if sample_size is None:
            # FIX: 're' is now guaranteed to be available from the top-level import
            match = re.search(r'(?:list|show|display|find|any|top|first|last|unique|records|collections)?\s*(\d{1,3})\s*(?:unique|records|collections|dcs|members|results|entries)?', user_question, re.IGNORECASE)
            if match:
                try:
                    sample_size = int(match.group(1))
                except Exception:
                    sample_size = 10
            else:
                sample_size = 10
        mongo_query_str = self._natural_language_to_query(user_question)
        print(f" LLM generated query string: {mongo_query_str}")

        if not self._validate_llm_query(mongo_query_str):
            print("❌ LLM did not return a valid MongoDB query. Showing raw output.")
            return f"Sorry, I could not understand your question well enough to generate a valid MongoDB query.\n\nRaw LLM output: {mongo_query_str}\nPlease try rephrasing your question."

        if mongo_query_str.startswith("error:"):
            return f" Query Generation Failed: {mongo_query_str}"

        filter_query = None
        if mongo_query_str.startswith('find('):
            # FIX: Removed 'import re'
            match = re.match(r'find\((.*?)\)(?:\.limit\((\d+)\))?$', mongo_query_str, re.DOTALL)
            if match:
                query_params_str = match.group(1).strip()
                try:
                    filter_query = self._safe_eval(query_params_str) if query_params_str else {}
                except Exception:
                    filter_query = None
                mongo_query_str = f'find({query_params_str}).limit({sample_size})'

        elif mongo_query_str.startswith('aggregate('):
            # FIX: Removed 'import re, ast'
            match = re.match(r'aggregate\((.*?)\)$', mongo_query_str, re.DOTALL)
            if match:
                pipeline_str = match.group(1).strip()
                try:
                    pipeline = self._safe_eval(pipeline_str)
                    if isinstance(pipeline, list) and pipeline and "$match" in pipeline[0]:
                        filter_query = pipeline[0]["$match"]
                except Exception:
                    filter_query = None

        elif mongo_query_str.startswith('distinct('):
            # FIX: Removed 'import re'
            match = re.match(r'distinct\("([^"]+)"(?:,\s*(.*?))?\)$', mongo_query_str, re.DOTALL)
            if match:
                filter_str = match.group(2).strip() if match.group(2) else None
                try:
                    filter_query = self._safe_eval(filter_str) if filter_str else {}
                except Exception:
                    filter_query = None
        # No change needed for count_documents, as it had no local import
        elif mongo_query_str.startswith('count_documents('):
            filter_query = None

        print(" Executing MongoDB query...")
        query_results_dict = self._execute_mongodb_query(mongo_query_str)

        true_count = None
        if filter_query is not None:
            try:
                true_count = self.collection.count_documents(filter_query)
                query_results_dict['true_count'] = true_count
            except Exception:
                pass

        if "error" in query_results_dict:
            return f" Query Execution Failed: {query_results_dict['error']}\n   Query attempted: {mongo_query_str}"

        print(" Formatting response...")
        natural_response = self._format_results_to_natural_language(user_question, query_results_dict, sample_size=sample_size)
        lines = natural_response.splitlines()
        filtered_lines = [line for line in lines if not line.strip().lower().startswith(("here are", "this data includes", "for more details", "for a complete list", "we found", "answer:", "there are", "sample record"))]
        while filtered_lines and not filtered_lines[0].strip():
            filtered_lines.pop(0)
        return '\n'.join(filtered_lines)

    def get_sample_questions(self):
        return [
            "Show me 5 collections for member 730110400002",
            "What are the details for dcsCode 001000001993 between 01/01/2025 and 03/01/2025?",
            "Find records where fat is greater than 4.1 and limit to 3 results",
            "List top 3 members by quantity on 01/01/2025",
            "Count records for member 730110400002",
            "List 5 unique dcs codes"
        ]

def main():
    print(" Welcome to MongoDB Bot ")
    try:
        # --- IMPORTANT ---
        # Replace these placeholders with your actual credentials.
        # It's recommended to use environment variables for security.
        GEMINI_API_KEY = "AIzaSyBW273iyp3NdBJdBEQzkRTRpUvZv-VcFuc"
        MONGODB_CONNECTION_STRING = "mongodb://user:pass@192.168.1.236:27017/?authSource=admin"

        if GEMINI_API_KEY == "YOUR_GEMINI_API_KEY_HERE" or MONGODB_CONNECTION_STRING == "YOUR_MONGODB_CONNECTION_STRING_HERE":
            print("\n‼️ CRITICAL SETUP REQUIRED ‼️")
            print("Please open the script and replace the placeholder values for")
            print("GEMINI_API_KEY and MONGODB_CONNECTION_STRING in the main() function.")
            return

        bot = EnhancedMongoDBBot(GEMINI_API_KEY, MONGODB_CONNECTION_STRING)
        print("\n✅ Enhanced MongoDB Natural Language Query Bot is ready!")
        print("Type 'samples' to see example questions, or 'quit' to exit.\n")

        while True:
            user_input = input(" Your question: ").strip()
            if user_input.lower() in ['quit', 'exit', 'bye']:
                print("👋 Goodbye!")
                break
            if user_input.lower() == 'samples':
                print("\nSample Questions You Can Ask:")
                for i, question in enumerate(bot.get_sample_questions(), 1):
                    print(f"{i}. {question}")
                print()
                continue
            if not user_input:
                continue

            print("\n" + "="*70)
            response = bot.ask_question(user_input)
            print(f"\n Answer:\n{response}")
            print("="*70 + "\n")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred in main: {e}")

if __name__ == "__main__":
    main()