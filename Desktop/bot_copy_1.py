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

        prompt = """
You are an expert MongoDB query generator for a milk collection database.
Your ONLY job is to convert the user's natural language question into a MongoDB query string
that can be executed by PyMongo's `collection.find()` or `collection.aggregate()` methods.

Collection Name: milk_collections
Sample Document Structure (field: type):
%s

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

User Question: "%s"

Query Generation Rules:
- All field names and values are dynamic and should be inferred from the user question.
1. Output ONLY a valid MongoDB query string, starting with aggregate([ ... ]), find({ ... }), distinct(...), or count_documents(...).
2. Do NOT include any code block markers, language names, explanations, or comments.
3. Do NOT include "python", "json", or "mongodb" anywhere in your output.
4. Do NOT include any text before or after the query string.
5. For simple filters output a find query. Example: find({{"memberCode": "123", "fat": {{"$gt": 3.5}}}}).
6. For aggregations (sum, average, count, group by, top N, min, max, trend, compare), output an aggregation pipeline. Example: aggregate([{{"$match": {{"fat": {{"$gt": 3.5}}}}}}, {{"$group": {{"_id": "$memberCode", "avgFat": {{"$avg": "$fat"}}}}}}]).
7. Date Handling: If a specific date like "01/01/2025" is mentioned, filter for the entire day. For date ranges like "between 01/01/2025 and 03/01/2025", use $gte for the start date and $lt for the day after the end date. Convert all date strings to ISODate format, e.g., ISODate("YYYY-MM-DDTHH:MM:SSZ").
8. Comparisons: For "greater than", use $gt. For "less than", use $lt.
9. Top/Bottom N: Use $sort and $limit in an aggregation pipeline. For "top" use $sort: -1, for "least" or "lowest" use $sort: 1. Ensure the field for sorting is correct (e.g., "$qty" for quantity, or a summed field like "$totalQuantity" if grouped).
10. Averages: Use $avg in a $group stage.
11. Minimum/Maximum: Use $min/$max in a $group stage, or $sort + $limit for records.
12. Counts: For specific counts, use count_documents({{filter}}). For grouped counts, use $sum: 1 in $group.
13. Distinct values: use distinct("fieldName", {{filter}})
14. Handle numbers as numbers in queries, not strings, unless they are codes.
15. Ensure all MongoDB operators (like $match, $group, $gte, $sum, $avg, $min, $max, $sort, $limit) and field names (like memberCode, qty) are enclosed in double quotes.
16. If the user asks for results by month, group by the month extracted from the date field using {{"month": {{"$month": "$dateTimeOfCollection"}}}} in the aggregation _id.
17. If the user asks for specific months (e.g., November and December), include a filter in the $match stage using $expr and $in with $month, e.g., {"$expr": {"$and": [ ... ]}}. Do not use $and in $match for month filtering; always use $expr and $in with $month and $year for month-based filtering.
18. For trend questions (trend of X per month/year), group by month/year and use $avg/$sum as needed, and $sort by time.
19. For compare questions (compare X for member A and member B), use $match with $in for codes, group by code and time if needed.
20. For top/least by field, use $group, $sort, and $limit.
21. For lowest/highest value on a date, use $match for the date, $sort, and $limit:1.
22. For unique values, use distinct.

Examples:
find({{"memberCode": "12345"}})
find({{"memberCode": "0010000019930016"}})
find({{"dcsCode": "001000001993", "dateTimeOfCollection": {{"$gte": ISODate("2025-01-01T00:00:00Z"), "$lt": ISODate("2025-03-02T00:00:00Z")}}}})
find({{"fat": {{"$gt": 4.1}}}})
find({{"memberCode": "0010000019930016", "fat": {{"$gt": 4.1}}, "dateTimeOfCollection": {{"$gte": ISODate("2025-01-01T00:00:00Z"), "$lt": ISODate("2025-02-01T00:00:00Z")}}}})
find({{"snf": {{"$gt": 9}}, "fat": {{"$gt": 3}}}})
find({{"memberCode": "0010000019930016", "snf": {{"$gt": 8.5}}, "fat": {{"$gt": 4.1}}}})
distinct("memberCode", {{}})
distinct("memberCode", {{"qty": {{"$gt": 100}}}})
aggregate([{{"$match": {{}}}}, {{"$group": {{"_id": "$memberCode", "totalQty": {{"$sum": "$qty"}}}}}}, {{"$sort": {{"totalQty": -1}}}}, {{"$limit": 5}}])
aggregate([{{"$match": {{"dcsCode": "001000001993"}}}}, {{"$group": {{"_id": "$dcsCode", "minSNF": {{"$min": "$snf"}}}}}}])
aggregate([{{"$match": {{"dateTimeOfCollection": {{"$gte": ISODate("2024-11-09T00:00:00Z"), "$lt": ISODate("2024-11-10T00:00:00Z")}}}}}}, {{"$sort": {{"qty": 1}}}}, {{"$limit": 1}}])
aggregate([{{"$match": {{"dcsCode": "001000001993"}}}}, {{"$group": {{"_id": "$dcsCode", "minSNF": {{"$min": "$snf"}}}}}}])
aggregate([{{"$match": {{}}}}, {{"$group": {{"_id": "$memberCode", "avgSNF": {{"$avg": "$snf"}}}}}}, {{"$sort": {{"avgSNF": -1}}}}, {{"$limit": 1}}])
aggregate([{{"$match": {{"$expr": {{"$and": [{{"$in": ["$memberCode", ["0010000019930016", "0010000004790107"]]}}, {{"$in": [{{"$month": "$dateTimeOfCollection"}}, [11, 12]]}}, {{"$eq": [{{"$year": "$dateTimeOfCollection"}}, 2024]}}]}}}}}}, {{"$group": {{"_id": {{"memberCode": "$memberCode", "month": {{"$month": "$dateTimeOfCollection"}}}}, "averageQty": {{"$avg": "$qty"}}}}}}, {{"$sort": {{"_id.memberCode": 1, "_id.month": 1}}}}])
aggregate([{{"$match": {{}}}}, {{"$group": {{"_id": {{"month": {{"$month": "$dateTimeOfCollection"}}}}, "avgFat": {{"$avg": "$fat"}}}}}}, {{"$sort": {{"_id.month": 1}}}}])
aggregate([{{"$match": {{"memberCode": "0010000008650047"}}}}, {{"$group": {{"_id": {{"month": {{"$month": "$dateTimeOfCollection"}}}}, "avgFat": {{"$avg": "$fat"}}}}}}, {{"$sort": {{"_id.month": 1}}}}])
aggregate([{{"$match": {{}}}}, {{"$group": {{"_id": {{"year": {{"$year": "$dateTimeOfCollection"}}}}, "totalQty": {{"$sum": "$qty"}}}}}}, {{"$sort": {{"_id.year": 1}}}}])
aggregate([{{"$match": {{"$expr": {{"$and": [{{"$in": ["$memberCode", ["0010000019930016", "0010000004790107"]]}}, {{"$in": [{{"$month": "$dateTimeOfCollection"}}, [11, 12]]}}, {{"$eq": [{{"$year": "$dateTimeOfCollection"}}, 2024]}}]}}}}}}, {{"$group": {{"_id": {{"memberCode": "$memberCode", "month": {{"$month": "$dateTimeOfCollection"}}}}, "avgQty": {{"$avg": "$qty"}}, "avgSnf": {{"$avg": "$snf"}}, "avgFat": {{"$avg": "$fat"}}}}}}, {{"$sort": {{"_id.memberCode": 1, "_id.month": 1}}}}])

Query:
""" % (json.dumps(self.schema_info, indent=2), user_question)
        try:
            response = self.model.generate_content(prompt)
            query_text = response.text.strip()
            # Remove code block markers, language names, and any explanations
            query_text = re.sub(r"^(```\\w*\\n*)|(```)$", "", query_text, flags=re.MULTILINE).strip()
            query_text = re.sub(r"^(python|json|mongodb)\\n*", "", query_text, flags=re.IGNORECASE).strip()
            # Remove any lines before the query string
            lines = query_text.splitlines()
            for line in lines:
                if line.strip().startswith(('find(', 'aggregate(', 'distinct(', 'count_documents(')):
                    query_text = line.strip()
                    break
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
        # Fix: Convert all JSON 'true'/'false' to Python 'True'/'False' for eval
        query_string = re.sub(r':\s*true', ': True', query_string, flags=re.IGNORECASE)
        query_string = re.sub(r':\s*false', ': False', query_string, flags=re.IGNORECASE)
        # Replace all ISODate("...") with datetime.fromisoformat(...) globally, even if nested
        def iso_to_datetime(match):
            iso = match.group(1).replace('Z', '+00:00')
            if 'T' not in iso:
                iso += 'T00:00:00+00:00'
            elif '+' not in iso and '-' not in iso[-6:]:
                iso += '+00:00'
            return f'datetime.fromisoformat("{iso}")'
        query_string = re.sub(r'ISODate\("([^\"]+)"\)', iso_to_datetime, query_string)
        # Remove trailing commas before } or ] everywhere in the string (not just at the end)
        query_string = re.sub(r',\s*([}\]])', r'\1', query_string)
        # Remove any trailing commas before ) (for function calls like find(...,))
        query_string = re.sub(r',\s*\)', ')', query_string)
        try:
            # ast.literal_eval is safer than eval, but can't handle function calls like datetime()
            return ast.literal_eval(query_string)
        except (ValueError, SyntaxError, TypeError):
            try:
                # Use eval only as a fallback for datetime objects, with a restricted scope
                return eval(query_string, {"__builtins__": {}}, {"datetime": datetime, "True": True, "False": False, "None": None})
            except Exception as e_eval:
                raise ValueError(f"Query parsing error with eval: {e_eval} on string: {query_string}")

    def _fix_aggregation_pipeline(self, pipeline):
        # Fix $expr $in to standard $in in $match
        if isinstance(pipeline, list) and pipeline:
            # Fix $expr $in in $match
            match_stage = pipeline[0]
            if "$match" in match_stage and "$expr" in match_stage["$match"]:
                expr = match_stage["$match"]["$expr"]
                if (
                    isinstance(expr, dict)
                    and "$in" in expr
                    and isinstance(expr["$in"], list)
                    and len(expr["$in"]) == 2
                    and isinstance(expr["$in"][0], str)
                    and expr["$in"][0].startswith("$")
                    and isinstance(expr["$in"][1], list)
                ):
                    field = expr["$in"][0][1:]  # remove leading $
                    values = expr["$in"][1]
                    match_stage["$match"] = {field: {"$in": values}}
                    if "$expr" in match_stage["$match"]:
                        del match_stage["$match"]["$expr"]
            # Fix $group _id: null to _id: "$memberCode" if user question asks for compare/group by member
            group_stage = pipeline[1] if len(pipeline) > 1 else None
            if (
                group_stage
                and "$group" in group_stage
                and group_stage["$group"].get("_id") is None
            ):
                group_stage["$group"]["_id"] = "$memberCode"
        return pipeline

    def _has_invalid_top_level_operator(self, obj):
        """Check if a dict or list has a top-level key that is a MongoDB operator (starts with $)."""
        if isinstance(obj, dict):
            for k in obj.keys():
                if isinstance(k, str) and k.startswith('$'):
                    return True
        elif isinstance(obj, list):
            for item in obj:
                if self._has_invalid_top_level_operator(item):
                    return True
        return False

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
                # --- Validation for top-level operator ---
                if self._has_invalid_top_level_operator(filter_query):
                    return {"error": "Invalid query: top-level key cannot be a MongoDB operator like $gte, $lt, etc. Please rephrase your question."}
                results = list(self.collection.find(filter_query).limit(limit))
                return {"type": "find", "results": results, "count": len(results)}

            elif query_text.startswith('aggregate('):
                match = re.match(r'aggregate\((.*?)\)$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid aggregate query format: {query_text}"}
                pipeline_str = match.group(1).strip()
                pipeline = self._safe_eval(pipeline_str)
                if not isinstance(pipeline, list):
                    return {"error": f"Aggregation pipeline must be a list, got: {type(pipeline)} from '{pipeline_str}'"}
                # --- Fix aggregation pipeline if needed ---
                pipeline = self._fix_aggregation_pipeline(pipeline)
                # --- Validation for top-level operator in $match ---
                if pipeline and "$match" in pipeline[0]:
                    match_stage = pipeline[0]["$match"]
                    if self._has_invalid_top_level_operator(match_stage):
                        return {"error": "Invalid aggregation: $match stage has a top-level MongoDB operator like $gte, $lt, etc. Please rephrase your question."}
                results = list(self.collection.aggregate(pipeline))
                return {"type": "aggregate", "results": results, "count": len(results)}

            elif query_text.startswith('distinct('):
                match = re.match(r'distinct\("([^"]+)"(?:,\s*(.*?))?\)$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid distinct query format: {query_text}"}
                field_name = match.group(1)
                filter_str = match.group(2).strip() if match.group(2) else None
                filter_query = self._safe_eval(filter_str) if filter_str else {}
                # --- Validation for top-level operator ---
                if self._has_invalid_top_level_operator(filter_query):
                    return {"error": "Invalid query: top-level key cannot be a MongoDB operator like $gte, $lt, etc. Please rephrase your question."}
                results = self.collection.distinct(field_name, filter_query)
                limited_results = results[:50]
                return {"type": "distinct", "field": field_name, "results": limited_results, "count": len(results), "actual_count_limited": len(limited_results) < len(results) }

            elif query_text.startswith('count_documents('):
                match = re.match(r'count_documents\((.*?)\)$', query_text, re.DOTALL)
                if not match: return {"error": f"Invalid count_documents query format: {query_text}"}
                filter_str = match.group(1).strip()
                filter_query = self._safe_eval(filter_str) if filter_str else {}
                # --- Validation for top-level operator ---
                if self._has_invalid_top_level_operator(filter_query):
                    return {"error": "Invalid query: top-level key cannot be a MongoDB operator like $gte, $lt, etc. Please rephrase your question."}
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

        # For find, use sample_size; for aggregate, show all results unless user asks for a limit
        if query_type == "find" and isinstance(results_data, list):
            if not results_data:
                return "No records found."
            return '\n'.join([
                json.dumps(doc, indent=2, default=str) for doc in results_data[:sample_size]
            ])
        if query_type == "aggregate" and isinstance(results_data, list):
            if not results_data:
                return "No records found."
            return '\n'.join([
                json.dumps(doc, indent=2, default=str) for doc in results_data
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
            print(" LLM did not return a valid MongoDB query. Showing raw output.")
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
        GEMINI_API_KEY = "xxx"
        MONGODB_CONNECTION_STRING = "xxx"

        if GEMINI_API_KEY == "YOUR_GEMINI_API_KEY_HERE" or MONGODB_CONNECTION_STRING == "YOUR_MONGODB_CONNECTION_STRING_HERE":
            print("\n CRITICAL SETUP REQUIRED ")
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
        print(f"\n An unexpected error occurred in main: {e}")

if __name__ == "__main__":
    main()