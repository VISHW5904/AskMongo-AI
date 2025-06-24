from pymongo import MongoClient # type: ignore
import google.generativeai as genai # type: ignore

# Initialize Gemini AI
genai.configure(api_key="AIzaSyBW273iyp3NdBJdBEQzkRTRpUvZv-VcFuc")
model = genai.GenerativeModel('models/gemini-2.5-flash-preview-05-20')

# Function to generate MongoDB query using Gemini
def generate_mongodb_query(user_question):
    prompt = f"Convert the following natural language question into a MongoDB query:\n\nUser Question: {user_question}"
    response = model.generate_content(prompt)
    return response.text.strip()

# Function to run the query in MongoDB shell and return results
def run_query_in_mongodb(query):
    # Connect to MongoDB
    client = MongoClient("mongodb://user:pass@192.168.1.236:27017/?authSource=admin")
    db = client['lactalis_db']
    collection = db['milk_collections']

    # Execute the query
    try:
        # Assuming the query is an aggregation pipeline
        results = list(collection.aggregate(eval(query)))
        return results
    except Exception as e:
        return str(e)

# Function to convert results to natural language using Gemini
def format_results_to_natural_language(user_question, results):
    prompt = f"Convert the following MongoDB query results into a clear, professional, human-readable response:\n\nUser Question: {user_question}\n\nResults: {results}"
    response = model.generate_content(prompt)
    return response.text.strip()

# Example usage
user_question = "List any 5 unique memberCode."
mongodb_query = generate_mongodb_query(user_question)
print("Generated MongoDB Query:", mongodb_query)

results = run_query_in_mongodb(mongodb_query)
print("Query Results:", results)

natural_language_response = format_results_to_natural_language(user_question, results)
print("Natural Language Response:", natural_language_response)