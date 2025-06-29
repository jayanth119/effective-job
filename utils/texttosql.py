import pandas as pd
import psycopg2
from langchain_google_genai import GoogleGenerativeAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.schema import OutputParserException
import os
from typing import List, Dict, Any
import re

class TextToSQLGenerator:
    def __init__(self, db_config: Dict[str, str], google_api_key: str):
        """
        Initialize the Text-to-SQL generator with database config and Google API key
        
        Args:
            db_config: Database connection configuration
            google_api_key: Google Gemini API key
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        
        # Initialize Google Gemini LLM
        os.environ["GOOGLE_API_KEY"] = google_api_key
        self.llm = GoogleGenerativeAI(
            model="gemini-1.5-flash",
            temperature=0.1,
            google_api_key=google_api_key
        )
        
        # Define the database schema
        self.schema_info = """
        Table: campaign_data
        Columns:
        - query (TEXT): Search query used
        - person_first_name (TEXT): Person's first name
        - person_last_name (TEXT): Person's last name
        - person_headline (TEXT): Person's professional headline
        - person_business_email (TEXT): Person's business email
        - person_personal_email (TEXT): Person's personal email
        - person_linkedin_url (TEXT): Person's LinkedIn profile URL
        - company_name (TEXT): Company name
        - company_size (TEXT): Company size
        - company_type (TEXT): Type of company
        - company_country (TEXT): Company country
        - company_industry (TEXT): Company industry
        - company_linkedin_url (TEXT): Company LinkedIn URL
        - company_meta_title (TEXT): Company meta title
        - company_meta_description (TEXT): Company meta description
        - company_meta_keywords (TEXT): Company meta keywords
        """
        
        # Create the prompt template
        self.prompt_template = PromptTemplate(
            input_variables=["schema", "question"],
            template="""
            You are a PostgreSQL expert. Given the following database schema and a natural language question, 
            generate a valid PostgreSQL query. Return ONLY the SQL query without any explanation or formatting.

            Database Schema:
            {schema}

            Question: {question}

            Rules:
            1. Use proper PostgreSQL syntax
            2. Use ILIKE for case-insensitive string matching
            3. Use appropriate WHERE clauses for filtering
            4. Use LIMIT when asking for specific number of records
            5. Use COUNT(*) for counting queries
            6. Use DISTINCT when needed to avoid duplicates
            7. Return only the SQL query, no explanation

            SQL Query:
            """
        )
        
        # Create the LLM chain
        self.sql_chain = LLMChain(
            llm=self.llm,
            prompt=self.prompt_template
        )

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✅ Connected to database successfully")
        except Exception as e:
            print(f"❌ Database connection error: {e}")
            raise

    def close_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✅ Database connection closed")

    def generate_sql_query(self, natural_language_question: str) -> str:
        """
        Generate SQL query from natural language question
        
        Args:
            natural_language_question: User's question in natural language
            
        Returns:
            Generated SQL query string
        """
        try:
            result = self.sql_chain.run(
                schema=self.schema_info,
                question=natural_language_question
            )
            
            # Clean up the generated query
            sql_query = self._clean_sql_query(result)
            return sql_query
            
        except Exception as e:
            print(f"❌ Error generating SQL query: {e}")
            return None

    def _clean_sql_query(self, raw_query: str) -> str:
        """Clean and validate the generated SQL query"""
        # Remove any markdown formatting or extra text
        query = raw_query.strip()
        
        # Remove code blocks if present
        if query.startswith("```"):
            query = re.sub(r"```sql\n|```\n|```", "", query)
        
        # Remove any explanatory text before or after the query
        lines = query.split('\n')
        sql_lines = []
        
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('--'):
                sql_lines.append(line)
        
        query = ' '.join(sql_lines)
        
        # Ensure query ends with semicolon
        if not query.endswith(';'):
            query += ';'
            
        return query

    def execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """
        Execute the generated SQL query and return results
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            List of dictionaries containing query results
        """
        try:
            if not self.conn:
                self.connect_db()
                
            self.cursor.execute(sql_query)
            
            # Get column names
            column_names = [desc[0] for desc in self.cursor.description]
            
            # Fetch all results
            rows = self.cursor.fetchall()
            
            # Convert to list of dictionaries
            results = []
            for row in rows:
                result_dict = dict(zip(column_names, row))
                results.append(result_dict)
                
            return results
            
        except Exception as e:
            print(f"❌ Error executing query: {e}")
            return []

    def query_from_text(self, natural_language_question: str) -> Dict[str, Any]:
        """
        Main method to convert natural language to SQL and execute query
        
        Args:
            natural_language_question: User's question in natural language
            
        Returns:
            Dictionary containing SQL query, results, and metadata
        """
        print(f"🔍 Processing question: {natural_language_question}")
        
        # Generate SQL query
        sql_query = self.generate_sql_query(natural_language_question)
        
        if not sql_query:
            return {
                "question": natural_language_question,
                "sql_query": None,
                "results": [],
                "error": "Failed to generate SQL query"
            }
        
        print(f"📝 Generated SQL: {sql_query}")
        
        # Execute query
        results = self.execute_query(sql_query)
        
        return {
            "question": natural_language_question,
            "sql_query": sql_query,
            "results": results,
            "num_results": len(results),
            "error": None
        }

    def get_sample_questions(self) -> List[str]:
        """Return sample questions users can ask"""
        return [
            "Show me all people from technology companies",
            "Find all companies in the United States",
            "Get all people with CEO in their headline",
            "Show me companies with more than 1000 employees",
            "Find all people from startups",
            "Get all companies in the software industry",
            "Show me people with gmail email addresses",
            "Find companies founded in California",
            "Get all people from companies with 'tech' in the name",
            "Show me the top 10 largest companies by size"
        ]


# Example usage and testing
if __name__ == "__main__":
    # Database configuration
    db_config = {
        "host": "localhost",
        "database": "postgres",
        "user": "jayanth",
        "password": "secretpassword"
    }
    
    # Replace with your actual Google API key
    GOOGLE_API_KEY = ""
    

    text_to_sql = TextToSQLGenerator(db_config, GOOGLE_API_KEY)
    
    try:
        # Connect to database
        text_to_sql.connect_db()
        
        # Example questions
        sample_questions = [
            "How many people are there in the database?",
        ]
        
        print("🚀 Testing Text-to-SQL Generation:")
        print("=" * 50)
        
        for question in sample_questions:
            result = text_to_sql.query_from_text(question)
            
            print(f"\n📋 Question: {result['question']}")
            print(f"🔧 SQL Query: {result['sql_query']}")
            print(f"📊 Number of results: {result['num_results']}")
            
            if result['error']:
                print(f"❌ Error: {result['error']}")
            else:
               
                if result['results']:
                    print("📋 Sample results:")
                    for i, row in enumerate(result['results'][:3]):  
                        print(f"  {i+1}. {row}")
                    if len(result['results']) > 3:
                        print(f"  ... and {len(result['results']) - 3} more")
            
            print("-" * 30)
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        text_to_sql.close_db()
        
