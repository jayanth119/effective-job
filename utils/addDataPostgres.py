import pandas as pd
import psycopg2

class CampaignDataUploader:
    def __init__(self, db_config, excel_path):
        self.db_config = db_config
        self.excel_path = excel_path
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS campaign_data (
            query TEXT,
            person_first_name TEXT,
            person_last_name TEXT,
            person_headline TEXT,
            person_business_email TEXT,
            person_personal_email TEXT,
            person_linkedin_url TEXT,
            company_name TEXT,
            company_size TEXT,
            company_type TEXT,
            company_country TEXT,
            company_industry TEXT,
            company_linkedin_url TEXT,
            company_meta_title TEXT,
            company_meta_description TEXT,
            company_meta_keywords TEXT
        );
        """
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def load_data_from_excel(self):
        df = pd.read_excel(self.excel_path)
        return df

    def insert_data(self, df):
        insert_query = """
        INSERT INTO campaign_data (
            query, person_first_name, person_last_name, person_headline,
            person_business_email, person_personal_email, person_linkedin_url,
            company_name, company_size, company_type, company_country,
            company_industry, company_linkedin_url, company_meta_title,
            company_meta_description, company_meta_keywords
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Convert NaN to None (for SQL NULL) to avoid formatting issues
        df = df.where(pd.notnull(df), None)

        for _, row in df.iterrows():
            values = (
                row['query'], row['person_first_name'], row['person_last_name'], row['person_headline'],
                row['person_business_email'], row['person_personal_email'], row['person_linkedin_url'],
                row['company_name'], row['company_size'], row['company_type'], row['company_country'],
                row['company_industry'], row['company_linkedin_url'], row['company_meta_title'],
                row['company_meta_description'], row['company_meta_keywords']
            )
            self.cursor.execute(insert_query, values)

        self.conn.commit()

    def run(self):
        try:
            self.connect()
            self.create_table()
            df = self.load_data_from_excel()
            self.insert_data(df)
            print("✅ Data inserted successfully.")
        except Exception as e:
            print("❌ Error:", e)
        finally:
            self.close()


if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "database": "postgres",
        "user": "jayanth",
        "password": "secretpassword"
    }
    excel_path = "data/Sample_Campaign_Data.xlsx"
    uploader = CampaignDataUploader(db_config, excel_path)
    uploader.run()