import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2 import sql, Error

app = Flask(__name__)

# Database Configuration (Neon PostgreSQL)
DB_CONFIG = {
    "host": "ep-cool-sunset-a8afco8c-pooler.eastus2.azure.neon.tech",
    "database": "position",
    "user": "position_owner",
    "password": "npg_p3hw5sNRFLQE",
    "port": "5432"
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Database connection error: {e}")
        return None

# API Endpoint to Insert a Deal
@app.route('/add_deal', methods=['POST'])
def add_deal():
    data = request.get_json()
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cursor = conn.cursor()
        insert_query = '''INSERT INTO deals (client, date, week, month, year, customer, source, deal, country, destination, gross_weight, final_profit, pure_weight) 
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            data.get('client'), data.get('date'), data.get('week'), data.get('month'), data.get('year'),
            data.get('customer'), data.get('source'), data.get('deal'), data.get('country'), data.get('destination'),
            data.get('gross_weight'), data.get('final_profit'), data.get('pure_weight')
        ))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Deal added successfully"}), 201
    except Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": f"Failed to insert deal: {str(e)}"}), 500

# API Endpoint to Fetch Deals
@app.route('/get_deals', methods=['GET'])
def get_deals():
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM deals")
        column_names = [desc[0] for desc in cursor.description]
        deals = [dict(zip(column_names, row)) for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify(deals)
    except Error as e:
        print(f"Database query error: {e}")
        return jsonify({"error": f"Failed to retrieve deals: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
