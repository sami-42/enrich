import os
import pandas as pd
import requests
import time
from urllib.parse import urlparse
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify, session
from werkzeug.utils import secure_filename
import json
import threading
from datetime import datetime
import uuid

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'outputs'
app.config['HISTORY_FILE'] = 'history.json'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

@app.template_filter('datetimeformat')
def datetimeformat(value, format='%Y-%m-%d %H:%M:%S'):
    if value:
        try:
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return dt.strftime(format)
        except:
            return value
    return ''

# Create directories if they don't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

# Global variables
processing_logs = []
processing_status = {}  # Store processing status by a unique ID
saved_api_key = ""  # Global variable to store API key

def add_log(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    processing_logs.append(log_entry)
    print(log_entry)  # Also print to console

def load_history():
    if os.path.exists(app.config['HISTORY_FILE']):
        try:
            with open(app.config['HISTORY_FILE'], 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_history(history):
    with open(app.config['HISTORY_FILE'], 'w') as f:
        json.dump(history, f, indent=2)

def add_history_entry(original_filename, output_filename, status, rows_processed=0):
    history = load_history()
    history.append({
        'type': 'processing',
        'id': str(uuid.uuid4()),
        'original_filename': original_filename,
        'output_filename': output_filename,
        'status': status,
        'rows_processed': rows_processed,
        'timestamp': datetime.now().isoformat()
    })
    save_history(history)

def extract_domain(url):
    if pd.isna(url) or not isinstance(url, str) or url.strip() == "":
        return None
    parsed = urlparse(url.strip())
    domain = parsed.netloc or parsed.path
    domain = domain.strip("/")
    if domain.startswith("www."):
        domain = domain[4:]
    return domain

def build_request_row(row):
    first_name = row["First Name"] if pd.notna(row["First Name"]) else None
    last_name = row["Last Name"] if pd.notna(row["Last Name"]) else None
    linkedin_url = row["LinkedIn URL"] if pd.notna(row["LinkedIn URL"]) else None
    organization_name = row["Company Name"] if pd.notna(row["Company Name"]) else None
    domain = extract_domain(row["Company Website"])

    if not first_name or not last_name or (not domain and not linkedin_url):
        return None

    return {
        "first_name": first_name.strip() if first_name else None,
        "last_name": last_name.strip() if last_name else None,
        "linkedin_url": linkedin_url.strip() if linkedin_url else None,
        "organization_name": organization_name.strip() if organization_name else None,
        "domain": domain
    }

def fetch_bulk_emails(batch, api_key):
    url = "https://api.apollo.io/api/v1/people/bulk_match?reveal_personal_emails=true&reveal_phone_number=false"
    payload = {"details": batch}
    headers = {
        "x-api-key": api_key,
        "accept": "application/json",
        "Content-Type": "application/json",
        "Cache-Control": "no-cache"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code == 422:
            add_log(f"Validation error from Apollo. Status: {response.status_code}")
            add_log(f"Response: {response.text[:200]}...")
            return ["Validation Error"] * len(batch)

        # Check for insufficient credits or other API errors
        if response.status_code != 200:
            error_msg = response.text
            add_log(f"API Error: {response.status_code} - {error_msg}")
            if "insufficient credits" in error_msg.lower():
                add_log("❌ STOPPING: Insufficient Apollo credits. Please upgrade your plan.")
                raise Exception("Insufficient Apollo credits. Please upgrade your plan.")
            return ["API Error"] * len(batch)

        response.raise_for_status()
        data = response.json()
        emails = []
        if "matches" in data:
            for match in data["matches"]:
                email = match.get("email", "No email found")
                emails.append(email)
        else:
            emails = ["No email found"] * len(batch)
        return emails
    except requests.exceptions.HTTPError as e:
        add_log(f"HTTP Error: {e}")
        add_log(f"Response Body: {response.text if 'response' in locals() else 'No response'}")
        return ["HTTP Error"] * len(batch)
    except Exception as e:
        add_log(f"General Error: {e}")
        # Re-raise the exception to stop processing
        raise e

def process_csv(api_key, input_file_path, output_file_path, original_filename, process_id):
    try:
        add_log("Starting CSV processing...")
        
        # Initialize processing status for this process
        processing_status[process_id] = {
            'download_ready': False,
            'download_file': None,
            'download_filename': None,
            'error': False,
            'error_message': None
        }
        
        # Read CSV
        df = pd.read_csv(input_file_path)
        add_log(f"Loaded CSV with {len(df)} rows")

        REQUIRED_HEADERS = ["First Name", "Last Name", "LinkedIn URL", "Company Name", "Company Website"]
        
        # Keep only required columns
        df = df[REQUIRED_HEADERS].copy()
        df["Email"] = ""  # new column for emails

        BATCH_SIZE = 10
        DELAY_SECONDS = 3
        total_rows = len(df)
        rows_processed = 0
        
        add_log(f"Processing {total_rows} rows in batches of {BATCH_SIZE}")

        for start in range(0, total_rows, BATCH_SIZE):
            batch_df = df.iloc[start:start+BATCH_SIZE]
            add_log(f"Processing batch {start//BATCH_SIZE + 1}: rows {start+1} to {min(start+len(batch_df), total_rows)}")

            requests_batch = []
            batch_indexes = []

            # Build batch, skip invalid rows
            for idx, row in batch_df.iterrows():
                req = build_request_row(row)
                if req:
                    requests_batch.append(req)
                    batch_indexes.append(idx)

            if not requests_batch:
                add_log(f"Skipping batch - no valid data")
                time.sleep(DELAY_SECONDS)
                continue

            add_log(f"Sending batch with {len(requests_batch)} valid records")
            emails = fetch_bulk_emails(requests_batch, api_key)
            df.loc[batch_indexes, "Email"] = emails
            rows_processed += len(requests_batch)
            
            add_log(f"Batch completed. Waiting {DELAY_SECONDS} seconds before next batch...")
            time.sleep(DELAY_SECONDS)

        # Save to new CSV with only input columns + Email
        df.to_csv(output_file_path, index=False)
        add_log(f"Processing complete! Output saved to {output_file_path}")
        
        # Add to history
        add_history_entry(original_filename, os.path.basename(output_file_path), "completed", rows_processed)
        
        # Set flag for automatic download
        processing_status[process_id] = {
            'download_ready': True,
            'download_file': os.path.basename(output_file_path),
            'download_filename': f"output_{original_filename}",
            'error': False,
            'error_message': None
        }
        
    except Exception as e:
        error_message = str(e)
        add_log(f"❌ FATAL ERROR: {error_message}")
        add_history_entry(original_filename, "", "failed", 0)
        
        # Update status with error
        processing_status[process_id] = {
            'download_ready': False,
            'download_file': None,
            'download_filename': None,
            'error': True,
            'error_message': error_message
        }
    finally:
        pass

@app.route('/')
def index():
     global saved_api_key
    # Check if there's a saved API key to determine checkbox state
     has_saved_key = bool(saved_api_key)
     return render_template('index.html', logs=processing_logs, saved_api_key=saved_api_key, has_saved_key=has_saved_key)

@app.route('/upload', methods=['POST'])
def upload_file():
    global processing_logs, saved_api_key
    processing_logs = []  # Clear previous logs
    
    try:
        api_key = request.form['api_key']
        save_api_key_flag = request.form.get('save_api_key') == 'on'
        
        if not api_key:
            flash('API key is required')
            return redirect(url_for('index'))
            
        # Save API key if requested
        if save_api_key_flag:
            saved_api_key = api_key
            add_log("API key saved for future use")
        else:
            saved_api_key = ""  # Clear saved key if not saving
            
        if 'file' not in request.files:
            flash('No file selected')
            return redirect(url_for('index'))
            
        file = request.files['file']
        if file.filename == '':
            flash('No file selected')
            return redirect(url_for('index'))
            
        if file and file.filename.endswith('.csv'):
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name_without_ext = os.path.splitext(filename)[0]
            new_filename = f"{name_without_ext}_{timestamp}.csv"
            input_path = os.path.join(app.config['UPLOAD_FOLDER'], new_filename)
            file.save(input_path)
            
            output_filename = f"output_{name_without_ext}_{timestamp}.csv"
            output_path = os.path.join(app.config['OUTPUT_FOLDER'], output_filename)
            
            # Generate unique process ID
            process_id = str(uuid.uuid4())
            session['process_id'] = process_id
            
            # Start processing in background thread
            thread = threading.Thread(
                target=process_csv, 
                args=(api_key, input_path, output_path, filename, process_id)
            )
            thread.start()
            
            flash('File uploaded successfully. Processing started...')
            return redirect(url_for('processing'))
        else:
            flash('Please upload a CSV file')
            return redirect(url_for('index'))
            
    except Exception as e:
        flash(f'Error: {str(e)}')
        return redirect(url_for('index'))

@app.route('/processing')
def processing():
    return render_template('processing.html', logs=processing_logs)

@app.route('/logs')
def logs():
    return jsonify({'logs': processing_logs})

@app.route('/check_download')
def check_download():
    process_id = session.get('process_id')
    if process_id and process_id in processing_status:
        status = processing_status[process_id]
        if status.get('error'):
            return jsonify({
                'ready': False,
                'error': True,
                'error_message': status.get('error_message')
            })
        if status.get('download_ready'):
            return jsonify({
                'ready': True,
                'file': status.get('download_file'),
                'filename': status.get('download_filename')
            })
    return jsonify({'ready': False})

@app.route('/api_key')
def show_api_key():
    global saved_api_key
    # Return the current API key being used (from form input, not saved)
    # This will be handled via JavaScript on the frontend
    return jsonify({'api_key': saved_api_key})

@app.route('/download/<filename>')
def download_file(filename):
    try:
        return send_file(os.path.join(app.config['OUTPUT_FOLDER'], filename), as_attachment=True)
    except:
        flash('File not found')
        return redirect(url_for('index'))

@app.route('/history')
def history():
    history_data = load_history()
    # Filter out API key entries and sort by timestamp
    processing_history = [entry for entry in history_data if entry.get('type') == 'processing']
    processing_history.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
    return render_template('history.html', history=processing_history)

if __name__ == '__main__':
    app.run(debug=True)