from flask import Flask, request, jsonify
import os
import json
import requests
import csv
import io
from datetime import datetime

# Add startup logging for Railway debugging
print("=== RAILWAY STARTUP DEBUG ===")
print("Python starting up...")
print("PORT:", os.environ.get('PORT', 'Not set'))
print("SUPABASE_URL:", "Set" if os.environ.get('SUPABASE_URL') else "Not set")
print("SUPABASE_SERVICE_ROLE_KEY:", "Set" if os.environ.get('SUPABASE_SERVICE_ROLE_KEY') else "Not set")

# Import with error handling
try:
    from supabase import create_client
    print("✓ Supabase import successful")
except ImportError as e:
    print(f"✗ Supabase import failed: {e}")
    raise

try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ dotenv loaded")
except ImportError:
    print("✓ dotenv not available (normal on Railway)")

app = Flask(__name__)
PHANTOMBUSTER_API_KEY = os.getenv("PHANTOMBUSTER_API_KEY")
PHANTOM_AGENT_ID = os.getenv("PHANTOM_AGENT_ID")
session_cookie = os.getenv('LINKEDIN_SESSION_COOKIE')


# Connect to Supabase with enhanced error handling
try:
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    
    if not supabase_url or not supabase_key:
        raise ValueError("Missing Supabase environment variables")
    
    supabase = create_client(supabase_url, supabase_key)
    print("✓ Supabase client created successfully")
except Exception as e:
    print(f"✗ Error creating Supabase client: {e}")
    supabase = None

def calculate_engagement_rate(post):
    """Calculate engagement rate for a post"""
    impressions = post.get('impressions', 1) or 1  # Avoid division by zero
    total_engagement = post.get('likes', 0) + post.get('comments', 0) + post.get('shares', 0) + post.get('clicks', 0)
    return round((total_engagement / impressions) * 100, 2)

def download_csv_from_url(csv_url):
    """Download CSV content from PhantomBuster URL"""
    try:
        print(f"📥 Downloading CSV from: {csv_url}")
        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()
        print(f"✓ CSV downloaded successfully, size: {len(response.content)} bytes")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"✗ Error downloading CSV: {e}")
        return None

def parse_csv_content(csv_content):
    """Parse CSV content and extract LinkedIn posts data"""
    try:
        print("📊 Parsing CSV content...")
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        
        posts = []
        for row_num, row in enumerate(csv_reader, 1):
            try:
                # Clean up the row data (remove extra spaces)
                cleaned_row = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
                posts.append(cleaned_row)
                
                if row_num <= 3:  # Log first 3 rows for debugging
                    print(f"Row {row_num} keys: {list(cleaned_row.keys())}")
                
            except Exception as row_error:
                print(f"⚠ Error processing row {row_num}: {row_error}")
                continue
        
        print(f"✓ Parsed {len(posts)} posts from CSV")
        return posts
        
    except Exception as e:
        print(f"✗ Error parsing CSV: {e}")
        return []

def extract_hashtags_and_mentions(content):
    """Extract hashtags and mentions from post content"""
    if not content:
        return [], []
    
    import re
    hashtags = re.findall(r'#\w+', content)
    mentions = re.findall(r'@\w+', content)
    
    return hashtags, mentions

def process_csv_posts(posts_data):
    """Process CSV posts data and save to Supabase"""
    processed_count = 0
    
    for i, post in enumerate(posts_data, 1):
        try:
            print(f"Processing CSV post {i}/{len(posts_data)}")
            
            # Map CSV columns to our data structure (adjust these based on your CSV headers)
            content = post.get('content') or post.get('Content') or post.get('text') or post.get('Text') or post.get('postContent') or ''
            post_url = post.get('postUrl') or post.get('Post URL') or post.get('url') or post.get('URL') or ''
            published_at = post.get('publishedAt') or post.get('Published At') or post.get('date') or post.get('Date') or post.get('createdAt') or ''
            author = post.get('author') or post.get('Author') or post.get('authorName') or post.get('profileName') or ''
            
            # Extract engagement metrics with flexible column names
            likes = int(post.get('likes') or post.get('Likes') or post.get('likeCount') or 0)
            comments = int(post.get('comments') or post.get('Comments') or post.get('commentCount') or 0)
            shares = int(post.get('shares') or post.get('Shares') or post.get('shareCount') or post.get('reposts') or 0)
            impressions = int(post.get('impressions') or post.get('Impressions') or post.get('views') or 0)
            
            # Extract hashtags and mentions from content
            hashtags, mentions = extract_hashtags_and_mentions(content)
            
            # Generate a unique post ID
            import hashlib
            post_id = post_url or hashlib.md5(f"{content[:100]}{published_at}".encode()).hexdigest()
            
            # Insert post data
            post_insert = supabase.table('posts').upsert({
                'linkedin_post_id': post_id,
                'content': content,
                'post_type': post.get('postType') or post.get('type') or 'post',
                'published_at': published_at,
                'author_id': author,
                'hashtags': hashtags,
                'mentions': mentions,
                'raw_data': post
            }).execute()
            
            if post_insert.data and len(post_insert.data) > 0:
                db_post_id = post_insert.data[0]['id']
                
                # Insert engagement metrics
                supabase.table('engagement_metrics').upsert({
                    'post_id': db_post_id,
                    'likes': likes,
                    'comments': comments,
                    'shares': shares,
                    'impressions': impressions,
                    'clicks': int(post.get('clicks') or post.get('Clicks') or 0),
                    'engagement_rate': calculate_engagement_rate({
                        'likes': likes, 'comments': comments, 'shares': shares, 'impressions': impressions
                    }),
                    'measured_at': datetime.utcnow().isoformat()
                }).execute()
                
                print(f"✓ Saved CSV post {i}: {content[:50]}...")
                processed_count += 1
                
        except Exception as post_error:
            print(f"✗ Error processing CSV post {i}: {post_error}")
            continue
    
    return processed_count

@app.route('/')
def root():
    return jsonify({
        'message': 'PhantomBuster Webhook Server - JSON & CSV Support!', 
        'status': 'ok',
        'supabase_status': 'connected' if supabase else 'disconnected',
        'supported_formats': ['JSON with resultObject', 'CSV download URL'],
        'phantom_credentials': 'configured' if PHANTOMBUSTER_API_KEY and PHANTOM_AGENT_ID else 'missing',
        'available_endpoints': [
            'GET /',
            'GET /health', 
            'POST /trigger-phantom',
            'POST /run-phantom',
            'POST /get-phantom-status',
            'POST /fetch-phantom-result',
            'POST /webhook'
        ],
        'timestamp': datetime.utcnow().isoformat()
    }), 200

@app.route('/health')
def health():
    return jsonify({
        'status': 'ok', 
        'supabase': 'ok' if supabase else 'error',
        'phantom_api': 'ok' if PHANTOMBUSTER_API_KEY else 'missing',
        'phantom_agent': 'ok' if PHANTOM_AGENT_ID else 'missing',
        'timestamp': datetime.utcnow().isoformat()
    }), 200

@app.route('/trigger-phantom', methods=['POST'])
def trigger_phantom():
    try:
        print("=== TRIGGER PHANTOM CALLED ===")
        
        api_key = os.getenv('PHANTOMBUSTER_API_KEY')
        agent_id = os.getenv('PHANTOM_AGENT_ID')
        
        if not api_key or not agent_id:
            print("✗ Phantom API credentials missing")
            return jsonify({'status': 'error', 'message': 'Phantom API credentials missing'}), 500
        
        print(f"✓ Using Agent ID: {agent_id}")
        
        # Get request data
        request_data = request.json or {}
        custom_args = request_data.get('arguments', {})

        # Inject the sessionCookie from ENV
        linkedin_cookie = os.getenv('LINKEDIN_SESSION_COOKIE')
        if linkedin_cookie:
            custom_args['sessionCookie'] = linkedin_cookie
        else:
            print("✗ LINKEDIN_SESSION_COOKIE not found in environment variables")
            return jsonify({'status': 'error', 'message': 'Missing LinkedIn sessionCookie in env'}), 500
        
        payload = {
            'id': agent_id,
            'argument': custom_args,
            'saveArgument': False
        }
        
        launch_url = 'https://api.phantombuster.com/api/v2/agents/launch'
        headers = {
            'Content-Type': 'application/json',
            'X-Phantombuster-Key-1': api_key
        }
        
        print(f"📤 Launching Phantom with payload: {payload}")
        
        response = requests.post(launch_url, headers=headers, json=payload, timeout=30)
        
        if response.status_code != 200:
            print(f"✗ Phantom API Error: {response.status_code} {response.text}")
            return jsonify({'status': 'error', 'message': 'Failed to trigger Phantom', 'details': response.text}), 500
        
        launch_data = response.json()
        print(f"✓ Phantom launched successfully: {launch_data}")
        
        return jsonify({'status': 'success', 'message': 'Phantom agent launched successfully', 'launch_data': launch_data}), 200
    
    except Exception as e:
        print(f"✗ Error triggering Phantom: {e}")
        return jsonify({'status': 'error', 'message': 'Internal server error', 'details': str(e)}), 500



@app.route('/run-phantom', methods=['POST'])
def run_phantom():
    try:
        print("=== RUNNING PHANTOM ===")
        
        # PhantomBuster API Config
        PHANTOMBUSTER_API_KEY = os.getenv('PHANTOMBUSTER_API_KEY')
        PHANTOM_AGENT_ID = '7741390690252670'  # <-- This is your Phantom Agent ID
        
        if not PHANTOMBUSTER_API_KEY:
            return jsonify({'status': 'error', 'message': 'Missing PhantomBuster API Key'}), 500
        
        # API Endpoint to launch Phantom
        api_url = f'https://api.phantombuster.com/api/v2/agents/launch'
        
        headers = {
            'Content-Type': 'application/json',
            'X-Phantombuster-Key-1': PHANTOMBUSTER_API_KEY
        }
        
        payload = {
            'id': PHANTOM_AGENT_ID,
            'save': True
        }
        
        print("📡 Launching Phantom via API Call...")
        response = requests.post(api_url, headers=headers, json=payload)
        response.raise_for_status()
        
        run_result = response.json()
        print("✓ Phantom run triggered successfully!")
        print(json.dumps(run_result, indent=2))
        
        return jsonify({
            'status': 'success',
            'message': 'Phantom run initiated.',
            'phantom_response': run_result
        }), 200
    
    except Exception as e:
        print(f"✗ Error running Phantom: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to run Phantom', 'error': str(e)}), 500

@app.route('/fetch-phantom-result', methods=['POST'])
def fetch_phantom_result():
    try:
        print("=== FETCHING PHANTOM RESULT VIA AGENT OUTPUT ===")
        api_key = os.getenv('PHANTOMBUSTER_API_KEY')
        agent_id = os.getenv('PHANTOM_AGENT_ID')
        if not api_key or not agent_id:
            return jsonify({'status': 'error', 'message': 'Missing Phantom API Key or Agent ID'}), 500

        url = f"https://api.phantombuster.com/api/v2/agents/fetch-output"
        headers = {'X-Phantombuster-Key-1': api_key}
        params = {'id': agent_id}

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        print("✓ Agent output fetched:", data)
        csv_url = data.get('csvUrl') or data.get('resultUrl')
        result_object = data.get('resultObject')

        return jsonify({
            'status': 'success',
            'message': 'Agent fetch-output successful',
            'csv_url': csv_url,
            'result_object': result_object,
            'full': data
        }), 200

    except requests.exceptions.RequestException as e:
        print("✗ Fetch error:", e)
        return jsonify({'status': 'error', 'message': 'Failed to fetch output', 'error': str(e)}), 500
    except Exception as e:
        print("✗ Unexpected error:", e)
        return jsonify({'status': 'error', 'message': 'Internal server error', 'error': str(e)}), 500



@app.route('/get-phantom-status', methods=['POST'])
def get_phantom_status():
    try:
        print("=== CHECKING PHANTOM STATUS ===")
        
        PHANTOMBUSTER_API_KEY = os.getenv('PHANTOMBUSTER_API_KEY')
        
        if not PHANTOMBUSTER_API_KEY:
            return jsonify({'status': 'error', 'message': 'Missing PhantomBuster API Key'}), 500
        
        # Get container ID from request body
        request_data = request.json or {}
        container_id = request_data.get('container_id') or request_data.get('containerId')
        
        if not container_id:
            return jsonify({
                'status': 'error', 
                'message': 'Container ID is required. Pass it as {"container_id": "your-container-id"}'
            }), 400
        
        print(f"🔍 Checking status for container: {container_id}")
        
        # API Endpoint to get container status
        api_url = f'https://api.phantombuster.com/api/v2/containers/fetch'
        
        headers = {
            'Content-Type': 'application/json',
            'X-Phantombuster-Key-1': PHANTOMBUSTER_API_KEY
        }
        
        payload = {
            'id': container_id
        }
        
        response = requests.post(api_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        status_data = response.json()
        
        if 'data' in status_data:
            container_status = status_data['data'].get('status', 'unknown')
            print(f"✓ Container Status: {container_status}")
            
            return jsonify({
                'status': 'success',
                'container_id': container_id,
                'container_status': container_status,
                'is_finished': container_status in ['finished', 'success', 'completed'],
                'is_running': container_status in ['running', 'started'],
                'is_error': container_status in ['error', 'failed'],
                'full_status': status_data
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': 'Could not determine container status',
                'response': status_data
            }), 500
    
    except Exception as e:
        print(f"✗ Error checking status: {e}")
        return jsonify({
            'status': 'error', 
            'message': 'Failed to check container status', 
            'error': str(e)
        }), 500

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        print("=== WEBHOOK CALLED ===")
        
        # Check if Supabase is available
        if not supabase:
            print("✗ Supabase not available")
            return jsonify({'status': 'error', 'message': 'Database connection not available'}), 500
        
        data = request.json
        if not data:
            print("✗ No JSON data received")
            return jsonify({'status': 'error', 'message': 'No JSON data received'}), 400
        
        print("✓ Incoming Data received:", list(data.keys()))
        
        # Check if this is a CSV-based webhook (new format)
        csv_url = data.get('csvUrl') or data.get('csv_url') or data.get('downloadUrl') or data.get('resultUrl')
        
        if csv_url:
            print("🔄 Processing CSV-based webhook...")
            
            # Download CSV content
            csv_content = download_csv_from_url(csv_url)
            if not csv_content:
                return jsonify({'status': 'error', 'message': 'Failed to download CSV'}), 400
            
            # Parse CSV and extract posts
            posts_data = parse_csv_content(csv_content)
            if not posts_data:
                return jsonify({'status': 'error', 'message': 'No valid posts found in CSV'}), 400
            
            # Process CSV posts
            processed_count = process_csv_posts(posts_data)
            
            print(f"✓ CSV Webhook completed: {processed_count}/{len(posts_data)} posts processed")
            
            return jsonify({
                'status': 'success',
                'message': f'CSV processed: {processed_count} out of {len(posts_data)} posts saved',
                'processed_count': processed_count,
                'total_count': len(posts_data),
                'format': 'CSV'
            }), 200
        
        # Check if this is the original JSON format
        elif 'resultObject' in data:
            print("🔄 Processing JSON-based webhook (original format)...")
            
            # Parse the resultObject string into JSON
            try:
                result_data = json.loads(data['resultObject'])
                print("✓ Parsed Result Data:", len(result_data), "items")
            except json.JSONDecodeError as e:
                print(f"✗ JSON parsing error: {e}")
                return jsonify({'status': 'error', 'message': f'Invalid JSON in resultObject: {str(e)}'}), 400
            
            if not isinstance(result_data, list):
                print("✗ resultObject is not a list")
                return jsonify({'status': 'error', 'message': 'resultObject should contain a list'}), 400
            
            processed_items = 0
            
            for i, item in enumerate(result_data):
                try:
                    print(f"Processing item {i+1}/{len(result_data)}")
                    
                    # Check if it's a Company Profile Data
                    if 'companyName' in item:
                        print(f"✓ Processing company: {item['companyName']}")
                        supabase.table('company_profile').upsert({
                            'name': item['companyName'],
                            'linkedin_url': item.get('companyUrl', ''),
                            'followers': item.get('followerCount', 0),
                            'website': item.get('website', ''),
                            'description': item.get('description', ''),
                            'industry': item.get('industry', ''),
                            'company_size': item.get('companySize', ''),
                            'specialties': [],
                            'location': item.get('location', ''),
                            'fetched_at': datetime.utcnow().isoformat()
                        }).execute()
                        print("✓ Company Profile Saved")
                        processed_items += 1
                    
                    # Check if it's a Post Data
                    elif 'postId' in item:
                        print(f"✓ Processing post: {item['postId']}")
                        post_insert = supabase.table('posts').upsert({
                            'linkedin_post_id': item.get('postId'),
                            'content': item.get('content', ''),
                            'post_type': item.get('postType', ''),
                            'published_at': item.get('publishedAt', ''),
                            'author_id': item.get('authorId', ''),
                            'hashtags': item.get('hashtags', []),
                            'mentions': item.get('mentions', []),
                            'raw_data': item
                        }).execute()
                        
                        if post_insert.data and len(post_insert.data) > 0:
                            post_id = post_insert.data[0]['id']
                            supabase.table('engagement_metrics').insert({
                                'post_id': post_id,
                                'likes': item.get('likes', 0),
                                'comments': item.get('comments', 0),
                                'shares': item.get('shares', 0),
                                'impressions': item.get('impressions', 0),
                                'clicks': item.get('clicks', 0),
                                'engagement_rate': calculate_engagement_rate(item),
                                'measured_at': datetime.utcnow().isoformat()
                            }).execute()
                            print(f"✓ Post and Engagement Data Saved for Post ID: {post_id}")
                            processed_items += 1
                        else:
                            print("⚠ Warning: No data returned from post insert")
                    else:
                        print(f"⚠ Unknown item type in item {i+1}")
                    
                except Exception as item_error:
                    print(f"✗ Error processing item {i+1}: {item_error}")
                    continue
            
            print(f"✓ JSON Webhook completed: {processed_items} items processed")
            return jsonify({
                'status': 'success', 
                'message': f'JSON processed: {processed_items} items saved',
                'processed_count': processed_items,
                'format': 'JSON'
            }), 200
        
        else:
            print("✗ No recognized data format found")
            print("Available keys:", list(data.keys()))
            return jsonify({
                'status': 'error', 
                'message': 'No CSV URL or resultObject found in webhook data',
                'received_keys': list(data.keys())
            }), 400
        
    except Exception as e:
        print(f"✗ Webhook error: {e}")
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    print(f"404 error: {error}")
    return jsonify({'status': 'error', 'message': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    print(f"500 error: {error}")
    return jsonify({'status': 'error', 'message': 'Internal server error'}), 500

print("✓ Complete Flask webhook ready - supports JSON, CSV, and Phantom management!")

# Comment out when using gunicorn
# if __name__ == "__main__":
#     port = int(os.environ.get('PORT', 5000))
#     app.run(host='0.0.0.0', port=port, debug=False)