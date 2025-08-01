from flask import Flask, request, jsonify
import os
import json
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

@app.route('/')
def root():
    return jsonify({
        'message': 'Flask app is running on Railway!', 
        'status': 'ok',
        'supabase_status': 'connected' if supabase else 'disconnected',
        'timestamp': datetime.utcnow().isoformat()
    }), 200

@app.route('/health')
def health():
    return jsonify({
        'status': 'ok', 
        'supabase': 'ok' if supabase else 'error',
        'timestamp': datetime.utcnow().isoformat()
    }), 200

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
        
        print("✓ Incoming Data received:", len(str(data)), "characters")
        
        # Check if 'resultObject' is present (PhantomBuster Data)
        if 'resultObject' not in data:
            print("✗ No resultObject found")
            return jsonify({'status': 'error', 'message': 'No resultObject found'}), 400
        
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
        
        print(f"✓ Webhook completed: {processed_items} items processed")
        return jsonify({
            'status': 'success', 
            'message': f'Data processed successfully. {processed_items} items processed.',
            'processed_count': processed_items
        }), 200
        
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

print("✓ Flask app setup complete, ready to handle requests")

# Comment out when using gunicorn
# if __name__ == "__main__":
#     port = int(os.environ.get('PORT', 5000))
#     app.run(host='0.0.0.0', port=port, debug=False)