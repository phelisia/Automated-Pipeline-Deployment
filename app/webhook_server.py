from flask import Flask, request, jsonify
import os
import json
from supabase import create_client
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
print("SUPABASE_URL:", os.getenv('SUPABASE_URL'))
print("SUPABASE_SERVICE_ROLE_KEY:", os.getenv('SUPABASE_SERVICE_ROLE_KEY'))

app = Flask(__name__)

# Connect to Supabase


# Get values from .env file
supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_SERVICE_ROLE_KEY')
)

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    print("Incoming Data:", data)  # Keep for debugging

    # Parse the resultObject string into JSON
   # Check if 'resultObject' is present (PhantomBuster Data)
    if 'resultObject' in data:
        result_data = json.loads(data['resultObject'])
        print("Parsed Result Data:", result_data)

        for item in result_data:
            # Check if it's a Company Profile Data
            if 'companyName' in item:
                supabase.table('company_profile').upsert({
                    'name': item['companyName'],
                    'linkedin_url': item['companyUrl'],
                    'followers': item.get('followerCount', 0),
                    'website': item.get('website', ''),
                    'description': item.get('description', ''),
                    'industry': item.get('industry', ''),
                    'company_size': item.get('companySize', ''),
                    'specialties': [],  # Optional
                    'location': item.get('location', ''),
                    'fetched_at': datetime.utcnow().isoformat()
                }).execute()
                print("Company Profile Saved")

            # Check if it's a Post Data
            if 'postId' in item:
                post_insert = supabase.table('posts').upsert({
                    'linkedin_post_id': item.get('postId'),
                    'content': item.get('content'),
                    'post_type': item.get('postType'),
                    'published_at': item.get('publishedAt'),
                    'author_id': item.get('authorId'),
                    'hashtags': item.get('hashtags', []),
                    'mentions': item.get('mentions', []),
                    'raw_data': item
                }).execute()

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
                print(f"Post and Engagement Data Saved for Post ID: {post_id}")

        return jsonify({'status': 'success', 'message': 'Data processed successfully'}), 200

    else:
        return jsonify({'status': 'fail', 'message': 'No resultObject found'}), 400

def calculate_engagement_rate(post):
    impressions = post.get('impressions', 1) or 1  # Avoid division by zero
    total_engagement = post.get('likes', 0) + post.get('comments', 0) + post.get('shares', 0) + post.get('clicks', 0)
    return round((total_engagement / impressions) * 100, 2)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200



if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))

